package raft

import (
	// "bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JasonLou99/Hybrid_KV_Store/pool"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/raftrpc"

	// "github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

// 服务端和Raft层面的数据传输通道
type ApplyMsg struct {
	CommandValid bool // true为log，false为snapshot

	// 向application层提交日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int
}

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []string   // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	dead      int32      // set by Kill()

	currentTerm       int                 // 见过的最大任期
	votedFor          int                 // 记录在currentTerm任期投票给谁了
	log               []*raftrpc.LogEntry // 操作日志
	lastIncludedIndex int                 // snapshot最后1个logEntry的index，没有snapshot则为0
	lastIncludedTerm  int                 // snapthost最后1个logEntry的term，没有snaphost则无意义

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role              string    // 身份
	leaderId          int       // leader的id
	lastActiveTime    time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	lastBroadcastTime time.Time // 作为leader，上次的广播时间

	applyCh chan ApplyMsg // 应用层的提交队列
	pools   []pool.Pool   // 用于日志同步的连接池
	// kvrpc.UnimplementedKVServer
	raftrpc.UnimplementedRaftServer
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) GetLeaderId() (leaderId int32) {
	return int32(rf.leaderId)
}

func (rf *Raft) RequestVote(ctx context.Context, args *raftrpc.RequestVoteRequest) (reply *raftrpc.RequestVoteResponse, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = int32(rf.currentTerm)
	reply.VoteGranted = false

	util.DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	defer func() {
		util.DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v] ", rf.me, args.CandidateId,
			args.Term, rf.currentTerm, reply.VoteGranted)
	}()

	// 任期不如我大，拒绝投票
	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1 // 有问题，如果两个leader同时选举，那会进行多次投票，因为都满足下方的投票条件---没有问题，如果第二个来请求投票，此时args.Term = rf.currentTerm。因为rf.currentTerm已经更新
		rf.leaderId = -1
	}

	// 每个任期，只能投票给1人
	if rf.votedFor == -1 || rf.votedFor == int(args.CandidateId) {
		// candidate的日志必须比我的新
		// 1, 最后一条log，任期大的更新
		// 2，任期相同, 更长的log则更新
		lastLogTerm := rf.lastTerm()
		// log长度一样也是可以给对方投票的
		if args.LastLogTerm > int32(lastLogTerm) || (args.LastLogTerm == int32(lastLogTerm) && args.LastLogIndex >= int32(rf.lastIndex())) {
			rf.votedFor = int(args.CandidateId)
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，重置选举超时的时间
		}
	}
	return reply, nil
}

// 已兼容snapshot
func (rf *Raft) AppendEntriesInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (reply *raftrpc.AppendEntriesInRaftResponse, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	util.DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)

	reply.Term = int32(rf.currentTerm)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	defer func() {
		util.DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
			rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log), reply.ConflictIndex)
	}()

	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
	}

	// 认识新的leader
	rf.leaderId = int(args.LeaderId)
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()

	if args.PrevLogIndex > int32(rf.lastIndex()) { // prevLogIndex位置没有日志的情况
		reply.ConflictIndex = int32(rf.lastIndex() + 1)
		return reply, nil
	}
	// prevLogIndex位置有日志，那么判断term必须相同，否则false
	if args.PrevLogIndex != 0 && (rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term != int32(args.PrevLogTerm)) {
		reply.ConflictTerm = rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term
		for index := rf.lastIncludedIndex + 1; index <= int(args.PrevLogIndex); index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
			if rf.log[rf.index2LogPos(index)].Term == int32(reply.ConflictTerm) {
				reply.ConflictIndex = int32(index)
				break
			}
		}
		return reply, nil
	}

	// 找到了第一个不同的index，开始同步日志
	for i, logEntry := range args.Entries {
		index := int(args.PrevLogIndex) + 1 + i
		logPos := rf.index2LogPos(index)
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}

	// 更新提交下标
	if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
		rf.commitIndex = int(args.LeaderCommit)
		if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
			rf.commitIndex = rf.lastIndex()
		}
	}
	reply.Success = true
	return reply, nil
}

// 已兼容snapshot
func (rf *Raft) Start(command *raftrpc.Interface) (int32, int32, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		return -1, -1, false
	}
	logEntry := raftrpc.LogEntry{
		Command: command,
		Term:    int32(rf.currentTerm),
	}
	rf.log = append(rf.log, &logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm

	util.DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
	return int32(index), int32(term), isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) RegisterRaftServer(ctx context.Context, address string) { // 传入的地址是internalAddress，节点间交流用的地址（用于类似日志同步等）
	util.DPrintf("RegisterRaftServer: %s", address)
	for { // 创建一个TCP监听器，并在指定的地址（）上监听传入的连接。如果监听失败，则会打印错误信息。
		lis, err := net.Listen("tcp", address)
		if err != nil {
			util.FPrintf("failed to listen: %v", err)
		}
		grpcServer := grpc.NewServer(
			grpc.InitialWindowSize(pool.InitialWindowSize),
			grpc.InitialConnWindowSize(pool.InitialConnWindowSize),
			grpc.MaxSendMsgSize(pool.MaxSendMsgSize),
			grpc.MaxRecvMsgSize(pool.MaxRecvMsgSize),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				PermitWithoutStream: true,
				MinTime:             10 * time.Second, // 这里设置与client的keepalive探测的最小时间间隔。
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:                  pool.KeepAliveTime,
				Timeout:               pool.KeepAliveTimeout,
				MaxConnectionAgeGrace: 20 * time.Second,
			}),
		)
		raftrpc.RegisterRaftServer(grpcServer, rf)
		reflection.Register(grpcServer)

		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
			fmt.Println("Raft stopped due to context cancellation-Raft.")
		}()

		if err := grpcServer.Serve(lis); err != nil { // 调用Serve方法来启动gRPC服务器，监听传入的连接，并处理相应的请求
			util.FPrintf("failed to serve: %v", err)
		}

		fmt.Println("跳出Raftserver的for循环，日志同步完成")
		break
	}
}

func (rf *Raft) sendRequestVote(address string, args *raftrpc.RequestVoteRequest) (bool, *raftrpc.RequestVoteResponse) {
	// time.Sleep(time.Millisecond * time.Duration(rf.delay+rand.Intn(25)))
	util.DPrintf("Start sendRequestVote")
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		util.EPrintf("did not connect: %v", err)
		return false, nil
	}
	defer conn.Close()
	client := raftrpc.NewRaftClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
	defer cancel()
	reply, err := client.RequestVote(ctx, args)

	if err != nil {
		util.EPrintf("Error calling RequestVote method on server side; err:%v; address:%v ", err, address)
		return false, reply
	} else {
		return true, reply

	}
}

func (rf *Raft) sendAppendEntries(address string, args *raftrpc.AppendEntriesInRaftRequest, p pool.Pool) (*raftrpc.AppendEntriesInRaftResponse, bool) {
	// 用grpc连接池同步日志
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()
	client := raftrpc.NewRaftClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*100)
	defer cancel()
	reply, err := client.AppendEntriesInRaft(ctx, args)

	if err != nil {
		util.EPrintf("Error calling AppendEntriesInRaft method on server side; err:%v; address:%v ", err, address)
		return reply, false
	}
	return reply, true
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // 每隔一小段时间，检查是否超时，也就是说follower如果变成candidate，还得等10ms才能开始选举

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			now := time.Now()
			timeout := time.Duration(15000+rand.Int31n(150)) * time.Millisecond // 超时随机化 10s-10s150ms
			elapses := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					util.DPrintf("RaftNode[%d] Follower -> Candidate", rf.me)
				}
			}
			// 请求vote，当变成candidate后，等待10ms才进入到该if语句
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = time.Now() // 重置下次选举时间
				rf.currentTerm += 1            // 发起新任期
				rf.votedFor = rf.me            // 该任期投了自己

				// 请求投票req
				args := raftrpc.RequestVoteRequest{
					Term:         int32(rf.currentTerm),
					CandidateId:  int32(rf.me),
					LastLogIndex: int32(rf.lastIndex()),
				}
				args.LastLogTerm = int32(rf.lastTerm())

				rf.mu.Unlock()	// 对raft的修改操作已经暂时结束，可以解锁

				util.DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
					args.LastLogIndex, args.LastLogTerm)
				// 并发RPC请求vote
				type VoteResult struct {
					peerId int
					resp   *raftrpc.RequestVoteResponse
				}
				voteCount := 1   // 收到投票个数（先给自己投1票）
				finishCount := 1 // 收到应答个数
				voteResultChan := make(chan *VoteResult, len(rf.peers))
				for peerId := 0; peerId < len(rf.peers); peerId++ {
					go func(id int) {
						if id == rf.me {
							return
						}
						if ok, reply := rf.sendRequestVote(rf.peers[id], &args); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: reply}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				for {
					select {
					case voteResult := <-voteResultChan:
						finishCount += 1
						if voteResult.resp != nil {
							if voteResult.resp.VoteGranted {
								voteCount += 1
							}
							if int(voteResult.resp.Term) > maxTerm { // 记录投票的server中最大的term
								maxTerm = int(voteResult.resp.Term)
							}
						}
						// 得到大多数vote后，立即离开
						if finishCount == len(rf.peers) || voteCount > len(rf.peers)/2 {
							goto VOTE_END
						}
					}
				}
			VOTE_END:
				// rf.mu.Lock()
				defer func() {
					util.DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
						rf.role, maxTerm, rf.currentTerm)
				}()
				// 如果角色改变了，则忽略本轮投票结果；当多个server同时开始选举，有一个leader已经选出后，则本server的选举结果可直接不用管。
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower；这个是不是可以在接受投票时就判断，如果有任期比自己大的，就直接转换为follower，也不看投票结果了
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = -1
					rf.currentTerm = maxTerm // 更新自己的Term和voteFor
					rf.votedFor = -1
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					util.DPrintf("RaftNode[%d] Candidate -> Leader", rf.me)
					op := &raftrpc.Interface{
						OpType: "TermLog",
					}
					op.Index, op.Term, _ = rf.Start(op) // 需要提交一个空的指令
					rf.leaderId = rf.me
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}
					rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行，因为leader的term开始时，需要提交一条空的无操作记录。
					return
				}
			}
		}()
	}
}

func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.lastIndex()) // 补充自己位置的index
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// if语句的第一个条件则是排除掉还没有复制到大多数server的情况
	if newCommitIndex > rf.commitIndex && rf.log[rf.index2LogPos(newCommitIndex)].Term == int32(rf.currentTerm) {
		rf.commitIndex = newCommitIndex // 保证是当前的Term才能根据同步到server的副本数量判断是否可以提交
	}
	util.DPrintf("RaftNode[%d] updateCommitIndex, newCommitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

// 已兼容snapshot
func (rf *Raft) doAppendEntries(peerId int) {
	args := raftrpc.AppendEntriesInRaftRequest{Entries: []*raftrpc.LogEntry{}}
	args.Term = int32(rf.currentTerm)
	args.LeaderId = int32(rf.me)
	args.LeaderCommit = int32(rf.commitIndex)
	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)
	args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
	args.Entries = append(args.Entries, rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]...)

	util.DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

	go func() {
		util.DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args.Term, args.LeaderId)
		if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, rf.pools[peerId]); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			defer func() {
				util.DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
					rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			}()

			// 如果不是rpc前的leader状态了，那么啥也别做了，可能遇到了term更大的server
			if rf.currentTerm != int(args.Term) {
				return
			}
			if reply.Term > int32(rf.currentTerm) { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = -1
				rf.currentTerm = int(reply.Term)
				rf.votedFor = -1
				// rf.persist()
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功
				rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(args.Entries) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 // 记录已经复制到其他server的日志的最后index的情况
				rf.updateCommitIndex()                           // 更新commitIndex
			} else {
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > int32(rf.lastIncludedIndex); index-- {
						// if rf.log[rf.index2LogPos(int(index))].Term == reply.ConflictTerm {
						// 	conflictTermIndex = int(index)
						// 	break
						// }
						// 我认为下方这个效果更好，这样PrevLogIndex的值就为 index
						if rf.log[rf.index2LogPos(int(index))].Term != reply.ConflictTerm {
							conflictTermIndex = int(index + 1)
							break
						}
					}
					if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
						rf.nextIndex[peerId] = conflictTermIndex
					} else {
						rf.nextIndex[peerId] = int(reply.ConflictIndex) // 用follower首次出现term的index作为同步开始
					}
				} else {
					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
					// 这时候我们将返回的conflictIndex设置为nextIndex即可
					rf.nextIndex[peerId] = int(reply.ConflictIndex)
				}
				util.DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
		}
	}()
}

func (rf *Raft) appendEntriesLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // 间隔10ms

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}

			// 100ms广播1次
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) < 100*time.Millisecond {
				return
			}
			rf.lastBroadcastTime = time.Now() // 确定过了广播的时间间隔，才开始进行广播，并且设置新的广播时间

			// 向所有follower发送心跳
			for peerId := 0; peerId < len(rf.peers); peerId++ {
				if peerId == rf.me {
					continue
				}
				rf.doAppendEntries(peerId) // 还要考虑append日志失败的情况
			}
		}()
	}
}

func (rf *Raft) applyLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  int(rf.log[appliedIndex].Term),
				}
				rf.applyCh <- appliedMsg // 引入snapshot后，这里必须在锁内投递了，否则会和snapshot的交错产生bug
				util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
				noMore = false
			}
		}()
	}
}

// 最后的index
func (rf *Raft) lastIndex() int {
	return len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	if len(rf.log) != 0 {
		lastLogTerm = int(rf.log[len(rf.log)-1].Term)
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - 1
}

// 服务器地址数组；当前方法对应的服务器地址数组中的下标；持久化存储了当前服务器状态的结构体；传递消息的通道结构体
func Make(peers []string, me int,
	persister *Persister, applyCh chan ApplyMsg, ctx context.Context) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.role = ROLE_FOLLOWER
	rf.leaderId = -1
	rf.votedFor = -1
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	// 这就是自己修改grpc线程池option参数的做法
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              128,
		MaxActive:            200,
		MaxConcurrentStreams: 64,
		Reuse:                true,
	}
	// 根据servers的地址，创建了一一对应server地址的grpc连接池
	for i := 0; i < len(peers); i++ {
		peers_single := []string{peers[i]}
		p, err := pool.New(peers_single, DesignOptions)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		// grpc连接池组
		rf.pools = append(rf.pools, p)
	}

	util.DPrintf("RaftNode[%d] Make again", rf.me)

	go rf.RegisterRaftServer(ctx, peers[me])
	// election
	go rf.electionLoop()
	// sync
	go rf.appendEntriesLoop()
	// apply
	go rf.applyLogLoop()

	// 设置一个定时器，每十秒检查一次条件
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if rf.killed() { // 如果上次KVS关闭了Raft，则可以关闭pool
			for _, pool := range rf.pools {
				pool.Close()
				util.DPrintf("The raft pool has been closed")
			}
		}
	}

	util.DPrintf("Raft has been closed")
	return rf
}
