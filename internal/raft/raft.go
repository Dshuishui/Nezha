package raft

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"log"
	"path/filepath"
	"runtime"
	"strconv"

	// "encoding/gob"
	// "encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	// "gitee.com/dong-shuishui/FlexSync/internal/raft"
	// "gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"

	// "gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	// "google.golang.org/protobuf/proto"
)

// 服务端和Raft层面的数据传输通道
type ApplyMsg struct {
	CommandValid bool // true为log，false为snapshot

	// 向application层提交日志
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	Offset       int64
	// FileVersion 标明 Offset 是相对哪一个 valuelog 文件的。
	//
	// 不能改用命令自带的 FileVersion：那个值在"决定写入"时记下（follower 在
	// AppendEntries 里、leader 在 StartPut 里），而 Offset 在"实际写入文件"时才
	//产生。两个时刻之间 GC 可以切换文件——切换走的是 logMu，与写入路径持有的
	// rf.mu 是两把锁，拦不住。于是偏移属于新文件、版本却记成旧的，读取时拿新文件
	// 的偏移去旧文件里找，越界报 EOF。
	//
	// 这里的值与 Offset 在同一把 logMu 下、同一时刻产生，因此严格对应。
	FileVersion int32
}

// 日志项
type LogEntry struct {
	Command DetailCod
	Term    int32
}

type DetailCod struct {
	Index    int32
	Term     int32
	OpType   string
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

type Entry struct {
	Index       uint32
	CurrentTerm uint32
	VotedFor    uint32
	Key         string
	Value       string
	// NoOp marks a leader's no-op entry (TermLog). It is written to the log file as well;
	// otherwise the file has index gaps and cannot be replayed on restart. On disk it is a
	// record with keySize==0 (real keys are always KeyLength bytes).
	NoOp bool
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

var threshold int64 = 30 * 1024 * 1024

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []string   // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	dead      int32      // set by Kill()

	currentTerm int                 // 见过的最大任期
	votedFor    int                 // 记录在currentTerm任期投票给谁了
	log         []*raftrpc.LogEntry // 操作日志

	// 所有服务器，易失状态
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 当前应用到状态机的索引

	// 仅Leader，易失状态（成为leader时重置）
	nextIndex  []int //	每个follower的log同步起点索引（初始为leader log的最后一项）
	matchIndex []int // 每个follower的log同步进度（初始为0），和nextIndex强关联

	// 所有服务器，选举相关状态
	role           string    // 身份
	leaderId       int       // leader的id
	lastActiveTime time.Time // 上次活跃时间（刷新时机：收到leader心跳、给其他candidates投票、请求其他节点投票）
	// lastBroadcastTime time.Time // 作为leader，上次的广播时间

	applyCh chan ApplyMsg // 应用层的提交队列
	pools   []pool.Pool   // 用于日志同步的连接池
	// LSM-Raft baseline: where incoming SSTable spans are assembled and who ingests them
	// (see sstable.go). Nil installer = the RPC is refused.
	sstIncomingDir string
	sstInstaller   SSTableInstaller
	// kvrpc.UnimplementedKVServer
	raftrpc.UnimplementedRaftServer
	LastAppendTime time.Time
	Gap            int
	Offsets        []int64
	// offsetVersions 与 Offsets 一一对应，记下每个偏移属于哪一轮 GC 的文件。
	// 两者在 WriteEntryToFile 里同一把 logMu 下一起追加、在 applyLogLoop 里
	// 一起消费，因此不会像"命令自带的 FileVersion"那样与实际写入的文件错开。
	offsetVersions []int32
	shotOffset     int
	SyncTime       int
	SyncChans      []chan string

	// replicaWake 唤醒 appendEntriesLoop：某个 peer 的一轮复制有了回执，或本节点刚当选。
	// 容量 1，多次信号合并成一次唤醒即可——循环醒来后会把所有 SyncChans 排空。
	replicaWake chan struct{}

	// heardFromLeader 表示本节点曾收到过某个 leader 的 AppendEntries/心跳。
	// 只用于 §4.2.3 的防打扰判据，一旦置真不再复位——它回答的是"这个集群里有没有过
	// leader"，而"多久之前"由 LastAppendTime 回答。
	heardFromLeader bool

	// 两者都只在测试里赋值，且必须在 appendEntriesLoop 起来之前：
	// sendAppend 替换 doAppendEntries，appendRoundBudget 覆盖看门狗时限（<=0 用默认值）。
	sendAppend        func(peerId int)
	appendRoundBudget time.Duration
	batchLog          []*Entry
	currentLog        string // 存储value的磁盘文件的描述符
	originalLog       string // dwisckey

	// 日志压缩基址：rf.log[0] 对应的日志 index 为 lastIncludedIndex+1。
	// 已压缩的条目从内存中物理删除，其内容仍保留在 rf.currentLog 磁盘文件中。
	lastIncludedIndex int   // 已压缩掉的最后一条日志的 index
	lastIncludedTerm  int32 // 上述条目的 term，用于 PrevLogTerm 一致性检查

	// ---- crash recovery ----
	stateFile   string // where term/votedFor/log base are persisted; empty disables persistence
	stateLoaded bool   // whether a state file was read at startup (enables the base cross-check)
	// index/term of the entry just before the oldest log file still on disk. Distinct from
	// lastIncludedIndex, which is the in-memory trim point and may be far ahead of it.
	// lastIncludedIndex starts from here on restart.
	fileBaseIndex int
	fileBaseTerm  int32
	// The write path tracks the last record written to the current file; a GC file switch
	// freezes it as pendingBase, and PersistLogBase promotes that to fileBase right before
	// the old file is deleted. Guarded by logMu.
	lastWrittenIndex int
	lastWrittenTerm  int32
	pendingBaseIndex int
	pendingBaseTerm  int32

	// 复用的日志文件句柄。此前每写一条日志都要 OpenFile + NewWriter + Seek +
	// Write + Flush + Close，五次系统调用里只有一次在搬运数据。写 64B 的小 value
	// 时这套固定开销就是全部成本；写 64KB 时它可以忽略——这正是本系统在小 value
	// 上吞吐塌掉的原因。
	// 句柄常驻后每条只剩 Write + Flush。持久化语义不变：仍是每批写完立刻 Flush，
	// 且原本就没有 fsync，本改动不触碰崩溃一致性。
	// group commit：攒批共用一次写入与一次 fsync，见 groupcommit.go
	// commitSignal 唤醒 commitIndexUpdateLoop。
	//
	// 该循环原先只靠每 10ms 一次的轮询推进 commitIndex。单节点下没有 follower、
	// 不走日志复制，commitIndex 便完全由这个定时器驱动：一条日志写完后要等它
	// 醒来才可能被提交，之后才轮到 apply。这是写入延迟里最大的一段固定开销，
	// 实测单客户端 p50 恰好贴在 10ms 上。
	commitSignal chan struct{}

	// applySignal 唤醒 applyLogLoop。
	//
	// 那个循环原先空转时睡 10ms 再查一遍 commitIndex，于是一条已经提交的日志
	// 最多干等 10ms 才被应用——实测单客户端下写日志只要 1.05ms 而等 apply
	// 达 14.48ms，这段固定延迟就是它。
	//
	// 容量 1 且非阻塞发送：信号只表示"有活干"，多次提交合并成一次唤醒即可。
	applySignal chan struct{}

	batchMu     sync.Mutex
	curBatch    *flushBatch
	flushSignal chan struct{}
	groupCommit bool
	batchWindow time.Duration

	// syncOnWrite 决定日志写入后是否 fsync。
	//
	// 默认关闭，与改动前的行为一致：此前每批只 Flush 到 OS page cache，从不落盘。
	// 那样一次"持久化写入"只值约 6 微秒（实测），在 11.45ms 的端到端延迟里占 0.05%，
	// 于是"少写一次"这个架构收益完全测不出来——开关本身是关着的。
	//
	// 打开后每批 Flush 之后再 Sync，这才是 Raft 要求的语义：日志落盘后才能响应客户端。
	// 单次 fsync 在 NVMe 上 50~200μs、SATA SSD 或机械盘 0.5~10ms，届时持久化成本
	// 成为主导项，"把两次持久化合成一次"的价值才显现出来。
	syncOnWrite bool
	// extraLog 是 Dwisckey 的第二份持久化副本。
	//
	// Nezha 让 Raft 日志兼任 valuelog，value 只落盘一次；Dwisckey 那类系统在
	// Raft 日志之外还要把 value 再写进自己的 valuelog，于是每条写入多一次落盘。
	// 论文里 Nezha 相对它的优势正是这一次。
	//
	// 这份副本只制造持久化开销，不参与读路径——读仍按 Raft 日志的偏移取值，
	// 与 nezha-nogc 完全一致。这样两者的差别就只剩那一次写，对比不掺别的因素。
	extraLogFile   *os.File
	extraLogWriter *bufio.Writer

	// logMu 单独保护下面三个字段，不复用 rf.mu：GC 在自己的 goroutine 里调用
	// SetCurrentLog 切换日志文件，而那条路径上并不持有 rf.mu。句柄常驻之后，
	// 换文件要关掉旧句柄，若不加锁就会与正在写入的 goroutine 撞车，报
	// "file already closed" 并让节点在 GC 中途崩溃。
	// 这是一把叶子锁：持有它时绝不去取 rf.mu，因此与 rf.mu -> logMu 的既有顺序无冲突。
	logMu     sync.Mutex
	logFile   *os.File
	logWriter *bufio.Writer
	logOffset int64 // 下一条记录的写入位置；自行维护以省掉每次的 Seek
	// logVersion 是 logFile 当前对应的 GC 轮次，与 logFile 一同在 logMu 下切换，
	// 保证"偏移"与"它属于哪个文件"这两件事永远一致。
	logVersion int32
}

// 	// 包装文件对象以进行缓冲写入
// 	writer := bufio.NewWriter(file)

// 	// 准备写入的数据
// 	// keySize := uint32(len(e.Key))
// 	// valueSize := uint32(len(e.Value))
// 	// data := make([]byte, 20+keySize+valueSize) // 48 bytes for 6 uint64 + key + value

// 	// // 将数据编码到byte slice中
// 	// binary.BigEndian.PutUint32(data[0:4], e.Index)
// 	// binary.BigEndian.PutUint32(data[4:8], e.CurrentTerm)
// 	// binary.BigEndian.PutUint32(data[8:12], e.VotedFor)
// 	// binary.BigEndian.PutUint32(data[12:16], keySize)
// 	// binary.BigEndian.PutUint32(data[16:20], valueSize)
// 	// copy(data[20:20+keySize], e.Key)
// 	// copy(data[20+keySize:], e.Value)

// 	for _, entry := range e {
// 		keySize := uint32(len(entry.Key))
// 		valueSize := uint32(len(entry.Value))
// 		data := make([]byte, 20+keySize+valueSize) // 48 bytes for 6 uint64 + key + value

// 		// 将数据编码到byte slice中
// 		binary.BigEndian.PutUint32(data[0:4], entry.Index)
// 		binary.BigEndian.PutUint32(data[4:8], entry.CurrentTerm)
// 		binary.BigEndian.PutUint32(data[8:12], entry.VotedFor)
// 		binary.BigEndian.PutUint32(data[12:16], keySize)
// 		binary.BigEndian.PutUint32(data[16:20], valueSize)
// 		copy(data[20:20+keySize], entry.Key)
// 		copy(data[20+keySize:], entry.Value)

// 		// 添加偏移量到数组中
// 		offsets = append(offsets, offset)
// 		offset += int64(len(data))
// 	}
// 	rf.Offsets = append(rf.Offsets, offsets...)
// 	return offsets, nil
// }

func (rf *Raft) GetLeaderId() (leaderId int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int32(rf.leaderId)
}

// IsLeader reports whether this node currently holds the leader role. Prefer it over
// comparing GetLeaderId with rf.me: leaderId 0 also stands for "unknown".
func (rf *Raft) IsLeader() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == ROLE_LEADER
}

func (rf *Raft) GetApplyIndex() (applyindex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) RequestVote(ctx context.Context, args *raftrpc.RequestVoteRequest) (*raftrpc.RequestVoteResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &raftrpc.RequestVoteResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.VoteGranted = false

	// Raft 论文 §4.2.3 的防打扰规则：最小选举超时之内确认过还有 leader 在工作，
	// 就整个忽略这次拉票——既不更新任期，也不投票。
	//
	// 少了这条，一个刚从卡顿里回来的节点每次拉票都能把健康的 leader 拉下台：它自己
	// 选不上（日志更旧，granted=false），却因为任期更高而迫使 leader 降级，leader 随后
	// 超时重新当选，它再拉一次票……实测 GC 期间 7 秒内换届 4 次，全是这么来的。
	//
	// leader 用自己发出 AppendEntries 的时刻判断，follower 用收到的时刻；
	// 刚启动、还没和任何 leader 打过交道的节点不适用本规则（heardFromLeader 为假），
	// 否则首次选举会被各节点的启动时差平白推迟。
	knowsLeader := rf.role == ROLE_LEADER || rf.heardFromLeader
	if since := time.Since(rf.LastAppendTime); knowsLeader && since < minElectionTimeout {
		util.DPrintf("RaftNode[%d] 忽略 [%d] 的拉票(term=%d)：%v 前刚与 leader 通过消息",
			rf.me, args.CandidateId, args.Term, since.Round(time.Millisecond))
		return reply, nil
	}

	// 任期不如我大，拒绝投票
	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1      // 有问题，如果两个leader同时选举，那会进行多次投票，因为都满足下方的投票条件---没有问题，如果第二个来请求投票，此时args.Term = rf.currentTerm。因为rf.currentTerm已经更新
		rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
		// rf.leaderId = int(args.CandidateId) // 先假设这个即将成为leader
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
			rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，重置选举超时的时间
		}
	}
	util.DPrintf("RaftNode[%d] RequestVote from[%d] term[%d] cand(lastIdx=%d,lastTerm=%d) mine(lastIdx=%d,lastTerm=%d,votedFor=%d) granted=%v",
		rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, rf.lastIndex(), rf.lastTerm(), rf.votedFor, reply.VoteGranted)
	return reply, nil
}

// 已兼容snapshot
// lockStall reports an rf.mu acquisition slow enough to matter for the election timeout.
// The replication loop and the AppendEntries handler both have to take rf.mu, and
// lastActiveTime is only refreshed inside the handler: whoever holds the lock for more
// than the 3 s timeout costs the cluster its leader. Only outliers print.
func lockStall(who string, me int, start time.Time) {
	if d := time.Since(start); d > time.Second {
		fmt.Printf("[LOCK-STALL] RaftNode[%d] %s waited %v for rf.mu\n", me, who, d.Round(time.Millisecond))
	}
}

func (rf *Raft) AppendEntriesInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (*raftrpc.AppendEntriesInRaftResponse, error) {
	tLock := time.Now()
	rf.mu.Lock()
	lockStall("AppendEntries handler", rf.me, tLock)
	defer rf.mu.Unlock()

	// 选举超时衡量的是"多久没收到 leader 的消息"，不是"这次处理花了多久"。
	//
	// 只在进门时刷新 lastActiveTime 是不够的：本函数持着 rf.mu 把日志写进磁盘，
	// 而节点自身跑 GC 时磁盘被打满，一次调用可能耗上几秒。electionLoop 要等这把锁，
	// 拿到后算出的 elapses 正好是这次的处理耗时，于是节点把自己判成"收不到心跳"，
	// 升任期发起选举；leader 从 AppendEntries 的回复里看到更高任期只能降级——
	// §4.2.3 的防打扰规则管不到这条路径，因为打扰不是从投票进来的。
	// 实测第二轮 GC 期间 node1 就这样 "silent for 3.33s" 了一次。
	//
	// 离开时再刷新一次，把处理耗时排除在外。fromLeader 只在确认对方确实是当前任期的
	// leader 之后才置真，任期更旧的请求不该重置本节点的选举计时。
	tHandler := time.Now()
	fromLeader := false
	defer func() {
		if fromLeader {
			now := time.Now()
			rf.lastActiveTime = now
			rf.LastAppendTime = now
		}
		if d := time.Since(tHandler); d > time.Second {
			fmt.Printf("[SLOW-APPEND] RaftNode[%d] AppendEntries 处理耗时 %v（持锁全程）\n", rf.me, d.Round(time.Millisecond))
		}
	}()

	// util.DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
	// rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)
	reply := &raftrpc.AppendEntriesInRaftResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	// var logEntrys []*raftrpc.LogEntry
	// json.Unmarshal(args.Entries, &logEntrys)
	logEntrys := args.Entries
	// if len(logEntrys) != 0 { // 除去普通的心跳
	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
	rf.heardFromLeader = true      // §4.2.3 的判据之一：这个集群里确实有 leader 在工作
	// fmt.Println("重置lastAppendTime")
	// }

	// defer func() {
	// 	util.DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
	// 		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log), reply.ConflictIndex)
	// }()

	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
		// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
	}

	// 认识新的leader
	rf.leaderId = int(args.LeaderId)
	// 刷新活跃时间。离开时还会再刷一次，见函数开头的 defer。
	rf.lastActiveTime = time.Now()
	fromLeader = true
	if len(logEntrys) == 0 {
		reply.Success = true                           // 成功心跳
		if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
			rf.commitIndex = int(args.LeaderCommit)
			if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
				rf.commitIndex = rf.lastIndex()
			}
			rf.signalApply()
		}
		return reply, nil
	}

	if args.PrevLogIndex > int32(rf.lastIndex()) { // prevLogIndex位置没有日志的情况
		reply.ConflictIndex = int32(rf.lastIndex() + 1)
		return reply, nil
	}
	// prevLogIndex位置有日志，那么判断term必须相同，否则false
	if args.PrevLogIndex != 0 {
		prevTerm := rf.termAt(int(args.PrevLogIndex))
		if prevTerm == -1 { // 该位置已被本节点压缩，无法校验，要求leader从内存中保留的首条重发
			reply.ConflictIndex = int32(rf.firstIndex())
			return reply, nil
		}
		if prevTerm != int32(args.PrevLogTerm) { // prevLogIndex位置有日志，那么判断term必须相同，否则false
			reply.ConflictTerm = prevTerm
			// 找到冲突term的首次出现位置（不早于内存中保留的首条），最差就是PrevLogIndex
			reply.ConflictIndex = int32(rf.firstIndex())
			for index := rf.firstIndex(); index <= int(args.PrevLogIndex); index++ {
				if rf.termAt(index) == reply.ConflictTerm {
					reply.ConflictIndex = int32(index)
					break
				}
			}
			return reply, nil
		}
	}
	// fmt.Printf("此时同步的日志为%v\n",len(logEntrys))
	// 找到了第一个不同的index，开始同步日志
	// var tempLogs []*Entry // 自动会在写入磁盘文件后进行清零的操作
	// var entry Entry
	var index int
	var logPos int
	for i, logEntry := range logEntrys {
		if logEntry == nil || logEntry.GetCommand() == nil {
			util.EPrintf("RaftNode[%d] AppendEntries carried a nil entry or a nil command; skipping it", rf.me)
			continue
		}

		index = int(args.PrevLogIndex) + 1 + i
		logPos = rf.index2LogPos(index)
		entry := Entry{
			Index:       uint32(index), // use the index we computed, not the one in the command
			CurrentTerm: uint32(logEntry.Term),
			VotedFor:    uint32(rf.leaderId),
			Key:         logEntry.GetCommand().Key,
			Value:       logEntry.GetCommand().Value,
			NoOp:        logEntry.GetCommand().OpType == "TermLog",
		}
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
			// No-ops are written as well (keySize==0 markers), matching the leader, so the
			// on-disk log is complete and replayable.
			rf.batchLog = append(rf.batchLog, &entry) // 将要写入磁盘文件的结构体暂存，批量存储。

			if index == rf.lastIndex() { // 已经将日志补足后，开始批量写入
				// offsets1, err := rf.WriteEntryToFile(tempLogs, "./raft/RaftState.log", 0)
				// rf.mu.Unlock()
				rf.WriteEntryToFile(rf.batchLog, 0)
				rf.batchLog = rf.batchLog[:0] // 清空暂存日志的数组
			}
			// util.DPrintf("追加RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] Index[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, index, rf.Offsets)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				util.DPrintf("RaftNode[%d] conflicting entry at index %d: local term %d, leader term %d; truncating from here", rf.me, index, rf.log[logPos].Term, logEntry.Term)
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来

				// offset := rf.Offsets[index]      // 截取后面错误的offset
				offset := rf.Offsets[index-rf.shotOffset-1] // 这个要减一
				// offset := rf.Offsets[index-rf.shotOffset] // 将上面的改为加一了
				// rf.Offsets = rf.Offsets[:logPos] // 删除当前错误的offset，以及后续的所有
				rf.Offsets = rf.Offsets[:index-rf.shotOffset-1] // logPos 现在是相对基址的，改用绝对 index 推导
				if n := index - rf.shotOffset - 1; n <= len(rf.offsetVersions) {
					rf.offsetVersions = rf.offsetVersions[:n] // 与 Offsets 同步截断，否则两者错位
				}
				arrEntry := []*Entry{&entry} // 这里由于发生的情况较少，所以每次只写入一个日志到磁盘文件
				// offsets2, err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
				// rf.mu.Unlock()
				rf.WriteEntryToFile(arrEntry, offset)
			} // term一样啥也不用做，继续向后比对Log
		} // 每追加一个日志就持久化，并将offset和index绑定，存储到内存中。后续可以考虑这里实现批量持久化
	}
	// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)

	// 更新提交下标
	if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
		rf.commitIndex = int(args.LeaderCommit)
		if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
			rf.commitIndex = rf.lastIndex()
		}
		rf.signalApply()
	}
	reply.Success = true
	return reply, nil
}

func (rf *Raft) HeartbeatInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (*raftrpc.AppendEntriesInRaftResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &raftrpc.AppendEntriesInRaftResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
	rf.heardFromLeader = true      // §4.2.3 的判据之一：这个集群里确实有 leader 在工作
	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}
	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1
		rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
	}
	// 认识新的leader
	rf.leaderId = int(args.LeaderId)
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()
	reply.Success = true // 成功心跳
	return reply, nil
}

// 已兼容snapshot
func (rf *Raft) Start(command interface{}) (int32, int32, bool) {
	index := -1
	term := -1
	isLeader := true
	// var buffer bytes.Buffer
	// enc := gob.NewEncoder(&buffer)
	// var fileSizeLimit int64 = 10 * 1024 * 1024 // 6MB
	tBeforeLock := time.Now()
	rf.mu.Lock()
	tAfterLock := time.Now()
	var tWriteFile time.Duration
	defer func() {
		// 分三段记账：等锁、写文件、持锁总时长。
		recordWrite(tAfterLock.Sub(tBeforeLock), tWriteFile, time.Since(tAfterLock))
	}()

	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		// fmt.Println("到这了嘛3")
		rf.mu.Unlock()
		return -1, -1, false
	}
	// logEntry := LogEntry{
	// 	Command: command.(DetailCod),
	// 	Term:    int32(rf.currentTerm),
	// }
	logEntry := raftrpc.LogEntry{
		Command: command.(*raftrpc.DetailCod),
		Term:    int32(rf.currentTerm),
	}
	index = rf.lastIndex() + 1 // 加一是为了除去空指令
	term = rf.currentTerm
	// Fill Index/Term into the command before publishing it into rf.log. Callers used to
	// write op.Index/op.Term back after Start returned, by which time the replication
	// goroutine could already be gob-encoding the same struct: an unsynchronised write/read
	// of the same field, which -race reported.
	logEntry.Command.Index = int32(index)
	logEntry.Command.Term = int32(term)
	// fmt.Printf("11111offset%v,changdu%v\n",rf.Offsets,len(rf.Offsets))
	var myBatch *flushBatch
	var needSignal bool
	{
		// No-op entries (TermLog) are written too, as keySize==0 marker records, so the
		// on-disk log maps one-to-one onto rf.log and can be replayed on restart. Each takes
		// an Offsets slot like any other entry.
		// 必须是局部变量。原先用包级的 entry_global 取地址，在逐条立即写入、
		// 全程持锁的前提下没问题；一旦攒批，同一批里所有指针都会指向它，
		// 最后写出去的是同一条记录重复 N 次。
		entry := &Entry{
			Index:       uint32(index),
			CurrentTerm: uint32(term),
			VotedFor:    uint32(rf.leaderId),
			Key:         command.(*raftrpc.DetailCod).Key,
			Value:       command.(*raftrpc.DetailCod).Value,
			NoOp:        logEntry.Command.OpType == "TermLog",
		}
		if rf.groupCommit {
			myBatch, needSignal = rf.enqueueForFlush(entry)
		} else {
			tw := time.Now()
			rf.WriteEntryToFile([]*Entry{entry}, 0)
			tWriteFile = time.Since(tw)
		}
	}
	rf.log = append(rf.log, &logEntry) // 确保日志落盘之后，再更新log
	rf.mu.Unlock()

	// 立刻叫醒提交检查，不必等它下一次轮询。单节点下 commitIndex 只由那个
	// 循环推进，纯轮询意味着每条日志平均要多等 5ms 才可能被提交。
	rf.signalCommit()

	// 攒批模式下在锁外等待本批落盘：Start 依旧在日志持久化之后才返回，
	// 但等待磁盘的时间不再占着 rf.mu，其余请求可以继续进来凑同一批。
	if myBatch != nil {
		if needSignal {
			select {
			case rf.flushSignal <- struct{}{}:
			default: // flusher 已被唤醒，无需重复投递
			}
		}
		tw := time.Now()
		<-myBatch.done
		tWriteFile = time.Since(tw)
	}

	// util.DPrintf("RaftNode[%d] Add Command, logIndex[%d] currentTerm[%d]", rf.me, index, term)
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

		util.DPrintf("RaftNode[%d] Raft gRPC server stopped", rf.me)
		break
	}
}

// sendRequestVote uses the same per-peer connection pool as AppendEntries. Every election
// used to grpc.Dial a fresh connection with WithBlock and no deadline; against a dead peer
// that call never returns, taking the goroutine and its vote with it. Through the pool an
// unreachable peer fails fast, so the tally learns that the vote is gone.
func (rf *Raft) sendRequestVote(peerId int, args *raftrpc.RequestVoteRequest) (bool, *raftrpc.RequestVoteResponse) {
	conn, err := rf.pools[peerId].Get()
	if err != nil {
		util.EPrintf("RequestVote: failed to get conn to %v: %v", rf.peers[peerId], err)
		return false, nil
	}
	defer conn.Close()
	client := raftrpc.NewRaftClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	reply, err := client.RequestVote(ctx, args)
	if err != nil {
		util.EPrintf("RequestVote to %v failed: %v", rf.peers[peerId], err)
		return false, nil
	}
	return true, reply
}

func (rf *Raft) sendAppendEntries(address string, args *raftrpc.AppendEntriesInRaftRequest, p pool.Pool) (*raftrpc.AppendEntriesInRaftResponse, bool) {
	// 用grpc连接池同步日志
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
		return nil, false

	}
	defer conn.Close()
	client := raftrpc.NewRaftClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.AppendEntriesInRaft(ctx, args)

	if err != nil {
		// util.EPrintf("Error calling AppendEntriesInRaft method on server side; err:%v; address:%v ", err, address)
		return reply, false
	}
	return reply, true
}

func (rf *Raft) sendHeartbeat(address string, args *raftrpc.AppendEntriesInRaftRequest, p pool.Pool) (*raftrpc.AppendEntriesInRaftResponse, bool) {
	// 用grpc连接池同步日志
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
		return nil, false

	}
	defer conn.Close()
	client := raftrpc.NewRaftClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()
	reply, err := client.HeartbeatInRaft(ctx, args)

	if err != nil {
		// util.EPrintf("Error calling HeartbeatInRaft method on server side; err:%v; address:%v ", err, address)
		return reply, false
	}
	return reply, true
}

func (rf *Raft) AppendMonitor() {
	timeout := 3 * time.Second
	for {
		time.Sleep(2 * time.Second)
		rf.mu.Lock()
		silent := time.Since(rf.LastAppendTime) > timeout
		last := rf.lastIndex()
		rf.mu.Unlock()
		if silent && rf.GetLeaderId() != int32(rf.me) {
			//  排在第一的服务器和后面的服务器，打印的内容是不一样的。因为排在第一个的默认就是满足第二个条件了。
			fmt.Println("3秒没有收到来自leader的同步或者心跳信息！")
			continue
		}
		fmt.Printf("当前的log大小%v\n", last)
	}
}

func (rf *Raft) electionLoop() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond) // 每隔一小段时间，检查是否超时，也就是说follower如果变成candidate，还得等10ms才能开始选举

		func() {
			rf.mu.Lock()
			// fmt.Println("拿到electionLoop的锁1或者2或者3")
			defer rf.mu.Unlock()
			// fmt.Println("释放electionLoop的锁1或者")
			now := time.Now()
			timeout := minElectionTimeout + time.Duration(rand.Int31n(int32(electionTimeoutJitter/time.Millisecond)))*time.Millisecond
			elapses := now.Sub(rf.lastActiveTime)
			// follower -> candidates
			if rf.role == ROLE_FOLLOWER {
				if elapses >= timeout {
					rf.role = ROLE_CANDIDATES
					util.DPrintf("RaftNode[%d] Follower -> Candidate (silent for %v)", rf.me, elapses.Round(time.Millisecond))
				}
			}
			// 请求vote，当变成candidate后，等待10ms才进入到该if语句
			if rf.role == ROLE_CANDIDATES && elapses >= timeout {
				rf.lastActiveTime = time.Now() // 重置下次选举时间
				rf.currentTerm += 1            // 发起新任期
				rf.votedFor = rf.me            // 该任期投了自己
				rf.persistHardState()          // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)

				// 请求投票req
				args := raftrpc.RequestVoteRequest{
					Term:         int32(rf.currentTerm),
					CandidateId:  int32(rf.me),
					LastLogIndex: int32(rf.lastIndex()),
				}
				args.LastLogTerm = int32(rf.lastTerm())

				rf.mu.Unlock() // 对raft的修改操作已经暂时结束，可以解锁

				// util.DPrintf("RaftNode[%d] RequestVote starts, Term[%d] LastLogIndex[%d] LastLogTerm[%d]", rf.me, args.Term,
				// args.LastLogIndex, args.LastLogTerm)
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
						if ok, reply := rf.sendRequestVote(id, &args); ok {
							voteResultChan <- &VoteResult{peerId: id, resp: reply}
						} else {
							voteResultChan <- &VoteResult{peerId: id, resp: nil}
						}
					}(peerId)
				}

				maxTerm := 0
				voteDeadline := time.After(timeout)
				if voteCount > len(rf.peers)/2 {
					goto VOTE_END
				}
				// The tally must not wait forever. It used to exit only on "everyone answered"
				// or "majority reached"; with one peer permanently silent (crashed, partitioned)
				// and the live vote lost to an RPC timeout, the candidate blocked here for good,
				// neither retrying nor stepping down. That is the 65-second leaderless window
				// seen after killing the leader of a -race build. On expiry go to VOTE_END with
				// the votes collected; without a majority the loop times out again and retries.
				for {
					select {
					case <-voteDeadline:
						util.DPrintf("RaftNode[%d] election term[%d] timed out with votes=%d answered=%d", rf.me, args.Term, voteCount, finishCount)
						goto VOTE_END
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
				rf.mu.Lock()
				util.DPrintf("RaftNode[%d] election term[%d] votes=%d/%d answered=%d maxTerm=%d role=%s",
					rf.me, rf.currentTerm, voteCount, len(rf.peers), finishCount, maxTerm, rf.role)
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower；这个是不是可以在接受投票时就判断，如果有任期比自己大的，就直接转换为follower，也不看投票结果了
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = 0
					rf.currentTerm = maxTerm // 更新自己的Term和voteFor
					rf.votedFor = -1
					rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
					// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					util.DPrintf("RaftNode[%d] Candidate -> Leader", rf.me)
					rf.wakeReplication() // 立刻开工，不必等复制循环的兜底 tick

					rf.leaderId = rf.me
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

					op := raftrpc.DetailCod{
						OpType: "TermLog",
					}
					rf.mu.Unlock()
					rf.Start(&op) // the no-op for the new term, after nextIndex is initialised; Start fills Index/Term
					rf.mu.Lock()
					util.DPrintf("成为leader后发送第一个空指令给Raft层")
					// rf.lastBroadcastTime = time.Unix(0, 0) // 令appendEntries广播立即执行，因为leader的term开始时，需要提交一条空的无操作记录。
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
	// newCommitIndex := sortedMatchIndex[len(sortedMatchIndex)-1]
	// fmt.Printf("newconmittindex%v\n",newCommitIndex)
	// if语句的第一个条件则是排除掉还没有复制到大多数server的情况
	// fmt.Printf("此时log的长度：%v以及newcommitindex的值：%v\n",len(rf.log),newCommitIndex)
	if newCommitIndex > rf.commitIndex && rf.termAt(newCommitIndex) == int32(rf.currentTerm) {
		rf.commitIndex = newCommitIndex // 保证是当前的Term才能根据同步到server的副本数量判断是否可以提交
		rf.signalApply()
		// fmt.Println("上任空包被提交了")	// 提交了的，因为虽然是空包，但是也赋予了当前任期，满足提交条件
	}
	// util.DPrintf("RaftNode[%d] updateCommitIndex, newCommitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

// armSync 归还一轮复制的回执，并唤醒复制循环。
// 所有写 SyncChans 的地方都走这里：漏掉唤醒只会让那个 peer 等到下一次兜底醒来，
// 漏掉回执则会让它的复制链彻底熄火（见 appendEntriesLoop 的注释）。
func (rf *Raft) armSync(peerId int, val string) {
	rf.SyncChans[peerId] <- val
	rf.wakeReplication()
}

// wakeReplication 叫醒 appendEntriesLoop。非阻塞：信号只表示"有活干"。
func (rf *Raft) wakeReplication() {
	select {
	case rf.replicaWake <- struct{}{}:
	default:
	}
}

// 已兼容snapshot
func (rf *Raft) doAppendEntries(peerId int) {
	var buffer bytes.Buffer
	enc := gob.NewEncoder(&buffer)
	var totalSize int64

	// The request must be assembled under rf.mu: currentTerm, commitIndex, nextIndex, log
	// and lastIncludedIndex are all modified by other goroutines inside the lock. This used
	// to read them unlocked, and -race showed two consequences: a reallocating append in
	// Start can expose a torn slice header here (out of range, or a nil entry; the old
	// "rf.log[i] == nil" check was a band-aid for exactly that), and compactLog swapping the
	// backing array and advancing lastIncludedIndex are two steps, so a position computed in
	// between maps to the wrong entries, which a follower cannot detect when terms match.
	// Only the entry pointers are copied inside the lock; gob encoding and the RPC happen
	// outside it, so Start is not slowed down.
	args := raftrpc.AppendEntriesInRaftRequest{}
	var candidates []*raftrpc.LogEntry
	tLock := time.Now()
	rf.mu.Lock()
	lockStall("doAppendEntries", rf.me, tLock)
	args.Term = int32(rf.currentTerm)
	args.LeaderId = int32(rf.me)
	args.LeaderCommit = int32(rf.commitIndex)
	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1) // 减一是为了拿到下标

	// The first entry to send must still be in memory. When nextIndex falls inside the
	// compacted range (including PrevLogIndex==0 on a node that has compacted) skip this
	// round; compaction is bounded by matchIndex so this should not happen in practice.
	start := rf.index2LogPos(rf.nextIndex[peerId])
	if start < 0 {
		util.DPrintf("RaftNode[%d] peer[%d] nextIndex[%d] 落后于已压缩点[%d]，跳过本轮日志同步",
			rf.me, peerId, rf.nextIndex[peerId], rf.lastIncludedIndex)
		rf.mu.Unlock()
		go func(id int) { rf.armSync(id, strconv.Itoa(id)) }(peerId)
		return
	}

	if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
		args.PrevLogTerm = 0
	} else {
		prevTerm := rf.termAt(int(args.PrevLogIndex))
		if prevTerm == -1 {
			util.DPrintf("RaftNode[%d] peer[%d] PrevLogIndex[%d] 已压缩，跳过本轮日志同步",
				rf.me, peerId, args.PrevLogIndex)
			rf.mu.Unlock()
			go func(id int) { rf.armSync(id, strconv.Itoa(id)) }(peerId)
			return
		}
		args.PrevLogTerm = prevTerm
	}
	if start < len(rf.log) {
		candidates = make([]*raftrpc.LogEntry, len(rf.log)-start)
		copy(candidates, rf.log[start:])
	}
	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
	rf.mu.Unlock()

	// Cap the batch by encoded size: past threshold only the prefix is sent. Entries are
	// immutable once published into rf.log, so encoding outside the lock is safe.
	n := len(candidates)
	for i, e := range candidates {
		if err := enc.Encode(e); err != nil { // 将日志项编码后的字节序列写入到 buffer 缓冲区中
			fmt.Println("Encode error：", err)
		}
		totalSize += int64(buffer.Len())
		if totalSize >= threshold {
			n = i // exclusive
			break
		}
	}
	buffer.Reset()
	appendLog := candidates[:n]
	args.Entries = appendLog

	go func(peerId int) {
		// util.DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args.Term, args.LeaderId)
		// T2开始 - 分发日志阶段
		// t2Start := time.Now()
		if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, rf.pools[peerId]); ok {
			if len(args.Entries) != 0 {
				// t2End := time.Now()
				// t2Duration := t2End.Sub(t2Start)
				// fmt.Println("T2(Distribution) duration:", t2Duration)
				// fmt.Println("此次分发的日志条数：\n", len(args.Entries))
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			// defer func() {
			// 	util.DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
			// 		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			// }()

			// 如果不是rpc前的leader状态了，那么啥也别做了，可能遇到了term更大的server，因为rpc的时候是没有加锁的
			if rf.currentTerm != int(args.Term) {
				rf.armSync(peerId, "NotLeader")
				fmt.Printf("rf.currentTerm-%v,args.Term-%v\n", rf.currentTerm, args.Term)
				return
			}
			if reply.Term > int32(rf.currentTerm) { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = 0
				rf.currentTerm = int(reply.Term)
				rf.votedFor = -1
				rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
				rf.armSync(peerId, "NotLeader")
				fmt.Printf("reply.Term-%v,rf.currentTerm-%v\n", reply.Term, rf.currentTerm)
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功
				// fmt.Printf("T2(Distribution):%v T3(Consensus):%v\n",
				// t2Duration, time.Since(t2Start))
				rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(appendLog) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 // 记录已经复制到其他server的日志的最后index的情况
				rf.updateCommitIndex()                           // 更新commitIndex
			} else {
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				// nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index >= int32(rf.firstIndex()); index-- {
						// if rf.log[rf.index2LogPos(int(index))].Term == reply.ConflictTerm {
						// 	conflictTermIndex = int(index)
						// 	break
						// }
						// 我认为下方这个效果更好，这样PrevLogIndex的值就为 index
						if rf.termAt(int(index)) != reply.ConflictTerm {
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
				// util.DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
			// rf.SyncChans[peerId] <- rf.peers[peerId]
			rf.armSync(peerId, strconv.Itoa(peerId))
		} else {
			// rf.SyncChans[peerId] <- rf.peers[peerId]	// 同步日志失败也要重新发起日志同步
			rf.armSync(peerId, strconv.Itoa(peerId))
		}
	}(peerId)
}

func (rf *Raft) CheckActive(peerId int, resultChan chan<- bool) {
	args := raftrpc.AppendEntriesInRaftRequest{}
	args.Term = int32(rf.currentTerm)
	args.LeaderId = int32(rf.me)
	args.LeaderCommit = int32(rf.commitIndex)
	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)
	if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = rf.termAt(int(args.PrevLogIndex))
		if args.PrevLogTerm == -1 { // 已压缩；心跳不携带日志，follower 不校验该字段
			args.PrevLogTerm = rf.lastIncludedTerm
		}
	}
	args.Entries = []*raftrpc.LogEntry{}
	if reply, ok := rf.sendHeartbeat(rf.peers[peerId], &args, rf.pools[peerId]); ok {
		rf.mu.Lock()
		// defer rf.mu.Unlock()
		if rf.currentTerm != int(args.Term) {
			rf.mu.Unlock()
			return
		}
		if reply.Term > int32(rf.currentTerm) { // 变成follower
			rf.role = ROLE_FOLLOWER
			// rf.leaderId = 0
			rf.currentTerm = int(reply.Term)
			rf.votedFor = -1
			rf.persistHardState() // Raft 要求 term/votedFor 落盘后再应答或发起 RPC
			// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
			rf.mu.Unlock()
			return
		}
		if reply.Success {
			// fmt.Printf("receive true from node %v\n", peerId)
			resultChan <- true
		} else {
			// fmt.Printf("receive false from node %v\n", peerId)
			resultChan <- false
		}
		rf.mu.Unlock()
	} else {
		fmt.Printf("Failed to send heartbeat to node %v\n", peerId)
		resultChan <- false
		return
	}
}

func (rf *Raft) GetReadIndex() (commitindex int, isleader bool) {
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 只有leader才执行，如果不是就返回false
	if rf.role != ROLE_LEADER {
		// fmt.Println("到这了嘛3")
		rf.mu.Unlock()
		return -1, false
	}
	rf.mu.Unlock()

	resultChan := make(chan bool, len(rf.peers)) // 设置为集群中服务器的数量以确保不会被阻塞
	var wg sync.WaitGroup

	for peerId := 0; peerId < len(rf.peers); peerId++ {
		if peerId == rf.me {
			continue
		}
		wg.Add(1)
		go func(peerId int) {
			defer wg.Done()
			rf.CheckActive(peerId, resultChan)
		}(peerId)
	}

	// 使用goroutine等待所有的心跳请求完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	successCount := 0
	for result := range resultChan {
		if result {
			successCount++
		}
	}

	if successCount+1 > len(rf.peers)/2 {
		// log.Printf("Majority of nodes responded. Current commit index: %d", rf.commitIndex)
		return rf.commitIndex, true
	}

	fmt.Println("Failed to get majority response")
	return -1, false // 表示失败，同时也不是合格的leader
}

// heartbeatInterval 是 leader 无条件向每个 peer 发心跳的间隔。
// 必须远小于 minElectionTimeout：follower 的选举计时靠收到 leader 的消息来重置。
const heartbeatInterval = 500 * time.Millisecond

// replicaIdleTick 是复制循环在没有任何回执时的兜底醒来间隔。
// 有 replicaWake 唤醒时用不到它，它只防信号与检查错开导致的睡死。
const replicaIdleTick = 20 * time.Millisecond

// 选举超时：最小值加一段随机抖动，避免多个节点同时发起选举。
// minElectionTimeout 同时是 Raft 论文 §4.2.3 防打扰规则的判据（见 RequestVote）。
const (
	minElectionTimeout    = 3000 * time.Millisecond
	electionTimeoutJitter = 150 * time.Millisecond
)

// defaultAppendRoundBudget 是一轮复制在被判定为"卡住、需要重发"之前的时限。
// 取得比 sendAppendEntries 的 5 秒 RPC 上限宽，好让正常的慢回复自己走完。
const defaultAppendRoundBudget = 8 * time.Second

// appendEntriesLoop 在本节点是 leader 期间，持续向每个 peer 复制日志/心跳。
//
// 复制靠一条自续的链：一轮 doAppendEntries 的回复协程把 peer 号写回
// SyncChans[peer]，本循环收到后发起下一轮。心跳间隔因此等于一次 RPC 往返，
// 没有独立的定时器。
//
// 这条链此前只被点火一次：一个进程级的 First 标志在本节点第一次成为 leader 时
// 给每个 peer 发一轮，随后置 false 且再不复位。可是 RPC 在飞行途中若 term 变了，
// 回复协程写回的是 "NotLeader" 而非 peer 号，循环见到它就退出本轮——该 peer 的
// 链就此熄火。等这个节点再次当选，First 早已用掉，链不会重新点火，于是它虽然是
// leader 却一条 AppendEntries 都发不出去。follower 每 3 秒选举超时一次，选出的
// 还是这个发不出心跳的节点，集群就永久卡在换届里。
//
// 实测：三节点、4KB value、GC 阈值 2GB，第一轮 GC 造成一次 term 变动之后，
// node0 日志里 "leaving the replication loop" 恰好两条（两个 peer 各一条），
// 此后它连任 108 次而没有任何复制。GC 只是触发器，任何造成 term 变动的抖动都够。
//
// 改为按 peer 记录"有没有一轮在飞"：链断了就在下一次循环里自动补发，降级再当选
// 也会从头点火。inFlight 只在本 goroutine 里读写，不需要额外的锁。
func (rf *Raft) appendEntriesLoop() {
	// 测试用 sendAppend 顶掉真正的复制调用，以便在不起 gRPC 的前提下驱动本循环；
	// 生产路径上它是 nil，直接走 doAppendEntries。
	send := rf.sendAppend
	if send == nil {
		send = rf.doAppendEntries
	}
	// 两个字段都只在起循环之前赋值（生产路径上不赋值），这里各读一次存成局部量，
	// 之后不再碰它们——单测调小预算时才不会与本 goroutine 形成竞态。
	budget := rf.appendRoundBudget
	if budget <= 0 {
		budget = defaultAppendRoundBudget
	}
	inFlight := make([]bool, len(rf.peers))
	sentAt := make([]time.Time, len(rf.peers))
	wasLeader := false
	for !rf.killed() {
		rf.mu.Lock()
		isLeader := rf.role == ROLE_LEADER
		empty := rf.lastIndex() == 0
		rf.mu.Unlock()

		if !isLeader {
			if wasLeader {
				// 卸任：丢掉本任期的记账，并排空channel里迟到的回执，
				// 免得下个任期把它们错当成"这一轮已经回来了"。
				for i := range inFlight {
					inFlight[i] = false
					for len(rf.SyncChans[i]) > 0 {
						<-rf.SyncChans[i]
					}
				}
				wasLeader = false
			}
			rf.waitReplicaWake(replicaIdleTick)
			continue
		}
		wasLeader = true
		if empty {
			rf.waitReplicaWake(replicaIdleTick)
			continue
		}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			// 先收回执。"NotLeader" 与 peer 号一样都表示这一轮已经结束，
			// 区别只在于要不要继续发——那由上面的 isLeader 决定，不在这里判断。
			for done := false; !done; {
				select {
				case <-rf.SyncChans[i]:
					inFlight[i] = false
				default:
					done = true
				}
			}
			// 看门狗：一轮最多占 appendRoundBudget，超时就重发。RPC 自己有 5 秒上限，
			// 所以正常情况下轮不到它；它防的是"某条路径忘了写回执"这类漏洞——本函数
			// 注释里那个 bug 正是这一类，而链一旦熄火，代价是集群不再有 leader。
			// AppendEntries 是幂等的，多发一轮不会有副作用。
			if inFlight[i] && time.Since(sentAt[i]) > budget {
				util.DPrintf("RaftNode[%d] peer %d 的复制轮次超过 %v 未回执，重发", rf.me, i, budget)
				inFlight[i] = false
			}
			if !inFlight[i] {
				inFlight[i] = true
				sentAt[i] = time.Now()
				send(i)
			}
		}

		// 等回执，别空转。此前这里没有任何等待，循环每次迭代都要取一次 rf.mu，
		// 与写入路径争同一把锁，白烧一个核；follower 上更是纯粹的空转。
		// 回执一到就唤醒，所以复制延迟不受影响；兜底的 tick 只防信号错过。
		rf.waitReplicaWake(replicaIdleTick)
	}
}

// waitReplicaWake 等一次复制唤醒，最多等 d。
func (rf *Raft) waitReplicaWake(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-rf.replicaWake:
	case <-t.C:
	}
}

// maxApplyBatch 限制一次持锁期间取走多少条待应用的日志。
// 取太小则锁开销摊不薄，取太大则一次持锁时间过长、反过来挡住写入路径。
const maxApplyBatch = 64

// signalApply 通知 applyLogLoop 有新提交。调用方须持有 rf.mu。
// 非阻塞：信号只表示"有活干"，多次提交合并成一次唤醒即可。
func (rf *Raft) signalApply() {
	select {
	case rf.applySignal <- struct{}{}:
	default:
	}
}

// applyLogLoop 把已提交的日志送往上层状态机。
//
// 此前的写法有三处会直接压住写入延迟：
//
//  1. 没有待应用日志时 time.Sleep(10ms) 再重新检查。一条刚提交的日志因此最多
//     干等 10ms——实测单客户端下写日志 1.05ms、等 apply 14.48ms，就是它。
//  2. 一次循环只应用一条，每条都完整 Lock/Unlock 一次 rf.mu，而那正是
//     raft.Start 写日志时持有的锁。
//  3. 往 applyCh 发送是在持锁状态下做的，而该通道容量仅为 3。上层稍慢，
//     这里就持锁阻塞，所有 raft.Start 一并卡住。
//
// 改为：等信号而非轮询；一次持锁取走一批；发送在锁外进行。
// 应用顺序仍严格按 index 递增——批内保持取出顺序，批间由 lastApplied 串联。
func (rf *Raft) applyLogLoop() {
	batch := make([]ApplyMsg, 0, maxApplyBatch)
	for !rf.killed() {
		batch = batch[:0]

		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			for len(batch) < maxApplyBatch && rf.commitIndex > rf.lastApplied {
				nextApplied := rf.lastApplied + 1
				appliedIndex := rf.index2LogPos(nextApplied)
				if appliedIndex < 0 || appliedIndex >= len(rf.log) {
					// 压缩点由 compactLog 限制在 lastApplied 之前，正常不会发生
					util.DPrintf("RaftNode[%d] applyLogLoop: index[%d] 越界(pos=%d, len=%d, base=%d)",
						rf.me, nextApplied, appliedIndex, len(rf.log), rf.lastIncludedIndex)
					return
				}
				cmd := rf.log[appliedIndex].Command

				if cmd.OpType == "TermLog" {
					// A TermLog now has a marker record in the file and a slot in Offsets;
					// consume the slot here but pass no offset up (the KV layer ignores TermLog).
					if len(rf.Offsets) == 0 {
						return
					}
					rf.lastApplied = nextApplied
					rf.shotOffset++
					rf.Offsets = rf.Offsets[1:]
					if len(rf.offsetVersions) > 0 {
						rf.offsetVersions = rf.offsetVersions[1:]
					}
					batch = append(batch, ApplyMsg{
						CommandValid: true,
						Command:      cmd,
						CommandIndex: nextApplied,
						CommandTerm:  int(rf.log[appliedIndex].Term),
						Offset:       0,
					})
					continue
				}

				if (rf.lastApplied - rf.shotOffset) >= len(rf.Offsets) {
					// 偏移还没写进来，等下一轮
					return
				}
				rf.lastApplied = nextApplied
				realIndex := rf.lastApplied - rf.shotOffset
				var ver int32
				if realIndex-1 < len(rf.offsetVersions) {
					ver = rf.offsetVersions[realIndex-1]
				}
				batch = append(batch, ApplyMsg{
					CommandValid: true,
					Command:      cmd,
					CommandIndex: rf.lastApplied,
					CommandTerm:  int(rf.log[appliedIndex].Term),
					Offset:       rf.Offsets[realIndex-1],
					FileVersion:  ver,
				})
				rf.Offsets = rf.Offsets[1:]
				if len(rf.offsetVersions) > 0 {
					rf.offsetVersions = rf.offsetVersions[1:]
				}
				rf.shotOffset++
				if rf.Gap > 0 && rf.lastApplied%rf.Gap == 0 {
					util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, len(rf.Offsets))
				}
			}
		}()

		if len(batch) == 0 {
			// 没有待应用的日志，等提交端唤醒。
			// 仍留一个兜底超时：信号在极端时序下可能与这里的检查错开，
			// 有它就不会永久睡死，代价只是空转一次。
			select {
			case <-rf.applySignal:
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}

		// 锁外发送：applyCh 容量很小，持锁发送会把写入路径一起堵住。
		for i := range batch {
			rf.applyCh <- batch[i]
		}
	}
}

func (rf *Raft) SetOriginalLog(filename string) {
	// 1. 获取当前代码文件 (raft.go) 的绝对路径
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("无法获取源代码路径")
	}

	// 2. 获取 raft.go 所在的目录 (即 .../Nezha/raft/)
	raftDir := filepath.Dir(currentFile)

	// 3. 将文件名拼接到该绝对路径下
	absPath := filepath.Join(raftDir, filename)
	rf.originalLog = absPath

	// 4. 确保目录存在
	if err := os.MkdirAll(raftDir, 0755); err != nil {
		log.Printf("无法创建目录: %v", err)
		return
	}

	// 5. 创建或覆盖文件
	file, err := os.Create(absPath)
	if err != nil {
		log.Printf("无法在路径 %s 创建文件: %v", absPath, err)
		return
	}
	file.Close()

	// 打印绝对路径，方便你检查
	fmt.Printf("RaftNode[%d] 原始日志绝对路径: %s\n", rf.me, absPath)
}

// firstIndex 返回内存中仍保留的第一条日志的 index
func (rf *Raft) firstIndex() int {
	return rf.lastIncludedIndex + 1
}

// signalCommit 通知 commitIndexUpdateLoop 有新日志待提交。
// 非阻塞，多次追加合并成一次唤醒。
func (rf *Raft) signalCommit() {
	select {
	case rf.commitSignal <- struct{}{}:
	default:
	}
}

// commitIndexUpdateLoop 推进 commitIndex。
//
// 原先纯靠每 10ms 一次的轮询。单节点下没有 follower、不走日志复制，
// commitIndex 完全由这个定时器驱动，于是一条刚写完的日志平均要等 5ms、
// 最多 10ms 才被提交——写入延迟里最大的一段固定开销。
//
// 改为等 raft.Start 的信号。保留 10ms 兜底：follower 的 matchIndex 是由
// 复制流程在别处推进的，那条路径不发信号，靠轮询兜住。
func (rf *Raft) commitIndexUpdateLoop() {
	for !rf.killed() {
		select {
		case <-rf.commitSignal:
		case <-time.After(10 * time.Millisecond):
		}

		rf.mu.Lock()
		if rf.role == ROLE_LEADER {
			rf.updateCommitIndex()
		}
		rf.mu.Unlock()
	}
}

// 服务器地址数组；当前方法对应的服务器地址数组中的下标；持久化存储了当前服务器状态的结构体；传递消息的通道结构体
// Make constructs the node and loads persisted state; it starts no goroutine and no gRPC
// server. The caller finishes log recovery (RecoverLog) and attaches the log file, then
// calls StartLoops; otherwise election, replication and apply would run against a log that
// is not rebuilt yet. An empty stateFile disables term/vote persistence.
func Make(peers []string, me int,
	persister *Persister, applyCh chan ApplyMsg, stateFile string) *Raft {
	rf := &Raft{}
	rf.stateFile = stateFile
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	for i := 0; i < len(peers); i++ {
		rf.SyncChans = append(rf.SyncChans, make(chan string, 1000))
	}
	rf.replicaWake = make(chan struct{}, 1)

	rf.role = ROLE_FOLLOWER
	rf.leaderId = 0
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
	if stateFile != "" {
		hs, ok, err := loadHardState(stateFile)
		if err != nil {
			log.Fatalf("read Raft state file: %v", err)
		}
		if ok {
			rf.stateLoaded = true
			rf.currentTerm = hs.CurrentTerm
			rf.votedFor = hs.VotedFor
			rf.fileBaseIndex = hs.BaseIndex
			rf.fileBaseTerm = hs.BaseTerm
			rf.lastIncludedIndex = hs.BaseIndex
			rf.lastIncludedTerm = hs.BaseTerm
			util.DPrintf("RaftNode[%d] loaded persisted state: term=%d votedFor=%d base=(%d,%d)", me, hs.CurrentTerm, hs.VotedFor, hs.BaseIndex, hs.BaseTerm)
		}
	}
	rf.applyCh = applyCh
	rf.applySignal = make(chan struct{}, 1)
	rf.commitSignal = make(chan struct{}, 1)
	// rf.SetOriginalLog("originalKvs.log")
	// 这里曾经 append 过一个哨兵 0，靠 index=1 的 TermLog 把它消费掉来对齐偏移量。
	// 该方案只对第一个 TermLog 成立：每次重新选主都会产生新的 TermLog，第二个就会吃掉一个
	// 真实偏移量，使其后所有 key 的偏移量整体错开一条 entry。
	// 现在的不变式是 Offsets[0] 恒对应日志下标 shotOffset+1，TermLog 只递增 shotOffset 而不出队，
	// 因此不需要哨兵。

	// 这就是自己修改grpc线程池option参数的做法
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              150,
		MaxActive:            300,
		MaxConcurrentStreams: 800,
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
	rf.LastAppendTime = time.Now()
	return rf
}

// StartLoops starts the gRPC server and every background loop. Call it only after log
// recovery has finished.
// heartbeatLoop 按固定间隔发心跳，与日志复制分开。
//
// 复制链是"一轮回执触发下一轮"，同一个 peer 同时只有一轮在飞，心跳间隔因此等于
// 一次 AppendEntries 的往返。三个节点同时 GC 时磁盘被打满，实测一次往返能到 5~6 秒，
// 超过 3 秒的选举超时：follower 判定失联、升任期发起选举，leader 从回复里看到更高
// 任期只能降级，白白换一次届。
//
// 日志复制慢是可以接受的——写入本来就在等磁盘；但"集群里还有没有 leader"这件事
// 不该由磁盘快慢来回答。所以心跳走独立的定时器和独立的轻量 RPC：HeartbeatInRaft
// 不带日志、不写盘，只刷新对方的选举计时。
func (rf *Raft) heartbeatLoop() {
	tick := time.NewTicker(heartbeatInterval)
	defer tick.Stop()
	for !rf.killed() {
		<-tick.C

		rf.mu.Lock()
		if rf.role != ROLE_LEADER {
			rf.mu.Unlock()
			continue
		}
		args := raftrpc.AppendEntriesInRaftRequest{
			Term:         int32(rf.currentTerm),
			LeaderId:     int32(rf.me),
			LeaderCommit: int32(rf.commitIndex),
		}
		// leader 自己的"最近与 leader 通过消息"就是它自己发出去的这一刻，
		// §4.2.3 的判据要用（见 RequestVote）。复制链卡住时它不会被刷新，
		// 那正是不该再拿它当"leader 健在"证据的时候。
		rf.LastAppendTime = time.Now()
		rf.mu.Unlock()

		for peerId := 0; peerId < len(rf.peers); peerId++ {
			if peerId == rf.me {
				continue
			}
			go func(id int) {
				reply, ok := rf.sendHeartbeat(rf.peers[id], &args, rf.pools[id])
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > int32(rf.currentTerm) { // 任期落后，退位
					rf.role = ROLE_FOLLOWER
					rf.currentTerm = int(reply.Term)
					rf.votedFor = -1
					rf.persistHardState()
				}
			}(peerId)
		}
	}
}

func (rf *Raft) StartLoops(ctx context.Context) {
	rf.mu.Lock()
	rf.lastActiveTime = time.Now() // recovery may have taken a while; do not time out immediately
	rf.LastAppendTime = time.Now()
	rf.mu.Unlock()
	go rf.RegisterRaftServer(ctx, rf.peers[rf.me])
	// election
	go rf.electionLoop()
	// sync
	go rf.appendEntriesLoop()
	// 心跳：与复制分开，保证"还有没有 leader"这件事不受磁盘快慢影响
	go rf.heartbeatLoop()
	// apply
	go rf.applyLogLoop()
	// 检查有没有收到日志同步的消息，若没有则连接有问题
	go rf.AppendMonitor()

	go rf.commitIndexUpdateLoop()

	go rf.compactLog()

	// 设置一个定时器，每十秒检查一次条件
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	go func() {
		for range ticker.C {
			if rf.killed() { // 如果上次KVS关闭了Raft，则可以关闭pool
				for _, pool := range rf.pools {
					pool.Close()
				}
				util.DPrintf("The raft pool has been closed")
				util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.Offsets)
				break
			}
		}
		util.DPrintf("Raft has been closed")
	}()
}
