package raft

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"strconv"

	// "encoding/gob"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JasonLou99/Hybrid_KV_Store/pool"
	// "github.com/JasonLou99/Hybrid_KV_Store/raft"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/raftrpc"

	// "github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"
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
}

// 当前角色
const ROLE_LEADER = "Leader"
const ROLE_FOLLOWER = "Follower"
const ROLE_CANDIDATES = "Candidates"

// doappendentries函数内
var threshold int64 = 30 * 1024 * 1024
var buffer bytes.Buffer
// start函数内
var entry Entry
var arrEntry []*Entry

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []string   // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	dead      int32      // set by Kill()

	currentTerm int        // 见过的最大任期
	votedFor    int        // 记录在currentTerm任期投票给谁了
	log         []LogEntry // 操作日志

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
	// kvrpc.UnimplementedKVServer
	raftrpc.UnimplementedRaftServer
	LastAppendTime time.Time
	Gap            int
	Offsets        []int64
	shotOffset     int
	SyncTime       int
	SyncChans      []chan string
	batchLog       []*Entry
	appendLog      []LogEntry
	appendLogSize   int64
}

func (rf *Raft) GetOffsets() []int64 {
	return rf.Offsets
}

// WriteEntryToFile 将条目写入指定的文件，并返回写入的起始偏移量。
func (rf *Raft) WriteEntryToFile(e []*Entry, filename string, startPos int64) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 打开文件，如果文件不存在则创建
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Errorf("打开存储Raft日志的磁盘文件失败：%v", err)
	}
	defer file.Close()

	// 包装文件对象以进行缓冲写入
	writer := bufio.NewWriter(file)

	// 获取当前写入位置，即为返回的偏移量
	var offset int64
	var offsets []int64
	if startPos == 0 {
		offset, err = file.Seek(0, os.SEEK_END)
		if err != nil {
			fmt.Errorf("定位存储Raft日志的磁盘文件失败：%v", err)
		}
	} else { // 同步日志时，需要已有的日志与leader的冲突，需要覆盖之前的错误的
		offset, err = file.Seek(startPos, os.SEEK_SET)
		if err != nil {
			fmt.Errorf("定位存储Raft日志的磁盘文件的起始位置失败：%v", err)
		}
	}

	for _, entry := range e {
		keySize := uint32(len(entry.Key))
		valueSize := uint32(len(entry.Value))
		data := make([]byte, 20+keySize+valueSize) // 48 bytes for 6 uint64 + key + value

		// 将数据编码到byte slice中
		binary.BigEndian.PutUint32(data[0:4], entry.Index)
		binary.BigEndian.PutUint32(data[4:8], entry.CurrentTerm)
		binary.BigEndian.PutUint32(data[8:12], entry.VotedFor)
		binary.BigEndian.PutUint32(data[12:16], keySize)
		binary.BigEndian.PutUint32(data[16:20], valueSize)
		copy(data[20:20+keySize], entry.Key)
		copy(data[20+keySize:], entry.Value)

		// 写入文件
		u, err := writer.Write(data)
		if err != nil || u < len(data) {
			fmt.Errorf("写入存储Raft日志的磁盘文件失败：%v", err)
		}

		// 刷新缓冲区以确保数据被写入文件
		err = writer.Flush()
		if err != nil {
			fmt.Errorf("刷新缓冲区失败：%v", err)
		}
		// _, err = file.Write(data)
		// if err != nil {
		// 	fmt.Println("写入存储Raft日志的磁盘文件有问题")
		// }
		// 添加偏移量到数组中
		offsets = append(offsets, offset)
		offset += int64(len(data))
	}
	// rf.mu.Lock()
	rf.Offsets = append(rf.Offsets, offsets...)
	// rf.mu.Unlock()
	// return offsets, nil
	// return nil
}

// func (rf *Raft) WriteEntryToFile(e []*Entry, filename string, startPos int64) (offsets []int64, err error) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// 打开文件，如果文件不存在则创建
// 	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
// 	if err != nil {
// 		return nil, fmt.Errorf("打开存储Raft日志的磁盘文件失败：%v", err)
// 	}
// 	defer file.Close()

// 	// 包装文件对象以进行缓冲写入
// 	writer := bufio.NewWriter(file)

// 	// 获取当前写入位置，即为返回的偏移量
// 	var offset int64
// 	if startPos == 0 {
// 		offset, err = file.Seek(0, os.SEEK_END)
// 		if err != nil {
// 			return nil, fmt.Errorf("定位存储Raft日志的磁盘文件失败：%v", err)
// 		}
// 	} else { // 同步日志时，需要已有的日志与leader的冲突，需要覆盖之前的错误的
// 		offset, err = file.Seek(startPos, os.SEEK_SET)
// 		if err != nil {
// 			return nil, fmt.Errorf("定位存储Raft日志的磁盘文件的起始位置失败：%v", err)
// 		}
// 	}

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

// 	// // 写入文件
// 	// _, err = file.Write(data)
// 	// if err != nil {
// 	// 	fmt.Println("写入存储Raft日志的磁盘文件有问题")
// 	// 	return 0, err
// 	// }
// 	// writer := bufio.NewWriter(file)
// 	// u,err := writer.Write(data)
// 	// if err != nil || u<len(data) {
// 	// 	fmt.Println("写入存储Raft日志的磁盘文件有问题")
// 	// 	return 0, err
// 	// }
// 	// writer.Flush()		// 刷新缓冲区数据到文件中

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

// 		// 写入文件
// 		_, err := writer.Write(data)
// 		if err != nil {
// 			return nil, fmt.Errorf("写入存储Raft日志的磁盘文件失败：%v", err)
// 		}

// 		// 刷新缓冲区以确保数据被写入文件
// 		err = writer.Flush()
// 		if err != nil {
// 			return nil, fmt.Errorf("刷新缓冲区失败：%v", err)
// 		}

// 		// 添加偏移量到数组中
// 		offsets = append(offsets, offset)
// 		offset += int64(len(data))
// 	}
// 	rf.Offsets = append(rf.Offsets, offsets...)
// 	return offsets, nil
// }

// ReadValueFromFile 从指定的偏移量读取value
func (rf *Raft) ReadValueFromFile(filename string, offset int64) (string, error) {
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// 移动到指定偏移量
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		fmt.Println("get时，seek文件的位置有问题")
		return "", err
	}

	// 读取数据到buffer中，首先是固定长度的20字节
	data := make([]byte, 20)
	if _, err := file.Read(data); err != nil {
		fmt.Println("get时，读取key和value的前20个固定字节时有问题")
		return "", err
	}

	// 解析固定长度的字段
	keySize := binary.BigEndian.Uint64(data[12:16])
	valueSize := binary.BigEndian.Uint64(data[16:20])

	// 读取Key和Value
	keyValueBuffer := make([]byte, keySize+valueSize)
	if _, err := file.Read(keyValueBuffer); err != nil {
		return "", err
	}

	// Value是紧跟在Key后面的部分
	value := string(keyValueBuffer[keySize:])

	return value, nil
}

// save Raft's persistent state to stable storage
// func (rf *Raft) raftStateForPersist(filePath string, currentTerm int, votedFor int, log []LogEntry) {
// 	state := RaftState{CurrentTerm: currentTerm, VotedFor: votedFor, Log: log}
// 	file, err := os.Create(filePath) // 如果文件已存在，则会截断该文件，原文件中的所有数据都会丢失，即不断更新持久化的数据
// 	if err != nil {
// 		util.EPrintf("Failed to create file: %v", err)
// 	}
// 	defer file.Close()

// 	encoder := gob.NewEncoder(file)
// 	if err := encoder.Encode(state); err != nil {
// 		util.EPrintf("Failed to encode data: %v", err)
// 	}
// }

// // restore previously persisted state.
// func (rf *Raft) ReadPersist(filePath string) *RaftState {
// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		util.EPrintf("Failed to open file: %v", err)
// 	}
// 	defer file.Close()
// 	// var err error
// 	// file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
// 	// if err != nil {
// 	// 	fmt.Println("打开RaftState文件有问题")
// 	// 	return nil
// 	// }

// 	var state RaftState
// 	decoder := gob.NewDecoder(file)
// 	if err := decoder.Decode(&state); err != nil {
// 		util.EPrintf("Failed to decode data: %v", err)
// 	}
// 	return &state
// }

func (rf *Raft) GetLeaderId() (leaderId int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return int32(rf.leaderId)
}

func (rf *Raft) RequestVote(ctx context.Context, args *raftrpc.RequestVoteRequest) (*raftrpc.RequestVoteResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply := &raftrpc.RequestVoteResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.VoteGranted = false

	// util.DPrintf("RaftNode[%d] Handle RequestVote, CandidatesId[%d] Term[%d] CurrentTerm[%d] LastLogIndex[%d] LastLogTerm[%d] votedFor[%d]",
	// 	rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	// defer func() {
	// 	util.DPrintf("RaftNode[%d] Return RequestVote, CandidatesId[%d] Term[%d] currentTerm[%d] VoteGranted[%v] votedFor[%d]", rf.me, args.CandidateId,
	// 		args.Term, rf.currentTerm, reply.VoteGranted, rf.votedFor)
	// }()

	// 任期不如我大，拒绝投票
	if args.Term < int32(rf.currentTerm) {
		return reply, nil
	}

	// 发现更大的任期，则转为该任期的follower
	if args.Term > int32(rf.currentTerm) {
		rf.currentTerm = int(args.Term)
		rf.role = ROLE_FOLLOWER
		rf.votedFor = -1                    // 有问题，如果两个leader同时选举，那会进行多次投票，因为都满足下方的投票条件---没有问题，如果第二个来请求投票，此时args.Term = rf.currentTerm。因为rf.currentTerm已经更新
		rf.leaderId = int(args.CandidateId) // 先假设这个即将成为leader
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
	// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
	return reply, nil
}

// 已兼容snapshot
func (rf *Raft) AppendEntriesInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (*raftrpc.AppendEntriesInRaftResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// util.DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
	// rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)
	reply := &raftrpc.AppendEntriesInRaftResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	var logEntrys []LogEntry
	json.Unmarshal(args.Entries, &logEntrys)
	// if len(logEntrys) != 0 { // 除去普通的心跳
	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
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
		// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
	}

	// 认识新的leader
	rf.leaderId = int(args.LeaderId)
	// 刷新活跃时间
	rf.lastActiveTime = time.Now()
	if len(logEntrys) == 0 {
		reply.Success = true                           // 成功心跳
		if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
			rf.commitIndex = int(args.LeaderCommit)
			if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
				rf.commitIndex = rf.lastIndex()
			}
		}
		return reply, nil
	}

	if args.PrevLogIndex > int32(rf.lastIndex()) { // prevLogIndex位置没有日志的情况
		reply.ConflictIndex = int32(rf.lastIndex() + 1)
		return reply, nil
	}
	// prevLogIndex位置有日志，那么判断term必须相同，否则false
	if args.PrevLogIndex != 0 && (rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term != int32(args.PrevLogTerm)) {
		reply.ConflictTerm = rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term
		for index := 1; index <= int(args.PrevLogIndex); index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
			if rf.log[rf.index2LogPos(index)].Term == int32(reply.ConflictTerm) {
				reply.ConflictIndex = int32(index)
				break
			}
		}
		return reply, nil
	}
	// fmt.Printf("此时同步的日志为%v\n",len(logEntrys))
	// 找到了第一个不同的index，开始同步日志
	// var tempLogs []*Entry // 自动会在写入磁盘文件后进行清零的操作
	for i, logEntry := range logEntrys {
		index := int(args.PrevLogIndex) + 1 + i
		logPos := rf.index2LogPos(index)
		entry := Entry{
			Index:       uint32(logEntry.Command.Index),
			CurrentTerm: uint32(logEntry.Command.Term),
			VotedFor:    uint32(rf.leaderId),
			Key:         logEntry.Command.Key,
			Value:       logEntry.Command.Value,
		}
		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
			rf.log = append(rf.log, logEntry)
			rf.batchLog= append(rf.batchLog, &entry) // 将要写入磁盘文件的结构体暂存，批量存储。

			if index == rf.lastIndex() { // 已经将日志补足后，开始批量写入
				// offsets1, err := rf.WriteEntryToFile(tempLogs, "./raft/RaftState.log", 0)
				// rf.mu.Unlock()
				rf.WriteEntryToFile(rf.batchLog, "./raft/RaftState.log", 0)
				rf.batchLog = rf.batchLog[:0]		// 清空暂存日志的数组
				// go func() {
				// 	err := rf.WriteEntryToFile(tempLogs, "./raft/RaftState.log", 0)
				// 	if err != nil {
				// 		fmt.Println("Error in WriteEntryToFile:", err)
				// 	}
				// }()
				// rf.mu.Lock()
				// rf.Offsets = append(rf.Offsets, offsets1...)
				// if err != nil {
				// 	fmt.Println("这里有问题嘛")
				// 	panic(err)
				// }
			}
			// util.DPrintf("追加RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] Index[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, index, rf.Offsets)
		} else { // 重叠部分
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
				rf.log = append(rf.log, logEntry) // 把新log加入进来

				offset := rf.Offsets[index]      // 截取后面错误的offset
				rf.Offsets = rf.Offsets[:logPos] // 删除当前错误的offset，以及后续的所有
				arrEntry := []*Entry{&entry}     // 这里由于发生的情况较少，所以每次只写入一个日志到磁盘文件
				// offsets2, err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
				// rf.mu.Unlock()
				rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
				// go func() {
				// 	err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
				// 	if err != nil {
				// 		fmt.Println("Error in WriteEntryToFile:", err)
				// 	}
				// }()
				// rf.mu.Lock()
				// rf.Offsets = append(rf.Offsets, offsets2[0])
				// if err != nil {
				// 	panic(err)
				// }
				// util.DPrintf("重叠RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.Offsets)
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
	}
	reply.Success = true
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 只有leader才能写入
	if rf.role != ROLE_LEADER {
		// fmt.Println("到这了嘛3")
		return -1, -1, false
	}
	logEntry := LogEntry{
		Command: command.(DetailCod),
		Term:    int32(rf.currentTerm),
	}
	// fmt.Println("到这了嘛4")
	rf.log = append(rf.log, logEntry)
	index = rf.lastIndex()
	term = rf.currentTerm
	entry = Entry{
		Index:       uint32(index),
		CurrentTerm: uint32(term),
		VotedFor:    uint32(rf.leaderId),
		Key:         command.(DetailCod).Key,
		Value:       command.(DetailCod).Value,
	}
	arrEntry = []*Entry{&entry}
	// rf.batchLog = append(rf.batchLog, &entry)
	// if err := enc.Encode(entry); err != nil {
	// 	util.EPrintf("Encode error in Start()：%v", err)
	// }
	// rf.batchLogSize += int64(buffer.Len())
	// // 如果总大小超过3MB，截取日志数组并退出循环
	// if rf.batchLogSize >= fileSizeLimit {
	// 	rf.WriteEntryToFile(rf.batchLog, "./raft/RaftState.log", 0)
		// go func() {
		// 	err := rf.WriteEntryToFile(rf.batchLog, "./raft/RaftState.log", 0)
		// 	if err != nil {
		// 		fmt.Println("Error in WriteEntryToFile:", err)
		// 	}
		// }()
	// 	buffer.Reset()
	// 	rf.batchLog = rf.batchLog[:0] // 清空缓存区和暂存的数组
	// }
	// rf.mu.Unlock()
	rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", 0)
	// // offsets, err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", 0)
	// if err != nil {
	// 	panic(err)
	// }
	// rf.Offsets = append(rf.Offsets, offsets...)
	// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)

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

		fmt.Println("跳出Raftserver的for循环，日志同步完成")
		break
	}
}

func (rf *Raft) sendRequestVote(address string, args *raftrpc.RequestVoteRequest) (bool, *raftrpc.RequestVoteResponse) {
	// time.Sleep(time.Millisecond * time.Duration(rf.delay+rand.Intn(25)))
	// util.DPrintf("Start sendRequestVote")
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

func (rf *Raft) AppendMonitor() {
	timeout := 3 * time.Second
	for {
		time.Sleep(2 * time.Second)
		if (time.Since(rf.LastAppendTime) > timeout) && rf.GetLeaderId() != int32(rf.me) {
			fmt.Println("3秒没有收到来自leader的同步或者心跳信息！")
			continue
		}
		fmt.Printf("当前的log大小%v\n", rf.lastIndex())
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
				rf.mu.Lock()
				// defer func() {
				// 	util.DPrintf("RaftNode[%d] RequestVote ends, finishCount[%d] voteCount[%d] Role[%s] maxTerm[%d] currentTerm[%d]", rf.me, finishCount, voteCount,
				// 		rf.role, maxTerm, rf.currentTerm)
				// }()
				// 如果角色改变了，则忽略本轮投票结果；当多个server同时开始选举，有一个leader已经选出后，则本server的选举结果可直接不用管。
				if rf.role != ROLE_CANDIDATES {
					return
				}
				// 发现了更高的任期，切回follower；这个是不是可以在接受投票时就判断，如果有任期比自己大的，就直接转换为follower，也不看投票结果了
				if maxTerm > rf.currentTerm {
					rf.role = ROLE_FOLLOWER
					rf.leaderId = 0
					rf.currentTerm = maxTerm // 更新自己的Term和voteFor
					rf.votedFor = -1
					// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
					return
				}
				// 赢得大多数选票，则成为leader
				if voteCount > len(rf.peers)/2 {
					rf.role = ROLE_LEADER
					util.DPrintf("RaftNode[%d] Candidate -> Leader", rf.me)

					rf.leaderId = rf.me
					rf.nextIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.lastIndex() + 1
					}
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.matchIndex[i] = 0
					}

					op := DetailCod{
						OpType: "TermLog",
					}
					rf.mu.Unlock()
					op.Index, op.Term, _ = rf.Start(op) // 需要提交一个空的指令，需要在初始化nextindex之后，提交空指令
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
	// if语句的第一个条件则是排除掉还没有复制到大多数server的情况
	// fmt.Printf("此时log的长度：%v以及newcommitindex的值：%v\n",len(rf.log),newCommitIndex)
	if newCommitIndex > rf.commitIndex && rf.log[rf.index2LogPos(newCommitIndex)].Term == int32(rf.currentTerm) {
		rf.commitIndex = newCommitIndex // 保证是当前的Term才能根据同步到server的副本数量判断是否可以提交
	}
	// util.DPrintf("RaftNode[%d] updateCommitIndex, newCommitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

// 已兼容snapshot
func (rf *Raft) doAppendEntries(peerId int) {
	enc := gob.NewEncoder(&buffer)

	args := raftrpc.AppendEntriesInRaftRequest{}
	args.Term = int32(rf.currentTerm)
	args.LeaderId = int32(rf.me)
	args.LeaderCommit = int32(rf.commitIndex)
	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)
	if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
	}
	// start := rf.index2LogPos(int(args.PrevLogIndex)+1)
	// if (start + 0)<len(rf.log)  {
	// 	appendLog = rf.log[start:start + 100]
	// }else{
	// 	appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):] //这里如果下标大于或等于log数组的长度，只是会返回一个空切片，所以正好当作心跳使用
	// }

	// 设置日志同步的阈值
	for i := rf.index2LogPos(int(args.PrevLogIndex) + 1); i < len(rf.log); i++ {
		if err := enc.Encode(rf.log[i]); err != nil { // 将 rf.log[i] 日志项编码后的字节序列写入到 buffer 缓冲区中
			util.EPrintf("Encode error：%v", err)
		}
		rf.appendLogSize += int64(buffer.Len())
		// 如果总大小超过3MB，截取日志数组并退出循环
		if rf.appendLogSize >= threshold {
			rf.appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):i]
			break
		}
	}
	if rf.appendLogSize < threshold {
		rf.appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]
	}
	buffer.Reset()

	// fmt.Printf("此时下标会不会有问题，log长度：%v，下标：%v", len(rf.log), args.PrevLogIndex+1)
	data, _ := json.Marshal(rf.appendLog) // 后续计算日志的长度的时候可千万别用这个转换后的直接数组
	args.Entries = data
	// args.Entries = append(args.Entries, rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]...)
	// util.DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
	// 	rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

	// if len(appendLog) != 0 { // 除去普通的心跳
	// rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
	// 	// fmt.Println("重置lastAppendTime")
	// }

	go func(peerId int) {
		// util.DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args.Term, args.LeaderId)
		if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, rf.pools[peerId]); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			// defer func() {
			// 	util.DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
			// 		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
			// }()

			// 如果不是rpc前的leader状态了，那么啥也别做了，可能遇到了term更大的server，因为rpc的时候是没有加锁的
			if rf.currentTerm != int(args.Term) {
				rf.SyncChans[peerId] <- "NotLeader"
				return
			}
			if reply.Term > int32(rf.currentTerm) { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = 0
				rf.currentTerm = int(reply.Term)
				rf.votedFor = -1
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
				rf.SyncChans[peerId] <- "NotLeader"
				return
			}
			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
			if reply.Success { // 同步日志成功
				rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(rf.appendLog) + 1
				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 // 记录已经复制到其他server的日志的最后index的情况
				rf.updateCommitIndex()                           // 更新commitIndex
			} else {
				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
				// nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
					conflictTermIndex := -1
					for index := args.PrevLogIndex; index > 0; index-- {
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
				// util.DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
			}
			// rf.SyncChans[peerId] <- rf.peers[peerId]
			rf.SyncChans[peerId] <- strconv.Itoa(peerId)
		} else {
			// rf.SyncChans[peerId] <- rf.peers[peerId]	// 同步日志失败也要重新发起日志同步
			rf.SyncChans[peerId] <- strconv.Itoa(peerId)
		}
	}(peerId)
}

func (rf *Raft) doHeartBeat(peerId int) {
	args := raftrpc.AppendEntriesInRaftRequest{}
	args.Term = int32(rf.currentTerm)
	args.LeaderId = int32(rf.me)
	args.LeaderCommit = int32(rf.commitIndex)
	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)
	if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
		args.PrevLogTerm = 0
	} else {
		args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
	}
	args.Entries = nil
	go func(peerId int) {
		if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, rf.pools[peerId]); ok {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != int(args.Term) {
				return
			}
			if reply.Term > int32(rf.currentTerm) { // 变成follower
				rf.role = ROLE_FOLLOWER
				rf.leaderId = 0
				rf.currentTerm = int(reply.Term)
				rf.votedFor = -1
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
				return
			}
		}
	}(peerId)
}

func (rf *Raft) appendEntriesLoop() {
	First := true
	for !rf.killed() {
		// time.Sleep(time.Duration(rf.SyncTime) * time.Millisecond) // 间隔10ms

		func() {
			rf.mu.Lock() // 这里可以用读锁
			defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				return
			}
			// 100ms广播1次
			// now := time.Now()
			// if now.Sub(rf.lastBroadcastTime) < 11*time.Millisecond {
			// 	return
			// }
			if rf.lastIndex() == 0 {
				return
			}
			// rf.lastBroadcastTime = time.Now() // 确定过了广播的时间间隔，才开始进行广播，并且设置新的广播时间
			// 向所有follower发送心跳
			// for peerId := 0; peerId < len(rf.peers); peerId++ {
			// for peerId := 0; peerId < 3; peerId++ { // 先固定，避免访问rf的属性，涉及到死锁问题
			// 	if peerId == rf.me {
			// 		continue
			// 	}
			// now := time.Now() // 心跳
			// if (now.Sub(rf.LastAppendTime) > 300*time.Millisecond) && Heartbeat == 1 {
			if First {
				for peerId := 0; peerId < len(rf.peers); peerId++ { // 先固定，避免访问rf的属性，涉及到死锁问题
					if peerId == rf.me {
						continue
					}
					// rf.doHeartBeat(peerId)
					rf.doAppendEntries(peerId)
				}
				First = false
			}

			select {
			case value1 := <-rf.SyncChans[0]:
				if value1 == "NotLeader" {
					fmt.Println("被告知不是NotLeader，退出")
					return
				}
				rf.doAppendEntries(0)
			default:
			}

			select {
			case value2 := <-rf.SyncChans[1]:
				if value2 == "NotLeader" {
					fmt.Println("被告知不是NotLeader，退出")
					return
				}
				rf.doAppendEntries(1)
			default:
			}

			select {
			case value3 := <-rf.SyncChans[2]:
				if value3 == "NotLeader" {
					fmt.Println("被告知不是NotLeader，退出")
					return
				}
				rf.doAppendEntries(2)
			default:
			}

			// select { //   日志同步由对方服务器发来的反馈触发，避免过于重复的日志同步
			// // case value := <-rf.SyncChan:
			// // fmt.Println("value",value)
			// // switch value {
			// case value1 := <-rf.SyncChans[0]:
			// 	if value1 == "NotLeader" {
			// 		fmt.Println("被告知不是NotLeader，退出")
			// 		return
			// 	}
			// 	rf.doAppendEntries(0)
			// case value2 := <-rf.SyncChans[1]:
			// 	if value2 == "NotLeader" {
			// 		fmt.Println("被告知不是NotLeader，退出")
			// 		return
			// 	}
			// 	rf.doAppendEntries(1)
			// case value3 := <-rf.SyncChans[2]:
			// 	if value3 == "NotLeader" {
			// 		fmt.Println("被告知不是NotLeader，退出")
			// 		return
			// 	}
			// 	rf.doAppendEntries(2)
			// default: // 如果不是leader了就退出，后续设置一下
			// 	fmt.Println("被告知不是NotLeader，退出")
			// 	return
			// }
			// }
		}()
	}
}

// func (rf *Raft) appendEntriesLoop() {
// 	for !rf.killed() {
// 		time.Sleep(10 * time.Millisecond)       // 间隔10ms
// 		for peerId := 0; peerId < 3; peerId++ { // 先固定，避免访问rf的属性，涉及到死锁问题
// 			if peerId == rf.me {
// 				continue
// 			}
// 			go func(peerId int) {
// 				rf.mu.Lock()
// 				// 只有leader才向外广播心跳
// 				if rf.role != ROLE_LEADER {
// 					rf.mu.Unlock()
// 					return
// 				}
// 				if rf.lastIndex() == 0 {
// 					rf.mu.Unlock()
// 					return
// 				}
// 				args := raftrpc.AppendEntriesInRaftRequest{}
// 				args.Term = int32(rf.currentTerm)
// 				args.LeaderId = int32(rf.me)
// 				args.LeaderCommit = int32(rf.commitIndex)
// 				args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)
// 				if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
// 					args.PrevLogTerm = 0
// 				} else {
// 					args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
// 				}
// 				appendLog := rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):] //这里如果下标大于或等于log数组的长度，只是会返回一个空切片，所以正好当作心跳使用
// 				// fmt.Printf("此时下标会不会有问题，log长度：%v，下标：%v", len(rf.log), args.PrevLogIndex+1)
// 				data, _ := json.Marshal(appendLog) // 后续计算日志的长度的时候可千万别用这个转换后的直接数组
// 				args.Entries = data
// 				rf.mu.Unlock()
// 				if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, rf.pools[peerId]); ok {
// 					rf.mu.Lock()
// 					// 如果不是rpc前的leader状态了，那么啥也别做了，可能遇到了term更大的server，因为rpc的时候是没有加锁的
// 					if rf.currentTerm != int(args.Term) {
// 						rf.mu.Unlock()
// 						return
// 					}
// 					if reply.Term > int32(rf.currentTerm) { // 变成follower
// 						rf.role = ROLE_FOLLOWER
// 						rf.leaderId = 0
// 						rf.currentTerm = int(reply.Term)
// 						rf.votedFor = -1
// 						// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
// 						rf.mu.Unlock()
// 						return
// 					}
// 					// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
// 					// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
// 					if reply.Success { // 同步日志成功
// 						rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(appendLog) + 1
// 						rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 // 记录已经复制到其他server的日志的最后index的情况
// 						rf.updateCommitIndex()  			// 更新commitIndex
// 					} else {
// 						// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
// 						// nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

// 						if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
// 							// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
// 							conflictTermIndex := -1
// 							for index := args.PrevLogIndex; index > 0; index-- {
// 								// if rf.log[rf.index2LogPos(int(index))].Term == reply.ConflictTerm {
// 								// 	conflictTermIndex = int(index)
// 								// 	break
// 								// }
// 								// 我认为下方这个效果更好，这样PrevLogIndex的值就为 index
// 								if rf.log[rf.index2LogPos(int(index))].Term != reply.ConflictTerm {
// 									conflictTermIndex = int(index + 1)
// 									break
// 								}
// 							}
// 							if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
// 								rf.nextIndex[peerId] = conflictTermIndex
// 							} else {
// 								rf.nextIndex[peerId] = int(reply.ConflictIndex) // 用follower首次出现term的index作为同步开始
// 							}
// 						} else {
// 							// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
// 							// 这时候我们将返回的conflictIndex设置为nextIndex即可
// 							rf.nextIndex[peerId] = int(reply.ConflictIndex)
// 						}
// 						// util.DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
// 					}
// 					rf.mu.Unlock()
// 				}
// 			}(peerId)
// 		}
// 	}
// }

func (rf *Raft) applyLogLoop() {
	noMore := false
	for !rf.killed() {
		if noMore {
			time.Sleep(10 * time.Millisecond)
			// fmt.Println("commitindex不够")
		}
		func() {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			noMore = true
			// fmt.Printf("此时的commitIndex是多少：%v",rf.commitIndex)
			if (rf.commitIndex > rf.lastApplied) && ((rf.lastApplied - rf.shotOffset) <
				len(rf.Offsets)) {
			// if rf.commitIndex > rf.lastApplied {
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
				rf.lastApplied += 1
				// util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.Offsets)
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				realIndex := rf.lastApplied - rf.shotOffset // 截断前1个数据,后续可以优化，考虑批量删除
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  int(rf.log[appliedIndex].Term),
					Offset:       rf.Offsets[realIndex-1], // 将偏移量传进通道
				}
				rf.applyCh <- appliedMsg // 引入snapshot后，这里必须在锁内投递了，否则会和snapshot的交错产生bug
				rf.Offsets = rf.Offsets[1:]
				rf.shotOffset++
				if rf.lastApplied%rf.Gap == 0 {
					// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
					util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, len(rf.Offsets))
				}
				noMore = false
			}
		}()
		//		设置一个定时器，每十秒检查一次条件
		// ticker := time.NewTicker(3 * time.Second)
		// // defer ticker.Stop()
		// go func() {
		// 	for range ticker.C {
		// 		if !noMore{
		// 			fmt.Println("Raft层还在传输数据给上层server")
		// 		}
		// 	}
		// }()
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
	} else {
		lastLogTerm = 0
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
	for i := 0; i < 3; i++ {
		rf.SyncChans = append(rf.SyncChans, make(chan string, 10))
	}

	rf.role = ROLE_FOLLOWER
	rf.leaderId = 0
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh
	rf.Offsets = append(rf.Offsets, 0) // 初始化时添加一个0，使得后续对index的访问和raft的对其，从1开始

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
	// go rf.ReadPersist("./raft/RaftState.log") // 如果文件已存在，则截断文件，后续如果有要求恢复raft状态的功能，可以修改打开文件的方式。

	go rf.RegisterRaftServer(ctx, peers[me])
	// election
	go rf.electionLoop()
	// sync
	go rf.appendEntriesLoop()
	// apply
	go rf.applyLogLoop()
	// 检查有没有收到日志同步的消息，若没有则连接有问题
	go rf.AppendMonitor()

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

	return rf
}
