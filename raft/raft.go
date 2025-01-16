package raft

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"strconv"
	"strings"

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

	"gitee.com/dong-shuishui/FlexSync/pool"
	// "gitee.com/dong-shuishui/FlexSync/raft"
	// "gitee.com/dong-shuishui/FlexSync/raft"
	"gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"

	// "gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
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

var threshold int64 = 30 * 1024 * 1024
var entry_global Entry

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
	// pools   []pool.Pool   // 用于日志同步的连接池
	pools     [][]pool.Pool   // 修改为二维数组,每个peer有多个连接池
    portCount int            // 每个peer的端口数量
    syncIndex []int32        // 记录每个peer当前使用的端口索引
	basePorts []int // 每个节点监听的基础端口
	// kvrpc.UnimplementedKVServer
	raftrpc.UnimplementedRaftServer
	LastAppendTime time.Time
	Gap            int
	Offsets        []int64
	shotOffset     int
	SyncTime       int
	SyncChans      []chan string
	batchLog       []*Entry
	batchLogSize   int64
	currentLog     string // 存储value的磁盘文件的描述符
}

func (rf *Raft) GetOffsets() []int64 {
	return rf.Offsets
}

func (rf *Raft) SetCurrentLog(currentLog string) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	rf.currentLog = currentLog
}

// func (rf *Raft) WriteEntryToFile(e []*Entry, filename string, startPos int64) {
// 	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
// 	if err != nil {
// 		log.Fatalf("打开存储Raft日志的磁盘文件失败：%v", err)
// 	}
// 	defer file.Close()
// 	// 获取当前写入位置，即为返回的偏移量
// 	var offset int64
// 	var offsets []int64
// 	// 预分配足够大的偏移量切片，避免了在循环中动态扩容偏移量切片的操作
// 	offsets = make([]int64, len(e))
// 	if startPos == 0 {
// 		offset, err = file.Seek(0, os.SEEK_END)
// 		if err != nil {
// 			log.Fatalf("定位存储Raft日志的磁盘文件失败：%v", err)
// 		}
// 	} else { // 同步日志时，需要已有的日志与leader的冲突，需要覆盖之前的错误的
// 		offset, err = file.Seek(startPos, os.SEEK_SET)
// 		if err != nil {
// 			log.Fatalf("定位存储Raft日志的磁盘文件的起始位置失败：%v", err)
// 		}
// 	}
// 	for i, entry := range e {
// 		// 将数据编码并直接写入文件
// 		err := binary.Write(file, binary.BigEndian, *entry)
// 		if err != nil {
// 			log.Fatalf("写入存储Raft日志的磁盘文件失败：%v", err)
// 		}
// 		offsets[i] = offset
// 		offset += int64(binary.Size(entry))
// 	}
// 	rf.Offsets = append(rf.Offsets, offsets...)
// }

// WriteEntryToFile 将条目写入指定的文件，并返回写入的起始偏移量。
func (rf *Raft) WriteEntryToFile(e []*Entry, filename string, startPos int64) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 打开文件，如果文件不存在则创建
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("打开存储Raft日志的磁盘文件失败：%v", err)
	}
	defer file.Close()

	// 包装文件对象以进行缓冲写入
	writer := bufio.NewWriter(file)

	// 获取当前写入位置，即为返回的偏移量
	var offset int64
	var offsets []int64
	// var err error

	// 预分配足够大的偏移量切片，避免了在循环中动态扩容偏移量切片的操作
	offsets = make([]int64, len(e))

	if startPos == 0 {	// 0是直接追加
		offset, err = file.Seek(0, os.SEEK_END)
		if err != nil {
			log.Fatalf("定位存储Raft日志的磁盘文件失败：%v", err)
		}
	} else { // 同步日志时，需要已有的日志与leader的冲突，需要覆盖之前的错误的
		offset, err = file.Seek(startPos, os.SEEK_SET)
		if err != nil {
			log.Fatalf("定位存储Raft日志的磁盘文件的起始位置失败：%v", err)
		}
	}

	for i, entry := range e {

		valueSize := uint32(len(entry.Value))

		paddedKey := rf.persister.PadKey(entry.Key) // 存入valuelog里面也用
		keySize := uint32(len(paddedKey))
		data := make([]byte, 20+keySize+valueSize) // 48 bytes for 6 uint64 + key + value

		// 将数据编码到byte slice中
		binary.LittleEndian.PutUint32(data[0:4], entry.Index)
		binary.LittleEndian.PutUint32(data[4:8], entry.CurrentTerm)
		binary.LittleEndian.PutUint32(data[8:12], entry.VotedFor)
		binary.LittleEndian.PutUint32(data[12:16], keySize)
		binary.LittleEndian.PutUint32(data[16:20], valueSize)

		copy(data[20:20+keySize], paddedKey)
		copy(data[20+keySize:], entry.Value)

		// 写入文件
		u, err := writer.Write(data)
		if err != nil || u < len(data) {
			log.Fatalf("写入存储Raft日志的磁盘文件失败：%v", err)
		}

		// _, err = file.Write(data)
		// if err != nil {
		// 	fmt.Println("写入存储Raft日志的磁盘文件有问题")
		// }
		// 添加偏移量到数组中
		// offsets = append(offsets, offset)
		offsets[i] = offset
		offset += int64(len(data))
	}
	// 刷新缓冲区以确保数据被写入文件
	err = writer.Flush()
	if err != nil {
		log.Fatalf("刷新缓冲区失败：%v", err)
	}

	rf.Offsets = append(rf.Offsets, offsets...)
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

// func (rf *Raft) ReadValueFromFile(filename string, offset int64) (string, error) {
// 	// 打开文件
// 	file, err := os.Open(filename)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()
// 	// 移动到指定偏移量
// 	_, err = file.Seek(offset, os.SEEK_SET)
// 	if err != nil {
// 		fmt.Println("get时，seek文件的位置有问题")
// 		return "", err
// 	}
// 	 // 从文件中读取数据并解码到 entry 结构体中
// 	 var entry Entry
// 	 err = binary.Read(file, binary.BigEndian, &entry)
// 	 if err != nil {
// 		 return "", err
// 	 }
// 	return entry.Value, nil
// }

// ReadValueFromFile 从指定的偏移量读取value
func (rf *Raft) ReadValueFromFile(filename string, offset int64) (string, string, error) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// 打开文件
	file, err := os.Open(filename)
	if err != nil {
		return "","", err
	}
	defer file.Close()

	if offset == -1 {
		return "NOKEY","", nil
	}

	// 移动到指定偏移量
	_, err = file.Seek(offset, os.SEEK_SET)
	if err != nil {
		fmt.Println("get时，seek文件的位置有问题")
		return "","", err
	}

	// 获取文件信息
	// fileInfo, err := file.Stat()
	// if err != nil {
	// 	panic(err)
	// }
	// fileSize := fileInfo.Size()
	// fmt.Printf("当前的offset: %v===filesize: %v\n", offset, fileSize)

	// 读取数据到buffer中，首先是固定长度的20字节
	header := make([]byte, 20)

	n, err := file.Read(header)
	// fmt.Printf("读取了几个字节的数据%v\n",n)
	if err != nil {
		fmt.Println("get时，读取key和value的前20个固定字节时有问题")
		return "","", err
	}
	// 确保读取的字节数足够
	if n < 20 {
		fmt.Printf("not enough data: expected 20 bytes, got %d\n", n)
		return "","", err
	}

	// 解析固定长度的字段
	keySize := binary.LittleEndian.Uint32(header[12:16])
	valueSize := binary.LittleEndian.Uint32(header[16:20])

	// 读取Key和Value
	keyValueBuffer := make([]byte, keySize+valueSize)
	if _, err := file.Read(keyValueBuffer); err != nil {
		return "","", err
	}

	// Key是从buffer的开始部分
	key := string(keyValueBuffer[:keySize])
	// Value是紧跟在Key后面的部分
	value := string(keyValueBuffer[keySize:])

	return key, value, nil
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
		rf.votedFor = -1 // 有问题，如果两个leader同时选举，那会进行多次投票，因为都满足下方的投票条件---没有问题，如果第二个来请求投票，此时args.Term = rf.currentTerm。因为rf.currentTerm已经更新
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
			reply.VoteGranted = true
			rf.lastActiveTime = time.Now() // 为其他人投票，重置选举超时的时间
		}
	}
	// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
	return reply, nil
}

// AppendEntriesInRaft 处理流式日志复制请求
func (rf *Raft) AppendEntriesInRaft(stream raftrpc.Raft_AppendEntriesInRaftServer) error {
    for {
        // 接收流式请求
        req, err := stream.Recv()
        if err == io.EOF {
            return nil
        }
        if err != nil {
            return err
        }

        rf.mu.Lock()
        reply := &raftrpc.AppendEntriesInRaftResponse{
            Term:          int32(rf.currentTerm),
            Success:       false,
            ConflictIndex: -1,
            ConflictTerm:  -1,
        }

        if req.Term < int32(rf.currentTerm) {
            rf.mu.Unlock()
            if err := stream.Send(reply); err != nil {
                return err
            }
            continue
        }

        // 更新任期
        if req.Term > int32(rf.currentTerm) {
            rf.currentTerm = int(req.Term)
            rf.role = ROLE_FOLLOWER
            rf.votedFor = -1
        }

        // 认识新的leader并刷新活跃时间
        rf.leaderId = int(req.LeaderId)
        rf.lastActiveTime = time.Now()
        rf.LastAppendTime = time.Now()

        // 处理日志条目
        if len(req.Entries) > 0 {
            // 验证PrevLogIndex的日志
            if req.PrevLogIndex > int32(rf.lastIndex()) {
                reply.ConflictIndex = int32(rf.lastIndex() + 1)
                rf.mu.Unlock()
                if err := stream.Send(reply); err != nil {
                    return err
                }
                continue
            }

            if req.PrevLogIndex != 0 && rf.log[rf.index2LogPos(int(req.PrevLogIndex))].Term != req.PrevLogTerm {
                reply.ConflictTerm = rf.log[rf.index2LogPos(int(req.PrevLogIndex))].Term
                for index := 1; index <= int(req.PrevLogIndex); index++ {
                    if rf.log[rf.index2LogPos(index)].Term == int32(reply.ConflictTerm) {
                        reply.ConflictIndex = int32(index)
                        break
                    }
                }
                rf.mu.Unlock()
                if err := stream.Send(reply); err != nil {
                    return err
                }
                continue
            }

            // 处理日志条目
            for i, logEntry := range req.Entries {
                index := int(req.PrevLogIndex) + 1 + i
                logPos := rf.index2LogPos(index)
                entry := Entry{
                    Index:       uint32(logEntry.GetCommand().Index),
                    CurrentTerm: uint32(logEntry.GetCommand().Term),
                    VotedFor:    uint32(rf.leaderId),
                    Key:         logEntry.GetCommand().Key,
                    Value:       logEntry.GetCommand().Value,
                }

                if index > rf.lastIndex() {
                    rf.log = append(rf.log, logEntry)
                    if logEntry.Command.OpType != "TermLog" {
                        rf.batchLog = append(rf.batchLog, &entry)
                    }

                    if index == rf.lastIndex() && logEntry.Command.OpType != "TermLog" {
                        rf.WriteEntryToFile(rf.batchLog, rf.currentLog, 0)
                        rf.batchLog = rf.batchLog[:0]
                    }
                } else if rf.log[logPos].Term != logEntry.Term {
                    rf.log = rf.log[:logPos]
                    rf.log = append(rf.log, logEntry)
                    offset := rf.Offsets[index-rf.shotOffset-1]
                    rf.Offsets = rf.Offsets[:logPos-rf.shotOffset]
                    arrEntry := []*Entry{&entry}
                    rf.WriteEntryToFile(arrEntry, rf.currentLog, offset)
                }
            }
        }

        // 更新commitIndex
        if req.LeaderCommit > int32(rf.commitIndex) {
            rf.commitIndex = int(req.LeaderCommit)
            if rf.lastIndex() < rf.commitIndex {
                rf.commitIndex = rf.lastIndex()
            }
        }

        reply.Success = true
        rf.mu.Unlock()
        
        if err := stream.Send(reply); err != nil {
            return err
        }
    }
}

// 已兼容snapshot
// func (rf *Raft) AppendEntriesInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (*raftrpc.AppendEntriesInRaftResponse, error) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	// util.DPrintf("RaftNode[%d] Handle AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] commitIndex[%d] Entries[%v]",
// 	// rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, args.Entries)
// 	reply := &raftrpc.AppendEntriesInRaftResponse{}
// 	reply.Term = int32(rf.currentTerm)
// 	reply.Success = false
// 	reply.ConflictIndex = -1
// 	reply.ConflictTerm = -1
// 	// var logEntrys []*raftrpc.LogEntry
// 	// json.Unmarshal(args.Entries, &logEntrys)
// 	logEntrys := args.Entries
// 	// if len(logEntrys) != 0 { // 除去普通的心跳
// 	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
// 	// fmt.Println("重置lastAppendTime")
// 	// }

// 	// defer func() {
// 	// 	util.DPrintf("RaftNode[%d] Return AppendEntries, LeaderId[%d] Term[%d] CurrentTerm[%d] role=[%s] logIndex[%d] prevLogIndex[%d] prevLogTerm[%d] Success[%v] commitIndex[%d] log[%v] ConflictIndex[%d]",
// 	// 		rf.me, rf.leaderId, args.Term, rf.currentTerm, rf.role, rf.lastIndex(), args.PrevLogIndex, args.PrevLogTerm, reply.Success, rf.commitIndex, len(rf.log), reply.ConflictIndex)
// 	// }()

// 	if args.Term < int32(rf.currentTerm) {
// 		return reply, nil
// 	}

// 	// 发现更大的任期，则转为该任期的follower
// 	if args.Term > int32(rf.currentTerm) {
// 		rf.currentTerm = int(args.Term)
// 		rf.role = ROLE_FOLLOWER
// 		rf.votedFor = -1
// 		// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
// 	}

// 	// 认识新的leader
// 	rf.leaderId = int(args.LeaderId)
// 	// 刷新活跃时间
// 	rf.lastActiveTime = time.Now()
// 	if len(logEntrys) == 0 {
// 		reply.Success = true                           // 成功心跳
// 		if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
// 			rf.commitIndex = int(args.LeaderCommit)
// 			if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
// 				rf.commitIndex = rf.lastIndex()
// 			}
// 		}
// 		return reply, nil
// 	}

// 	if args.PrevLogIndex > int32(rf.lastIndex()) { // prevLogIndex位置没有日志的情况
// 		reply.ConflictIndex = int32(rf.lastIndex() + 1)
// 		return reply, nil
// 	}
// 	// prevLogIndex位置有日志，那么判断term必须相同，否则false
// 	if args.PrevLogIndex != 0 && (rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term != int32(args.PrevLogTerm)) {
// 		reply.ConflictTerm = rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term
// 		for index := 1; index <= int(args.PrevLogIndex); index++ { // 找到冲突term的首次出现位置，最差就是PrevLogIndex
// 			if rf.log[rf.index2LogPos(index)].Term == int32(reply.ConflictTerm) {
// 				reply.ConflictIndex = int32(index)
// 				break
// 			}
// 		}
// 		return reply, nil
// 	}
// 	// fmt.Printf("此时同步的日志为%v\n",len(logEntrys))
// 	// 找到了第一个不同的index，开始同步日志
// 	// var tempLogs []*Entry // 自动会在写入磁盘文件后进行清零的操作
// 	var entry Entry
// 	var index int
// 	var logPos int
// 	for i, logEntry := range logEntrys {
// 		if logEntry == nil || logEntry.GetCommand() == nil {
// 			fmt.Println("此时logEntry为nil，或者logEntry中的Command为nil。太抽象了")
// 			continue
// 		}
// 		index = int(args.PrevLogIndex) + 1 + i
// 		logPos = rf.index2LogPos(index)
// 		entry = Entry{
// 			Index:       uint32(logEntry.GetCommand().Index),
// 			CurrentTerm: uint32(logEntry.GetCommand().Term),
// 			VotedFor:    uint32(rf.leaderId),
// 			Key:         logEntry.GetCommand().Key,
// 			Value:       logEntry.GetCommand().Value,
// 		}
// 		if index > rf.lastIndex() { // 超出现有日志长度，继续追加
// 			rf.log = append(rf.log, logEntry)
// 			if logEntry.Command.OpType!="TermLog" {
// 				rf.batchLog = append(rf.batchLog, &entry) // 将要写入磁盘文件的结构体暂存，批量存储。
// 			}

// 			if index == rf.lastIndex()&&logEntry.Command.OpType!="TermLog" { // 已经将日志补足后，开始批量写入，同时为了与leader在偏移量上的统一，对于空指令，也不写入
// 				// offsets1, err := rf.WriteEntryToFile(tempLogs, "./raft/RaftState.log", 0)
// 				// rf.mu.Unlock()
// 				rf.WriteEntryToFile(rf.batchLog, rf.currentLog, 0)
// 				rf.batchLog = rf.batchLog[:0] // 清空暂存日志的数组
// 				// go func() {
// 				// 	err := rf.WriteEntryToFile(tempLogs, "./raft/RaftState.log", 0)
// 				// 	if err != nil {
// 				// 		fmt.Println("Error in WriteEntryToFile:", err)
// 				// 	}
// 				// }()
// 				// rf.mu.Lock()
// 				// rf.Offsets = append(rf.Offsets, offsets1...)
// 				// if err != nil {
// 				// 	fmt.Println("这里有问题嘛")
// 				// 	panic(err)
// 				// }
// 			}
// 			// util.DPrintf("追加RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] Index[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, index, rf.Offsets)
// 		} else { // 重叠部分
// 			if rf.log[logPos].Term != logEntry.Term {
// 				fmt.Println("还有重叠的情况嘛？？？")
// 				rf.log = rf.log[:logPos]          // 删除当前以及后续所有log
// 				rf.log = append(rf.log, logEntry) // 把新log加入进来

// 				// offset := rf.Offsets[index]      // 截取后面错误的offset
// 				offset := rf.Offsets[index-rf.shotOffset-1] // 这个要减一
// 				// offset := rf.Offsets[index-rf.shotOffset] // 将上面的改为加一了
// 				// rf.Offsets = rf.Offsets[:logPos] // 删除当前错误的offset，以及后续的所有
// 				rf.Offsets = rf.Offsets[:logPos-rf.shotOffset] // 不用减一，因为logPos已经是减一了的
// 				arrEntry := []*Entry{&entry}                   // 这里由于发生的情况较少，所以每次只写入一个日志到磁盘文件
// 				// offsets2, err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
// 				// rf.mu.Unlock()
// 				rf.WriteEntryToFile(arrEntry, rf.currentLog, offset)
// 				// go func() {
// 				// 	err := rf.WriteEntryToFile(arrEntry, "./raft/RaftState.log", offset)
// 				// 	if err != nil {
// 				// 		fmt.Println("Error in WriteEntryToFile:", err)
// 				// 	}
// 				// }()
// 				// rf.mu.Lock()
// 				// rf.Offsets = append(rf.Offsets, offsets2[0])
// 				// if err != nil {
// 				// 	panic(err)
// 				// }
// 				// util.DPrintf("重叠RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets[%d]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.Offsets)
// 			} // term一样啥也不用做，继续向后比对Log
// 		} // 每追加一个日志就持久化，并将offset和index绑定，存储到内存中。后续可以考虑这里实现批量持久化
// 	}
// 	// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)

// 	// 更新提交下标
// 	if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
// 		rf.commitIndex = int(args.LeaderCommit)
// 		if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
// 			rf.commitIndex = rf.lastIndex()
// 		}
// 	}
// 	reply.Success = true
// 	return reply, nil
// }

func (rf *Raft) HeartbeatInRaft(ctx context.Context, args *raftrpc.AppendEntriesInRaftRequest) (*raftrpc.AppendEntriesInRaftResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply := &raftrpc.AppendEntriesInRaftResponse{}
	reply.Term = int32(rf.currentTerm)
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1
	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
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
	reply.Success = true // 成功心跳
	// if args.LeaderCommit > int32(rf.commitIndex) { // 取leaderCommit和本server中lastIndex的最小值。
	// 	rf.commitIndex = int(args.LeaderCommit)
	// 	if rf.lastIndex() < rf.commitIndex { // 感觉，不存在这种情况，走到这里基本都是日志与leader一样了，怎么还会索引比commitindex小
	// 		rf.commitIndex = rf.lastIndex()
	// 	}
	// }
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
	// fmt.Println("到这了嘛4")
	index = rf.lastIndex()+1	// 加一是为了除去空指令
	term = rf.currentTerm
	// fmt.Printf("11111offset%v,changdu%v\n",rf.Offsets,len(rf.Offsets))
	if logEntry.Command.OpType != "TermLog" { // 除去上任leader后的空指令
		entry_global = Entry{
			Index:       uint32(index),
			CurrentTerm: uint32(term),
			VotedFor:    uint32(rf.leaderId),
			Key:         command.(*raftrpc.DetailCod).Key,
			Value:       command.(*raftrpc.DetailCod).Value,
		}
		arrEntry := []*Entry{&entry_global}
		rf.WriteEntryToFile(arrEntry, rf.currentLog, 0)
	}
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
	rf.log = append(rf.log, &logEntry) // 确保日志落盘之后，再更新log
	rf.mu.Unlock()
	// fmt.Printf("22222offset%v,changdu%v\n",rf.Offsets,len(rf.Offsets))
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

func (rf *Raft) RegisterRaftServer(ctx context.Context, baseAddr string) {
    // 从基础地址中获取主机名和基础端口
    host := strings.Split(baseAddr, ":")[0]
    basePort, _ := strconv.Atoi(strings.Split(baseAddr, ":")[1])
    
    // 创建多个监听器，每个都在不同的端口上
    for portOffset := 0; portOffset < 3; portOffset++ { // 使用3个端口
        port := basePort + portOffset
        address := fmt.Sprintf("%s:%d", host, port)
        
        // 为每个端口启动一个独立的gRPC服务器
        go func(addr string) {
            for {
                lis, err := net.Listen("tcp", addr)
                if err != nil {
                    util.FPrintf("failed to listen on %s: %v", addr, err)
                    time.Sleep(time.Second) // 如果监听失败，等待一会再重试
                    continue
                }
                
                grpcServer := grpc.NewServer(
                    grpc.InitialWindowSize(pool.InitialWindowSize),
                    grpc.InitialConnWindowSize(pool.InitialConnWindowSize),
                    grpc.MaxSendMsgSize(pool.MaxSendMsgSize),
                    grpc.MaxRecvMsgSize(pool.MaxRecvMsgSize),
                    grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
                        PermitWithoutStream: true,
                        MinTime:             10 * time.Second,
                    }),
                    grpc.KeepaliveParams(keepalive.ServerParameters{
                        Time:                  pool.KeepAliveTime,
                        Timeout:               pool.KeepAliveTimeout,
                        MaxConnectionAgeGrace: 20 * time.Second,
                    }),
                )
                
                raftrpc.RegisterRaftServer(grpcServer, rf)
                reflection.Register(grpcServer)
                
                // 处理服务器优雅关闭
                go func() {
                    <-ctx.Done()
                    grpcServer.GracefulStop()
                    fmt.Printf("Raft stopped on port %s due to context cancellation\n", addr)
                }()
                
                util.DPrintf("Starting Raft server on: %s", addr)
                if err := grpcServer.Serve(lis); err != nil {
                    util.FPrintf("failed to serve on %s: %v", addr, err)
                }
                
                break // 如果Serve返回错误，跳出循环
            }
        }(address)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	reply, err := client.RequestVote(ctx, args)

	if err != nil {
		util.EPrintf("Error calling RequestVote method on server side; err:%v; address:%v ", err, address)
		return false, reply
	} else {
		return true, reply
	}
}

// func (rf *Raft) sendAppendEntries(address string, args *raftrpc.AppendEntriesInRaftRequest, p pool.Pool) (*raftrpc.AppendEntriesInRaftResponse, bool) {
// 	// 用grpc连接池同步日志
// 	conn, err := p.Get()
// 	if err != nil {
// 		util.EPrintf("failed to get conn: %v", err)
// 		return nil, false

// 	}
// 	defer conn.Close()
// 	client := raftrpc.NewRaftClient(conn.Value())
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
// 	defer cancel()
// 	reply, err := client.AppendEntriesInRaft(ctx, args)

// 	if err != nil {
// 		// util.EPrintf("Error calling AppendEntriesInRaft method on server side; err:%v; address:%v ", err, address)
// 		return reply, false
// 	}
// 	return reply, true
// }

// sendAppendEntries 发送流式日志复制请求
func (rf *Raft) sendAppendEntries(address string, args *raftrpc.AppendEntriesInRaftRequest, peerId int) (*raftrpc.AppendEntriesInRaftResponse, bool) {
    poolIndex := atomic.AddInt32(&rf.syncIndex[peerId], 1) % int32(rf.portCount)
    p := rf.pools[peerId][poolIndex]
    
    conn, err := p.Get()
    if err != nil {
        return nil, false
    }
    defer conn.Close()
    
    client := raftrpc.NewRaftClient(conn.Value())
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
    defer cancel()

    // 建立双向流
    stream, err := client.AppendEntriesInRaft(ctx)
    if err != nil {
        return nil, false
    }

    // 发送请求
    if err := stream.Send(args); err != nil {
        return nil, false
    }

    // 接收响应
    reply, err := stream.Recv()
    if err != nil {
        return nil, false
    }

    // 关闭发送
    if err := stream.CloseSend(); err != nil {
        return nil, false
    }

    return reply, true
}

// // 修改sendAppendEntries函数使用多端口
// func (rf *Raft) sendAppendEntries(address string, args *raftrpc.AppendEntriesInRaftRequest, peerId int) (*raftrpc.AppendEntriesInRaftResponse, bool) {
//     // 轮询选择端口
//     poolIndex := atomic.AddInt32(&rf.syncIndex[peerId], 1) % int32(rf.portCount)
//     p := rf.pools[peerId][poolIndex]
    
//     conn, err := p.Get()
//     if err != nil {
//         util.EPrintf("failed to get conn: %v", err)
//         return nil, false
//     }
//     defer conn.Close()
    
//     client := raftrpc.NewRaftClient(conn.Value())
//     ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
//     defer cancel()
    
//     reply, err := client.AppendEntriesInRaft(ctx, args)
//     if err != nil {
// 		// util.EPrintf("Error calling AppendEntriesInRaft method on server side; err:%v; address:%v ", err, address)
//         return reply, false
//     }
//     return reply, true
// }

func (rf *Raft) sendHeartbeat(address string, args *raftrpc.AppendEntriesInRaftRequest, peerId int) (*raftrpc.AppendEntriesInRaftResponse, bool) {
    // 轮询选择一个连接池
    poolIndex := atomic.AddInt32(&rf.syncIndex[peerId], 1) % int32(len(rf.pools[peerId]))
    p := rf.pools[peerId][poolIndex]
    
    // 使用选择的连接池获取连接
    conn, err := p.Get()
    if err != nil {
        util.EPrintf("failed to get conn for heartbeat: %v", err)
        return nil, false
    }
    defer conn.Close()

    client := raftrpc.NewRaftClient(conn.Value())
    ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
    defer cancel()

    reply, err := client.HeartbeatInRaft(ctx, args)

    if err != nil {
        util.EPrintf("Error calling HeartbeatInRaft method on server side; err:%v; address:%v poolIndex:%d", 
            err, address, poolIndex)
        return reply, false
    }
    return reply, true
}

func (rf *Raft) AppendMonitor() {
	timeout := 3 * time.Second
	for {
		time.Sleep(2 * time.Second)
		if (time.Since(rf.LastAppendTime) > timeout) && rf.GetLeaderId() != int32(rf.me) {
			//  排在第一的服务器和后面的服务器，打印的内容是不一样的。因为排在第一个的默认就是满足第二个条件了。
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
			timeout := time.Duration(5000+rand.Int31n(150)) * time.Millisecond // 超时随机化 10s-10s150ms
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

					op := raftrpc.DetailCod{
						OpType: "TermLog",
					}
					rf.mu.Unlock()
					op.Index, op.Term, _ = rf.Start(&op) // 需要提交一个空的指令，需要在初始化nextindex之后，提交空指令
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
	// fmt.Printf("newconmittindex%v\n",newCommitIndex)
	// if语句的第一个条件则是排除掉还没有复制到大多数server的情况
	// fmt.Printf("此时log的长度：%v以及newcommitindex的值：%v\n",len(rf.log),newCommitIndex)
	if newCommitIndex > rf.commitIndex && rf.log[rf.index2LogPos(newCommitIndex)].Term == int32(rf.currentTerm) {
		rf.commitIndex = newCommitIndex // 保证是当前的Term才能根据同步到server的副本数量判断是否可以提交
		// fmt.Println("上任空包被提交了")	// 提交了的，因为虽然是空包，但是也赋予了当前任期，满足提交条件
	}
	// util.DPrintf("RaftNode[%d] updateCommitIndex, newCommitIndex[%d] matchIndex[%v]", rf.me, rf.commitIndex, sortedMatchIndex)
}

// doAppendEntries 准备并发送流式日志
func (rf *Raft) doAppendEntries(peerId int) {
    var buffer bytes.Buffer
    enc := gob.NewEncoder(&buffer)
    var totalSize int64
    var appendLog []*raftrpc.LogEntry

    args := raftrpc.AppendEntriesInRaftRequest{
        Term:         int32(rf.currentTerm),
        LeaderId:     int32(rf.me),
        LeaderCommit: int32(rf.commitIndex),
        PrevLogIndex: int32(rf.nextIndex[peerId] - 1),
        PrevLogTerm:  0,
    }

    if args.PrevLogIndex != 0 {
        args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
    }

    // 准备日志条目
    for i := rf.index2LogPos(int(args.PrevLogIndex) + 1); i < len(rf.log); i++ {
        if rf.log[i] == nil {
            continue
        }
        if err := enc.Encode(rf.log[i]); err != nil {
            fmt.Println("Encode error：", err)
        }
        totalSize += int64(buffer.Len())
        if totalSize >= threshold {
            appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):i]
            break
        }
    }
    if totalSize < threshold {
        appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]
    }
    buffer.Reset()
    args.Entries = appendLog

    rf.LastAppendTime = time.Now()

    go func(peerId int) {
        if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, peerId); ok {
            rf.mu.Lock()
            defer rf.mu.Unlock()

            if rf.currentTerm != int(args.Term) {
                rf.SyncChans[peerId] <- "NotLeader"
                return
            }

            if reply.Term > int32(rf.currentTerm) {
                rf.role = ROLE_FOLLOWER
                rf.leaderId = 0
                rf.currentTerm = int(reply.Term)
                rf.votedFor = -1
                rf.SyncChans[peerId] <- "NotLeader"
                return
            }

            if reply.Success {
                rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(appendLog) + 1
                rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1
                rf.updateCommitIndex()
            } else {
                if reply.ConflictTerm != -1 {
                    conflictTermIndex := -1
                    for index := args.PrevLogIndex; index > 0; index-- {
                        if rf.log[rf.index2LogPos(int(index))].Term != reply.ConflictTerm {
                            conflictTermIndex = int(index + 1)
                            break
                        }
                    }
                    if conflictTermIndex != -1 {
                        rf.nextIndex[peerId] = conflictTermIndex
                    } else {
                        rf.nextIndex[peerId] = int(reply.ConflictIndex)
                    }
                } else {
                    rf.nextIndex[peerId] = int(reply.ConflictIndex)
                }
            }
            rf.SyncChans[peerId] <- strconv.Itoa(peerId)
        } else {
            rf.SyncChans[peerId] <- strconv.Itoa(peerId)
        }
    }(peerId)
}

// 已兼容snapshot
// func (rf *Raft) doAppendEntries(peerId int) {
// 	var buffer bytes.Buffer
// 	enc := gob.NewEncoder(&buffer)
// 	var totalSize int64
// 	var appendLog []*raftrpc.LogEntry

// 	args := raftrpc.AppendEntriesInRaftRequest{}
// 	args.Term = int32(rf.currentTerm)
// 	args.LeaderId = int32(rf.me)
// 	args.LeaderCommit = int32(rf.commitIndex)
// 	args.PrevLogIndex = int32(rf.nextIndex[peerId] - 1)	// 减一是为了拿到下标
// 	if args.PrevLogIndex == 0 { // 确保在从0开始的时候直接进行日志追加即可
// 		args.PrevLogTerm = 0
// 	} else {
// 		// fmt.Printf("此时log%v,PrevLogIndex%v\n",len(rf.log),args.PrevLogIndex)
// 		args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
// 	}
// 	// start := rf.index2LogPos(int(args.PrevLogIndex)+1)
// 	// if (start + 0)<len(rf.log)  {
// 	// 	appendLog = rf.log[start:start + 100]
// 	// }else{
// 	// 	appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):] //这里如果下标大于或等于log数组的长度，只是会返回一个空切片，所以正好当作心跳使用
// 	// }

// 	// 设置日志同步的阈值
// 	// fmt.Println("The length of appendlog:",len(rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]))
// 	for i := rf.index2LogPos(int(args.PrevLogIndex) + 1); i < len(rf.log); i++ {
// 		if rf.log[i] == nil {
// 			fmt.Printf("rf.log的第%v个为nil\n", i)
// 			continue
// 		}
// 		if err := enc.Encode(rf.log[i]); err != nil { // 将 rf.log[i] 日志项编码后的字节序列写入到 buffer 缓冲区中
// 			fmt.Println("Encode error：", err)
// 		}
// 		totalSize += int64(buffer.Len())
// 		// 如果总大小超过3MB，截取日志数组并退出循环
// 		if totalSize >= threshold {
// 			appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):i]	// 不包括第i个索引
// 			break
// 		}
// 	}
// 	if totalSize < threshold {
// 		appendLog = rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]
// 	}
// 	buffer.Reset()
// 	args.Entries = appendLog

// 	// fmt.Printf("此时下标会不会有问题，log长度：%v，下标：%v", len(rf.log), args.PrevLogIndex+1)
// 	// data, _ := json.Marshal(appendLog) // 后续计算日志的长度的时候可千万别用这个转换后的直接数组
// 	// args.Entries = data
// 	// args.Entries = append(args.Entries, rf.log[rf.index2LogPos(int(args.PrevLogIndex)+1):]...)
// 	// util.DPrintf("RaftNode[%d] appendEntries starts,  currentTerm[%d] peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] args.Entries[%d] commitIndex[%d]",
// 	// 	rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], len(args.Entries), rf.commitIndex)

// 	// if len(appendLog) != 0 { // 除去普通的心跳
// 	rf.LastAppendTime = time.Now() // 检查有没有收到日志同步，是不是自己的连接断掉了
// 	// 	// fmt.Println("重置lastAppendTime")
// 	// }

// 	go func(peerId int) {
// 		// util.DPrintf("RaftNode[%d] appendEntries starts, myTerm[%d] peerId[%d]", rf.me, args.Term, args.LeaderId)
// 		if reply, ok := rf.sendAppendEntries(rf.peers[peerId], &args, peerId); ok {
// 			rf.mu.Lock()
// 			defer rf.mu.Unlock()
// 			// defer func() {
// 			// 	util.DPrintf("RaftNode[%d] appendEntries ends,  currentTerm[%d]  peer[%d] logIndex=[%d] nextIndex[%d] matchIndex[%d] commitIndex[%d]",
// 			// 		rf.me, rf.currentTerm, peerId, rf.lastIndex(), rf.nextIndex[peerId], rf.matchIndex[peerId], rf.commitIndex)
// 			// }()

// 			// 如果不是rpc前的leader状态了，那么啥也别做了，可能遇到了term更大的server，因为rpc的时候是没有加锁的
// 			if rf.currentTerm != int(args.Term) {
// 				rf.SyncChans[peerId] <- "NotLeader"
// 				return
// 			}
// 			if reply.Term > int32(rf.currentTerm) { // 变成follower
// 				rf.role = ROLE_FOLLOWER
// 				rf.leaderId = 0
// 				rf.currentTerm = int(reply.Term)
// 				rf.votedFor = -1
// 				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
// 				rf.SyncChans[peerId] <- "NotLeader"
// 				return
// 			}
// 			// 因为RPC期间无锁, 可能相关状态被其他RPC修改了
// 			// 因此这里得根据发出RPC请求时的状态做更新，而不要直接对nextIndex和matchIndex做相对加减
// 			if reply.Success { // 同步日志成功
// 				rf.nextIndex[peerId] = int(args.PrevLogIndex) + len(appendLog) + 1
// 				rf.matchIndex[peerId] = rf.nextIndex[peerId] - 1 // 记录已经复制到其他server的日志的最后index的情况
// 				rf.updateCommitIndex()                           // 更新commitIndex
// 			} else {
// 				// 回退优化，参考：https://thesquareplanet.com/blog/students-guide-to-raft/#an-aside-on-optimizations
// 				// nextIndexBefore := rf.nextIndex[peerId] // 仅为打印log

// 				if reply.ConflictTerm != -1 { // follower的prevLogIndex位置term冲突了
// 					// 我们找leader log中conflictTerm最后出现位置，如果找到了就用它作为nextIndex，否则用follower的conflictIndex
// 					conflictTermIndex := -1
// 					for index := args.PrevLogIndex; index > 0; index-- {
// 						// if rf.log[rf.index2LogPos(int(index))].Term == reply.ConflictTerm {
// 						// 	conflictTermIndex = int(index)
// 						// 	break
// 						// }
// 						// 我认为下方这个效果更好，这样PrevLogIndex的值就为 index
// 						if rf.log[rf.index2LogPos(int(index))].Term != reply.ConflictTerm {
// 							conflictTermIndex = int(index + 1)
// 							break
// 						}
// 					}
// 					if conflictTermIndex != -1 { // leader log出现了这个term，那么从这里prevLogIndex之前的最晚出现位置尝试同步
// 						rf.nextIndex[peerId] = conflictTermIndex
// 					} else {
// 						rf.nextIndex[peerId] = int(reply.ConflictIndex) // 用follower首次出现term的index作为同步开始
// 					}
// 				} else {
// 					// follower没有发现prevLogIndex term冲突, 可能是被snapshot了或者日志长度不够
// 					// 这时候我们将返回的conflictIndex设置为nextIndex即可
// 					rf.nextIndex[peerId] = int(reply.ConflictIndex)
// 				}
// 				// util.DPrintf("RaftNode[%d] back-off nextIndex, peer[%d] nextIndexBefore[%d] nextIndex[%d]", rf.me, peerId, nextIndexBefore, rf.nextIndex[peerId])
// 			}
// 			// rf.SyncChans[peerId] <- rf.peers[peerId]
// 			rf.SyncChans[peerId] <- strconv.Itoa(peerId)
// 		} else {
// 			// rf.SyncChans[peerId] <- rf.peers[peerId]	// 同步日志失败也要重新发起日志同步
// 			rf.SyncChans[peerId] <- strconv.Itoa(peerId)
// 		}
// 	}(peerId)
// }

// 对应地需要修改调用sendHeartbeat的地方，比如在doHeartBeat中
func (rf *Raft) doHeartBeat(peerId int) {
    args := raftrpc.AppendEntriesInRaftRequest{
        Term:         int32(rf.currentTerm),
        LeaderId:     int32(rf.me),
        LeaderCommit: int32(rf.commitIndex),
        PrevLogIndex: int32(rf.nextIndex[peerId] - 1),
    }
    
    if args.PrevLogIndex == 0 {
        args.PrevLogTerm = 0
    } else {
        args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
    }
    args.Entries = []*raftrpc.LogEntry{}

    go func(peerId int) {
        if reply, ok := rf.sendHeartbeat(rf.peers[peerId], &args, peerId); ok {
            rf.mu.Lock()
            defer rf.mu.Unlock()
            
            if rf.currentTerm != int(args.Term) {
                return
            }
            
            if reply.Term > int32(rf.currentTerm) {
                rf.role = ROLE_FOLLOWER
                rf.currentTerm = int(reply.Term)
                rf.votedFor = -1
                return
            }
        }
    }(peerId)
}

func (rf *Raft) CheckActive(peerId int, resultChan chan<- bool) {
    args := raftrpc.AppendEntriesInRaftRequest{
        Term:         int32(rf.currentTerm),
        LeaderId:     int32(rf.me),
        LeaderCommit: int32(rf.commitIndex),
        PrevLogIndex: int32(rf.nextIndex[peerId] - 1),
    }
    
    if args.PrevLogIndex == 0 {
        args.PrevLogTerm = 0
    } else {
        args.PrevLogTerm = int32(rf.log[rf.index2LogPos(int(args.PrevLogIndex))].Term)
    }
    args.Entries = []*raftrpc.LogEntry{}

    if reply, ok := rf.sendHeartbeat(rf.peers[peerId], &args, peerId); ok {
        rf.mu.Lock()
        if rf.currentTerm != int(args.Term) {
            rf.mu.Unlock()
            return
        }
        
        if reply.Term > int32(rf.currentTerm) {
            rf.role = ROLE_FOLLOWER
            rf.currentTerm = int(reply.Term)
            rf.votedFor = -1
            rf.mu.Unlock()
            return
        }
        
        if reply.Success {
            resultChan <- true
        } else {
            resultChan <- false
        }
        rf.mu.Unlock()
    } else {
        resultChan <- false
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

func (rf *Raft) appendEntriesLoop() {
	First := true
	for !rf.killed() {
		// time.Sleep(time.Duration(rf.SyncTime) * time.Millisecond) // 间隔10ms

		func() {
			rf.mu.Lock() // 这里可以用读锁
			// defer rf.mu.Unlock()

			// 只有leader才向外广播心跳
			if rf.role != ROLE_LEADER {
				rf.mu.Unlock()
				return
			}
			// 100ms广播1次
			// now := time.Now()
			// if now.Sub(rf.lastBroadcastTime) < 11*time.Millisecond {
			// 	return
			// }
			if rf.lastIndex() == 0 {
				rf.mu.Unlock()
				return
			}
			// rf.lastBroadcastTime = time.Now() // 确定过了广播的时间间隔，才开始进行广播，并且设置新的广播时间
			// 向所有follower发送心跳
			// for peerId := 0; peerId < len(rf.peers); peerId++ {
			// for peerId := 0; peerId < 3; peerId++ { // 先固定，避免访问rf的属性，涉及到死锁问题
			// 	if peerId == rf.me {
			// 		continue
			// 	}
			// if (now.Sub(rf.LastAppendTime) > 300*time.Millisecond) && Heartbeat == 1 {
			rf.mu.Unlock()
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
			now := time.Now() // 心跳
			if now.Sub(rf.LastAppendTime) > 500*time.Millisecond {
				for peerId := 0; peerId < len(rf.peers); peerId++ { // 先固定，避免访问rf的属性，涉及到死锁问题
					if peerId == rf.me {
						continue
					}
					rf.doHeartBeat(peerId)
				}
				rf.LastAppendTime = time.Now()
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
			// fmt.Printf("此时的offset的长度是多少：%v",len(rf.Offsets))
			if (rf.commitIndex > rf.lastApplied) && ((rf.lastApplied - rf.shotOffset) <
				len(rf.Offsets)) {
					// fmt.Printf("提交了一次commitidnex%v-lastapplied%v-shotoffset%v-len(off)%v\n",rf.commitIndex,rf.lastApplied,rf.shotOffset,len(rf.Offsets))
					// fmt.Println("offset",rf.Offsets)
				// if rf.commitIndex > rf.lastApplied {
				// rf.raftStateForPersist("./raft/RaftState.log", rf.currentTerm, rf.votedFor, rf.log)
				rf.lastApplied += 1
				// util.DPrintf("RaftNode[%d] applyLog, currentTerm[%d] lastApplied[%d] commitIndex[%d] Offsets%d", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex, rf.Offsets)
				appliedIndex := rf.index2LogPos(rf.lastApplied)
				realIndex := rf.lastApplied - rf.shotOffset // 截断前1个数据,后续可以优化，考虑批量删除
				appliedMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[appliedIndex].Command,
					CommandIndex: rf.lastApplied,
					CommandTerm:  int(rf.log[appliedIndex].Term),
					Offset:       rf.Offsets[realIndex-1], // 将偏移量传进通道
					// Offset:       rf.Offsets[appliedIndex],
				}
				// fmt.Printf("发了index:%v给服务器端\n",appliedMsg.Offset)
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
		rf.SyncChans = append(rf.SyncChans, make(chan string, 1000))
	}

	rf.role = ROLE_FOLLOWER
	rf.leaderId = 0
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh
	rf.Offsets = append(rf.Offsets, 0) // 初始化时添加一个0，使得后续对index的访问和raft的对其，从1开始

	// 初始化 syncIndex
    rf.syncIndex = make([]int32, len(peers))
	rf.portCount = 3
    // 初始化连接池
    rf.pools = make([][]pool.Pool, len(peers))
    for i := 0; i < len(peers); i++ {
        rf.pools[i] = make([]pool.Pool, 3) // 为每个peer创建3个连接池
        
        // 解析基础地址
        peerHost := strings.Split(peers[i], ":")[0]
        basePort, _ := strconv.Atoi(strings.Split(peers[i], ":")[1])
        
        // 为每个端口创建连接池
        for j := 0; j < 3; j++ {
            addr := fmt.Sprintf("%s:%d", peerHost, basePort+j)
            p, err := pool.New([]string{addr}, pool.Options{
                Dial:                 pool.Dial,
                MaxIdle:              50,
                MaxActive:            150,
                MaxConcurrentStreams: 400,
                Reuse:               true,
            })
            if err != nil {
                util.EPrintf("failed to new pool for %s: %v", addr, err)
            }
            rf.pools[i][j] = p
        }
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
				for i := range rf.pools {
					for j := range rf.pools[i] {
						rf.pools[i][j].Close()
					}
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
