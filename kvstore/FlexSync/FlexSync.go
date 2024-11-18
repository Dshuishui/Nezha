package main

import (
	"context"
	"errors"
	"io"

	// "runtime"

	// "io"
	"strconv"

	// "encoding/json"
	"flag"
	"fmt"
	"sync/atomic"

	// "math/rand"
	"bufio"
	"encoding/binary"
	"log"
	"net"
	_ "net/http/pprof"
	"os"

	// "sort"
	"strings"
	"sync"
	"time"

	// "gitee.com/dong-shuishui/FlexSync/config"
	// "gitee.com/dong-shuishui/FlexSync/kvstore/GC4"

	"gitee.com/dong-shuishui/FlexSync/raft"
	// "gitee.com/dong-shuishui/FlexSync/persister"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"

	// "gitee.com/dong-shuishui/FlexSync/gc4"

	// "gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
	"gitee.com/dong-shuishui/FlexSync/pool"
	// "gitee.com/dong-shuishui/FlexSync/kvstore/GC4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/tecbot/gorocksdb"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	// lru "github.com/hashicorp/golang-lru"
	"github.com/edsrzf/mmap-go"
)

var (
	internalAddress_arg = flag.String("internalAddress", "", "Input Your address") // 返回的是一个指向string类型的指针
	address_arg         = flag.String("address", "", "Input Your address")
	peers_arg           = flag.String("peers", "", "Input Your Peers")
	gap_arg             = flag.String("gap", "", "Input Your gap")
	syncTime_arg        = flag.String("syncTime", "", "Input Your syncTime")
)

const (
	OP_TYPE_PUT = "Put"
	OP_TYPE_GET = "Get"
)

type IndexEntry struct {
	Key    string
	Offset int64
}

type SortedFileIndex struct {
	Entries  map[string]int64 // 改用map来存储键值对
	FilePath string
}

type KVServer struct {
	mu              sync.Mutex
	peers           []string
	address         string
	internalAddress string    // internal address for communication between nodes
	lastPutTime     time.Time // lastPutTime记录最后一次PUT请求的时间
	// valuelog        *ValueLog
	// pools           []pool.Pool // 用于日志同步的连接池

	me        int
	raft      *raft.Raft
	persister *raft.Persister    // 对数据库进行读写操作的接口
	applyCh   chan raft.ApplyMsg // 用于与Raft层面传输数据的通道
	dead      int32              // set by Kill()
	reqMap    map[int]*OpContext // log index -> 请求上下文
	seqMap    map[int64]int64    // 客户端id -> 客户端seq

	lastAppliedIndex int // 已持久化存储的日志index
	kvrpc.UnimplementedKVServer
	// resultCh  chan *kvrpc.PutInRaftResponse

	sortedFilePath  string // 用于存储已排序文件的位置
	sortedFileIndex *SortedFileIndex
	currentLog      string          // 排序后
	oldLog          string          // 排序前
	oldPersister    *raft.Persister // 排序前
	startGC         bool            // GC是否开始
	endGC           bool            // GC是否结束
	valueSize       int             // valuesize
	// currentPersister *raft.Persister
	// getFromFile     func(string) (string, error)			// 对应与垃圾分离前后的两种查询方法。
	// scanFromFile    func(string, string) (map[string]string, error)
	getMeasurements []time.Duration

	sortedFileCache *lru.Cache  // 用于缓存key到offset的映射
}

// ValueLog represents the Value Log file for storing values.
type ValueLog struct {
	file         *os.File
	leveldb      *leveldb.DB
	valueLogPath string
}

type Op struct {
	Index    int    // 写入raft log时的index
	Term     int    // 写入raft log时的term
	Type     string // Put、Get
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *raftrpc.DetailCod
	committed chan byte

	wrongLeader bool // 因为index位置log的term不一致, 说明leader换过了
	ignored     bool // 因为req id过期, 表示已经执行过，该日志需要被跳过

	// Get操作的结果
	keyExist bool
	value    string
}

var wg = sync.WaitGroup{}

func newOpContext(op *raftrpc.DetailCod) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kvs *KVServer) ScanRangeInRaft(ctx context.Context, in *kvrpc.ScanRangeRequest) (*kvrpc.ScanRangeResponse, error) {
	reply := &kvrpc.ScanRangeResponse{Err: raft.OK}

	// commitIndex, isLeader := kvs.raft.GetReadIndex()
	// if !isLeader {
	// 	reply.Err = raft.ErrWrongLeader
	// 	reply.LeaderId = kvs.raft.GetLeaderId()
	// 	return reply, nil
	// }

	// for {
	// 	if kvs.raft.GetApplyIndex() >= commitIndex {
	result, err := kvs.scanFromSortedOrNew(in.StartKey, in.EndKey)
	if err != nil {
		reply.Err = "error in scan"
		return reply, nil
	}
	reply.KeyValuePairs = result
	return reply, nil
	// }
	// 	time.Sleep(6 * time.Millisecond) // 等待applyindex赶上commitindex
	// }
	// ————以下是之前的scan查询————
	// reply := kvs.StartScan(in)
	// 检查是否已经垃圾回收完毕
	// 垃圾回收完毕再调用在已排序文件的scan方法，范围查询结果，最好用goroutine，两者同时进行scan查询
	// 如果垃圾回收没完，需要调用在旧未排序的文件，进行范围查询
	// 还有一个比较复杂的情况，针对已排序文件，继已排序文件后的新文件，以及前两者即将合并时又生成的新文件。
	// 这三个文件就比较复杂，需要在最新文件、新文件、已排序的文件同时查询。
	// 后面再合并两者的结果，或者合并三者的结果
	// 返回即可
	// if reply.Err == raft.ErrWrongLeader {
	// reply.LeaderId = kvs.raft.GetLeaderId()
	// } else if reply.Err == raft.ErrNoKey {
	// 返回客户端没有该key即可，这里先不做操作
	// fmt.Println("server端没有client查询的key")
	// } else if reply.Err == "error in scan" {
	// reply.Err = "error in scan"
	// }
	// return reply, nil
}

func (kvs *KVServer) scanFromSortedOrNew(startKey, endKey string) (map[string]string, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	type scanResult struct {
		data map[string]string
		err  error
	}

	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if kvs.startGC && !kvs.endGC {
		// 并发查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.oldPersister, kvs.oldLog)
			sortedChan <- scanResult{data: result.KeyValuePairs, err: nil}
		}()

		// 并发查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			// if err != nil {
			//     newChan <- scanResult{data: nil, err: err}
			//     return
			// }
			newChan <- scanResult{data: result.KeyValuePairs, err: nil}
		}()
	}
	if kvs.startGC && kvs.endGC {
		// 并发查询排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 并发查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			// if err != nil {
			//     newChan <- scanResult{data: nil, err: err}
			//     return
			// }
			newChan <- scanResult{data: result.KeyValuePairs, err: nil}
		}()
	}
	if !kvs.startGC {
		// 只查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.oldPersister, kvs.oldLog)
			sortedChan <- scanResult{data: result.KeyValuePairs, err: nil}
		}()
		wg.Done()
		wg.Wait()
		close(sortedChan)
		close(newChan)
		sortedResult := <-sortedChan
		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		return result, nil //  不用合并，直接退出即可
	}
	// 等待两个查询都完成
	wg.Wait()
	close(sortedChan)
	close(newChan)

	// 获取结果
	sortedResult := <-sortedChan
	newResult := <-newChan

	// 检查错误
	if sortedResult.err != nil {
		return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
	}
	if newResult.err != nil {
		return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
	}

	// 合并结果
	result := make(map[string]string)
	for k, v := range sortedResult.data {
		result[k] = v
	}
	for k, v := range newResult.data {
		result[k] = v // 新文件的数据会覆盖排序文件中的旧数据
	}

	return result, nil
}

func (kvs *KVServer) StartScan_opt(args *kvrpc.ScanRangeRequest, persister *raft.Persister, logLocation string) *kvrpc.ScanRangeResponse {
	startKey := args.GetStartKey()
	endKey := args.GetEndKey()
	reply := &kvrpc.ScanRangeResponse{Err: raft.OK}

	// 执行范围查询
	result, err := kvs.scanNewFile(startKey, endKey, persister, logLocation)
	if err != nil {
		log.Printf("Scan error: %v", err)
		reply.Err = "error in scan"
		return reply
	}

	// 构造响应并返回
	reply.KeyValuePairs = result
	return reply
}

func (kvs *KVServer) scanNewFile(startKey, endKey string, persister *raft.Persister, logLocation string) (map[string]string, error) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	result := make(map[string]string)
	paddedStartKey := kvs.persister.PadKey(startKey)
	paddedEndKey := kvs.persister.PadKey(endKey)

	// 从RocksDB中获取范围内的key-value对
	rdb := persister.GetDb()
	iter := rdb.NewIterator(ro)
	defer iter.Close()

	for iter.Seek([]byte(paddedStartKey)); iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		if key > paddedEndKey {
			break
		}

		// 从新的日志文件中读取实际的value
		value, err := ReadValueFromNewFile(iter.Value().Data(), logLocation) // 读取与rocksdb对应的log
		if err != nil {
			return nil, err
		}
		originalKey := kvs.persister.UnpadKey(string(key))
		result[originalKey] = value
	}

	return result, nil
}

// ==================================================
func ReadValueFromNewFile(positionBytes []byte, logLocation string) (string, error) {
	position := int64(binary.LittleEndian.Uint64(positionBytes))

	// Open the file
	file, err := os.Open(logLocation)
	if err != nil {
		return "", fmt.Errorf("failed to open log file: %v", err)
	}
	defer file.Close()

	// Seek to the position
	_, err = file.Seek(position, 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek in file: %v", err)
	}

	reader := bufio.NewReader(file)
	entry, _, err := ReadEntry(reader, 0) // 保留了 0，但你可能需要根据 ReadEntry 函数的实际需求调整这个值
	if err != nil {
		return "", fmt.Errorf("failed to read entry: %v", err)
	}

	return entry.Value, nil
}

func ReadEntry(reader *bufio.Reader, currentOffset int64) (*raft.Entry, int64, error) {
	var entry raft.Entry
	var keySize, valueSize uint32

	// Read all 20 bytes at once
	header := make([]byte, 20)
	n, err := io.ReadFull(reader, header)
	if err != nil {
		if err == io.EOF && n == 0 {
			return nil, 0, io.EOF // File is empty or we're at the end
		}
		return nil, 0, fmt.Errorf("failed to read header: %v (read %d bytes)", err, n)
	}

	// Parse the header
	keySize = binary.LittleEndian.Uint32(header[12:16])
	valueSize = binary.LittleEndian.Uint32(header[16:20])

	// Calculate total size
	entrySize := int64(20 + keySize + valueSize)

	// Read key and value
	data := make([]byte, keySize+valueSize)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read key and value: %v", err)
	}

	entry.Key = string(data[:keySize])
	entry.Value = string(data[keySize:])

	return &entry, entrySize, nil
}

// ==================================================

func (kvs *KVServer) StartScan(args *kvrpc.ScanRangeRequest) *kvrpc.ScanRangeResponse {
	startKey := args.GetStartKey()
	endKey := args.GetEndKey()
	reply := &kvrpc.ScanRangeResponse{Err: raft.OK}

	commitIndex, isLeader := kvs.raft.GetReadIndex()
	if !isLeader {
		reply.Err = raft.ErrWrongLeader
		reply.LeaderId = kvs.raft.GetLeaderId()
		return reply // 不是leader，拿不到commitindex直接退出，找其它leader
	}

	for {
		if kvs.raft.GetApplyIndex() >= commitIndex {
			// 执行范围查询
			result, err := kvs.persister.ScanRange_opt(startKey, endKey)
			if err != nil {
				log.Printf("Scan error: %v", err)
				reply.Err = "error in scan"
				return reply
			}

			// 处理查询结果
			finalResult := make(map[string]string)
			// var mu sync.Mutex
			var wg sync.WaitGroup

			for key, position := range result {
				wg.Add(1)
				go func(k string, pos int64) {
					defer wg.Done()
					_, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, pos)
					if err != nil {
						// log.Printf("Error reading value for key %s: %v", k, err)
						// mu.Lock()
						// finalResult[k] = raft.NoKey
						// mu.Unlock()
						fmt.Println("scan时，拿取单个key有问题")
						panic(err)
					} else { // 迭代器在rocksdb中找到的key和偏移量数组，里面的key不重复，可以并发修改数组
						// mu.Lock()
						finalResult[k] = value
						// mu.Unlock()
					}
				}(key, position)
			}

			wg.Wait()

			// 构造响应并返回
			reply.KeyValuePairs = finalResult
			return reply
		}
		time.Sleep(6 * time.Millisecond) // 等待applyindex赶上commitindex
	}
}

func (kvs *KVServer) StartGet(args *kvrpc.GetInRaftRequest) *kvrpc.GetInRaftResponse {
	reply := &kvrpc.GetInRaftResponse{Err: raft.OK}
	// commitindex, isleader := kvs.raft.GetReadIndex()
	// if !isleader {
	// 	reply.Err = raft.ErrWrongLeader
	// 	reply.LeaderId = kvs.raft.GetLeaderId()
	// 	return reply // 不是leader，拿不到commitindex直接退出，找其它leader
	// }
	// for { // 证明了此服务器就是leader
	// if kvs.raft.GetApplyIndex() >= commitindex {
	key := args.GetKey()

	if !kvs.startGC { // 还未开始GC，先去旧的rocksdb查询
		// startTime := time.Now()
		positionBytes, err := kvs.oldPersister.Get_opt(key)
		if err != nil {
			fmt.Println("去旧的rocksdb中拿取key对应的index有问题")
			panic(err)
		}
		if positionBytes == -1 { // 旧rocksdb中没有，就是不存在该key
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		} else {
			// fmt.Printf("直接去rocksdb中找花费了%v\n", time.Since(startTime))
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				fmt.Println("拿取value有问题")
				panic(err)
			}
			if read_key == kvs.persister.PadKey(key) {
				reply.Value = value
			} else {
				panic("错乱了，新的rocksdb中的key与index不匹配！！！")
			}
			return reply
		}
	}
	if kvs.startGC && !kvs.endGC {
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			fmt.Println("去新的rocksdb中拿取key对应的index有问题")
			panic(err)
		}
		if positionBytes == -1 { // 新的没有，就去读旧文件的rocksdb
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				fmt.Println("新的文件中没有，转而去旧的rocksdb中拿取key对应的index有问题")
				panic(err)
			}
			if positionBytes == -1 { // 旧rocksdb中没有，就是不存在该key
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
				return reply
			} else {
				read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
				if err != nil {
					fmt.Println("拿取value有问题")
					panic(err)
				}
				if read_key == kvs.persister.PadKey(key) {
					reply.Value = value
					// fmt.Println("找到了key", key)
				} else {
					panic("错乱了，旧的rocksdb中的key与index不匹配！！！")
				}
				return reply
			}
		} else { // 表明新的文件存在该key，则去新的log文件中找
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			// fmt.Println("此处——读完了磁盘文件")
			if err != nil {
				fmt.Println("拿取value有问题")
				panic(err)
			}
			if read_key == kvs.persister.PadKey(key) {
				reply.Value = value
				// fmt.Println("找到了key", key)
			} else {
				panic("错乱了，rocksdb中的key与index不匹配！！！")
			}
			return reply
		}
	}
	if kvs.startGC && kvs.endGC {
		// start := time.Now()
        positionBytes, err := kvs.persister.Get_opt(key)
        // duration := time.Since(start)
		if err != nil {
			fmt.Println("去新的rocksdb中拿取key对应的index有问题")
			panic(err)
		}
		if positionBytes == -1 {
			// fmt.Println("去新文件找了")
			// kvs.getMeasurements = append(kvs.getMeasurements, duration)  // 统计去新rocksdb文件中没有找到该key的时间
			value, err := kvs.getFromSortedFile(key)
			if err == nil {
				reply.Value = value // 找到了，赋值
				// fmt.Println("找到了找到了，通过索引找到的，key为: ",key)
			} else {
				reply.Err = raft.ErrNoKey // 已排序的文件中没有就是没有
				reply.Value = raft.NoKey
			}
			// kvs.OutputMeasurements()
			return reply
		} else { // 表明新的文件存在该key，则去新的log文件中找
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			// fmt.Println("此处——读完了磁盘文件")
			if err != nil {
				fmt.Println("拿取value有问题")
				panic(err)
			}
			if read_key == kvs.persister.PadKey(key) {
				reply.Value = value
				// fmt.Println("找到了key", key)
			} else {
				panic("错乱了，rocksdb中的key与index不匹配！！！")
			}
			return reply
		}
	}
	return reply
	// }
	// time.Sleep(6 * time.Millisecond) // 等待applyindex赶上commitindex
	// }
}

func (kvs *KVServer) OutputMeasurements() {
    if len(kvs.getMeasurements) <= 100 {
        return
    }

    file, err := os.Create("/home/DYC/Gitee/FlexSync/result/NotFound/newRocksdb.txt")
    if err != nil {
        log.Printf("Error creating file: %v", err)
        return
    }
    defer file.Close()

    var total time.Duration
    for i, duration := range kvs.getMeasurements {
        _, err := fmt.Fprintf(file, "Measurement %d: %v\n", i+1, duration)
        if err != nil {
            log.Printf("Error writing to file: %v", err)
            return
        }
        total += duration
    }

    average := total / time.Duration(len(kvs.getMeasurements))
    _, err = fmt.Fprintf(file, "\nAverage: %v\n", average)
    if err != nil {
        log.Printf("Error writing average to file: %v", err)
        return
    }

    log.Printf("Measurements written to get_measurements.txt")
    log.Printf("Average measurement: %v", average)

    // Clear the measurements after output
    // kvs.getMeasurements = kvs.getMeasurements[:0]
}

func (kvs *KVServer) GetInRaft(ctx context.Context, in *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	reply := kvs.StartGet(in)
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	} else if reply.Err == raft.ErrNoKey {
		// 返回客户端没有该key即可，这里先不做操作
		// fmt.Println("执行成功，但是server端没有client查询的key")
	}
	return reply, nil
}

func (kvs *KVServer) PutInRaft(ctx context.Context, in *kvrpc.PutInRaftRequest) (*kvrpc.PutInRaftResponse, error) {
	// fmt.Println("走到了server端的put函数")
	reply := kvs.StartPut(in)
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	}
	return reply, nil

	// 创建一个用于接收处理结果的通道
	// resultCh := make(chan *kvrpc.PutInRaftResponse)
	// // 在 goroutine 中处理请求
	// go func() {
	// // 处理请求的逻辑...
	// // 这里可以根据具体的业务逻辑来处理客户端请求并将其发送到 Raft 集群中

	// // 处理完成后，将结果发送到通道
	// reply := kvs.StartPut(in)
	// if reply.Err == raft.ErrWrongLeader {
	// 	reply.LeaderId = kvs.raft.GetLeaderId()
	// }
	// resultCh <- reply
	// }()

	// // 返回结果通道，让客户端可以等待结果
	// return <-resultCh, nil
}

func (kvs *KVServer) StartPut(args *kvrpc.PutInRaftRequest) *kvrpc.PutInRaftResponse {
	reply := &kvrpc.PutInRaftResponse{Err: raft.OK, LeaderId: 0}
	op := raftrpc.DetailCod{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	op.Index, op.Term, isLeader = kvs.raft.Start(&op)
	if !isLeader {
		// fmt.Println("不是leader，返回")
		reply.Err = raft.ErrWrongLeader
		return reply // 如果收到客户端put请求的不是leader，需要将leader的id返回给客户端的reply中
	}
	opCtx := newOpContext(&op)
	func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kvs.reqMap[int(op.Index)] = opCtx
	}()
	// _,exist:=kvs.reqMap[int(op.Index)]
	// fmt.Println("大撒上的",exist)
	// fmt.Printf("index%v\n",op.Index)

	// fmt.Println("222")

	// func() {
	// 	kvs.mu.Lock()
	// 	defer kvs.mu.Unlock()
	// 	// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
	// 	kvs.reqMap[int(op.Index)] = opCtx
	// }()
	// fmt.Println("333")
	// 超时后，结束apply请求的RPC，清理该请求index的上下文
	defer func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if one, ok := kvs.reqMap[int(op.Index)]; ok {
			if one == opCtx {
				delete(kvs.reqMap, int(op.Index))
			}
		}
	}()
	timer := time.NewTimer(50000 * time.Millisecond)
	defer timer.Stop()
	select {
	// 通道关闭或者有数据传入都会执行以下的分支
	case <-opCtx.committed: // ApplyLoop函数执行完后，会关闭committed通道，再根据相关的值设置请求reply的结果
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = raft.ErrWrongLeader
			// fmt.Println("走了哪个操作1")
			// fmt.Println("设置reply为WrongLeader")
		} else if opCtx.ignored {
			// fmt.Println("走了哪个操作2")
			// 说明req id过期了，该请求被忽略，对MIT这个lab来说只需要告知客户端OK跳过即可
			reply.Err = raft.OK
		}
		// fmt.Println("444")
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		// fmt.Println("Put请求执行超时了，超过了2s，重新让client发送执行")
		// reply.Err = raft.ErrWrongLeader
		reply.Err = "defeat"
		// fmt.Println("555")
	}
	return reply
}

// func (kvs *KVServer) parallelSearchIndex(key string) (int, error) {
// 	paddedKey := kvs.persister.PadKey(key)
// 	chunks := runtime.GOMAXPROCS(0) // 使用可用的CPU核心数
// 	chunkSize := len(kvs.sortedFileIndex.Entries) / chunks

// 	type result struct {
// 		index int
// 		found bool
// 	}

// 	results := make(chan result, chunks)
// 	for i := 0; i < chunks; i++ {
// 		go func(start, end int) {
// 			idx := sort.Search(end-start, func(j int) bool {
// 				return kvs.persister.PadKey(kvs.sortedFileIndex.Entries[start+j].Key) >= paddedKey
// 			})
// 			globalIdx := start + idx
// 			if globalIdx < end && kvs.persister.PadKey(kvs.sortedFileIndex.Entries[globalIdx].Key) == paddedKey {
// 				results <- result{index: globalIdx, found: true}
// 			} else if globalIdx > start {
// 				results <- result{index: globalIdx - 1, found: false}
// 			} else {
// 				results <- result{index: -1, found: false}
// 			}
// 		}(i*chunkSize, min((i+1)*chunkSize, len(kvs.sortedFileIndex.Entries)))
// 	}

// 	bestIndex := -1
// 	for i := 0; i < chunks; i++ {
// 		res := <-results
// 		if res.found {
// 			return res.index, nil // 找到精确匹配，立即返回
// 		}
// 		if res.index > bestIndex {
// 			bestIndex = res.index
// 		}
// 	}

// 	if bestIndex == -1 {
// 		// return -1, errors.New(raft.ErrNoKey)
// 		return -1,nil
// 	}
// 	return bestIndex, nil
// }
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 添加一个方法来快速查找键的偏移量
func (sfi *SortedFileIndex) GetOffset(key string) (int64, bool) {
	offset, exists := sfi.Entries[key]
	return offset, exists
}

// getFromSortedFile 增加直接缓存value的LRU缓存功能
func (kvs *KVServer) getFromSortedFile(key string) (string, error) {
    // 先检查LRU缓存
    if value, ok := kvs.sortedFileCache.Get(key); ok {
        // 缓存命中，直接返回缓存的value
        return value.(string), nil
    }

    // 缓存未命中，使用索引查找
    index := kvs.sortedFileIndex
    offset, exists := index.GetOffset(key)
    if !exists {
        return "", errors.New(raft.ErrNoKey)
    }

    // 打开文件并移动到索引位置
    file, err := os.Open(index.FilePath)
    if err != nil {
        return "", err
    }
    defer file.Close()

    _, err = file.Seek(offset, 0)
    if err != nil {
        return "", err
    }

    reader := bufio.NewReader(file)
    entry, _, err := ReadEntry(reader, offset)
    if err != nil {
        return "", err
    }

    // 将查询到的value添加到缓存中
    kvs.sortedFileCache.Add(key, entry.Value)

    return entry.Value, nil
}

// 在初始化 KVServer 时，需要初始化 LRU 缓存
func (kvs *KVServer) initSortedFileCache(cacheSize int) error {
    cache, err := lru.New(cacheSize)  // 创建指定大小的LRU缓存
    if err != nil {
        return fmt.Errorf("failed to create LRU cache: %v", err)
    }
    kvs.sortedFileCache = cache
    return nil
}
// 普通的
// func (kvs *KVServer) getFromSortedFile(key string) (string, error) {
// 	// 假设我们已经创建了索引并存储在 kvs.sortedFileIndex 中
// 	index := kvs.sortedFileIndex
// 	paddedKey := kvs.persister.PadKey(key)
// 	// startTime := time.Now()
// 	// 二分查找找到小于等于目标key的最大索引项
// 	i := sort.Search(len(index.Entries), func(i int) bool {
// 		return kvs.persister.PadKey(index.Entries[i].Key) > paddedKey
// 	}) - 1

// 	// i, err := kvs.parallelSearchIndex(key)
// 	// if err != nil {
// 	// 	fmt.Println("新的搜索索引的方式有问题！！！")
// 	// 	panic(err)
// 	// }

// 	if i < 0 {
// 		return "", errors.New(raft.ErrNoKey)
// 	}

// 	// 打开文件并移动到索引位置
// 	file, err := os.Open(index.FilePath)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()

// 	_, err = file.Seek(index.Entries[i].Offset, 0)
// 	if err != nil {
// 		return "", err
// 	}
// 	// fmt.Printf("找索引花费了%v\n", time.Since(startTime))
// 	// fmt.Printf("此时的索引对应的key以及后面三个key为%v-%v-%v-%v，以及查找的key为%v\n",index.Entries[i].Key,index.Entries[i+1].Key,index.Entries[i+2].Key,index.Entries[i+3].Key,paddedKey)

// 	reader := bufio.NewReader(file)

// 	// 从索引位置开始线性搜索
// 	for {
// 		entry, _, err := ReadEntry(reader, 0)
// 		if err != nil {
// 			if err == io.EOF {
// 				return "", errors.New(raft.ErrNoKey)
// 			}
// 			return "", err
// 		}

// 		if entry.Key == paddedKey {
// 			return entry.Value, nil
// 		}

// 		if entry.Key > paddedKey {
// 			return "", errors.New(raft.ErrNoKey)
// 		}
// 	}
// }

// 带内存映射的
// func (kvs *KVServer) getFromSortedFile(key string) (string, error) {
// 	index := kvs.sortedFileIndex
// 	paddedKey := kvs.persister.PadKey(key)

// 	// 二分查找找到小于等于目标key的最大索引项
// 	i := sort.Search(len(index.Entries), func(i int) bool {
// 		return kvs.persister.PadKey(index.Entries[i].Key) > paddedKey
// 	}) - 1

// 	if i < 0 {
// 		return "", errors.New(raft.ErrNoKey)
// 	}

// 	// 打开文件
// 	file, err := os.Open(index.FilePath)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer file.Close()

// 	// 获取文件信息
// 	fileInfo, err := file.Stat()
// 	if err != nil {
// 		return "", err
// 	}
// 	fileSize := fileInfo.Size()

// 	// 创建内存映射
// 	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
// 	if err != nil {
// 		return "", err
// 	}
// 	defer mmap.Unmap()

// 	// 确定起始位置
// 	startOffset := index.Entries[i].Offset

// 	// 从索引位置开始线性搜索
// 	for offset := startOffset; offset < fileSize; {
// 		entry, entrySize, err := ReadEntryFromMMap(mmap[offset:])
// 		if err != nil {
// 			if err == io.EOF {
// 				return "", errors.New(raft.ErrNoKey)
// 			}
// 			return "", err
// 		}

// 		if entry.Key == paddedKey {
// 			return entry.Value, nil
// 		}

// 		if entry.Key > paddedKey {
// 			return "", errors.New(raft.ErrNoKey)
// 		}

// 		offset += int64(entrySize)
// 	}

// 	return "", errors.New(raft.ErrNoKey)
// }

// 内存映射，并行索引区间查询
// func (kvs *KVServer) scanFromSortedFile(startKey, endKey string) (map[string]string, error) {
// 	index := kvs.sortedFileIndex
// 	paddedStartKey := kvs.persister.PadKey(startKey)
// 	paddedEndKey := kvs.persister.PadKey(endKey)
// 	// fmt.Printf("Padded start key: %s\n", paddedStartKey)
// 	// fmt.Printf("Padded end key: %s\n", paddedEndKey)
// 	// fmt.Printf("First index key: %s\n", index.Entries[0].Key)
// 	// fmt.Printf("Last index key: %s\n", index.Entries[len(index.Entries)-1].Key)

// 	// 1. 使用二分查找找到开始和结束的索引
// 	// （注意下面index中的entrys中的key没有填充，且下面的kvs.persister没有什么特殊含义，就是为了调用PadKey函数）
// 	startIndex := sort.Search(len(index.Entries), func(i int) bool {
// 		return kvs.persister.PadKey(index.Entries[i].Key) >= paddedStartKey
// 	})
// 	endIndex := sort.Search(len(index.Entries), func(i int) bool {
// 		return kvs.persister.PadKey(index.Entries[i].Key) > paddedEndKey
// 	})

// 	if startIndex == len(index.Entries) {
// 		return nil, nil // startKey 大于所有索引项，返回空结果
// 	}

// 	// 2. 使用内存映射文件
// 	file, err := os.Open(index.FilePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	_, err = file.Stat()
// 	if err != nil {
// 		return nil, err
// 	}

// 	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer mmap.Unmap()
// 	// log.Printf("File size: %d", len(mmap))

// 	result := make(map[string]string)

// 	// 3. 并行处理索引区间
// 	var wg sync.WaitGroup
// 	resultChan := make(chan map[string]string, endIndex-startIndex)
// 	errorChan := make(chan error, endIndex-startIndex)
// 	// fmt.Printf("startIndex为%v，endIndex为%v\n ",startIndex,endIndex)
// 	// 优化：避免两个index同样的时候，不进行查询，但是这可能索引的间隔数量比scan范围查询的范围大造成的
// 	if startIndex == endIndex && startIndex > 0 {
// 		// 检查前一个索引项
// 		prevIndex := startIndex - 1
// 		if index.Entries[prevIndex].Key <= paddedEndKey {
// 			startIndex = prevIndex
// 		}
// 	}
// 	for i := startIndex; i < endIndex; i++ {
// 		// fmt.Println("走到这里了？？1111")
// 		wg.Add(1)
// 		go func(idx int) {
// 			defer wg.Done()
// 			localResult := make(map[string]string)

// 			startOffset := index.Entries[idx].Offset
// 			endOffset := int64(len(mmap))
// 			if idx < len(index.Entries)-1 {
// 				endOffset = index.Entries[idx+1].Offset
// 			}
// 			// fmt.Println("走到这里了？？2222")
// 			// fmt.Printf("startOffset为%v，endOffset为%v, idx为：%v\n ",startOffset,endOffset,idx)
// 			for offset := startOffset; offset < endOffset; {
// 				entry, entrySize, err := ReadEntryFromMMap(mmap[offset:])
// 				// fmt.Printf("Read entry: key=%s\n", entry.Key)
// 				if err != nil {
// 					errorChan <- err
// 					return
// 				}

// 				if entry.Key >= paddedStartKey && entry.Key <= paddedEndKey {
// 					unpadKey := kvs.persister.UnpadKey(entry.Key)
// 					localResult[unpadKey] = entry.Value
// 				} else if entry.Key > paddedEndKey {
// 					break
// 				}

// 				offset += int64(entrySize)
// 			}

// 			resultChan <- localResult
// 		}(i)
// 	}

// 	// 等待所有goroutine完成
// 	go func() {
// 		wg.Wait()
// 		close(resultChan)
// 		close(errorChan)
// 	}()

// 	// 收集结果和错误
// 	for localResult := range resultChan {
// 		for k, v := range localResult {
// 			result[k] = v
// 		}
// 	}
// 	// fmt.Printf("Total entries collected: %d\n", len(result))

// 	for err := range errorChan {
// 		if err != nil {
// 			return nil, err
// 		}
// 	}

// 	return result, nil
// }

// ReadEntryFromMMap 从内存映射中读取条目
func ReadEntryFromMMap(data []byte) (*raft.Entry, int, error) {
	var entry raft.Entry
	var entrySize int

	// 读取固定长度的字段
	if len(data) < 20 {
		return nil, 0, errors.New("insufficient data")
	}

	entry.Index = binary.LittleEndian.Uint32(data[0:4])
	entry.CurrentTerm = binary.LittleEndian.Uint32(data[4:8])
	entry.VotedFor = binary.LittleEndian.Uint32(data[8:12])
	keySize := binary.LittleEndian.Uint32(data[12:16])
	valueSize := binary.LittleEndian.Uint32(data[16:20])

	entrySize = 20 + int(keySize) + int(valueSize)

	if len(data) < entrySize {
		return nil, 0, errors.New("insufficient data")
	}

	entry.Key = string(data[20 : 20+keySize])
	entry.Value = string(data[20+keySize : entrySize])

	return &entry, entrySize, nil
}

// 普通的scan读取磁盘文件
// func (kvs *KVServer) scanFromSortedFile(startKey, endKey string) (map[string]string, error) {
// 	index := kvs.sortedFileIndex
// 	paddedStartKey := kvs.persister.PadKey(startKey)
// 	paddedEndKey := kvs.persister.PadKey(endKey)

// 	// 找到大于等于 startKey 的最小索引项，比较string大小需要给index中的key进行填充
// 	startIndex := sort.Search(len(index.Entries), func(i int) bool {
// 		return kvs.persister.PadKey(index.Entries[i].Key) >= paddedStartKey
// 	})

// 	if startIndex == len(index.Entries) {
// 		return nil, nil // startKey 大于所有索引项，返回空结果
// 	}

// 	// 打开文件并移动到起始位置
// 	file, err := os.Open(index.FilePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer file.Close()

// 	var seekOffset int64
// 	if startIndex > 0 {
// 		seekOffset = index.Entries[startIndex-1].Offset
// 	}
// 	_, err = file.Seek(seekOffset, 0)
// 	if err != nil {
// 		return nil, err
// 	}

// 	reader := bufio.NewReader(file)
// 	result := make(map[string]string)

// 	for {
// 		entry, _, err := ReadEntry(reader, 0)
// 		if err != nil {
// 			if err == io.EOF {
// 				break
// 			}
// 			return nil, err
// 		}

// 		if entry.Key >= paddedStartKey {
// 			if entry.Key > paddedEndKey {
// 				break // 已经超过了endKey，结束扫描
// 			}
// 			UnpadKey := kvs.persister.UnpadKey(entry.Key)
// 			result[UnpadKey] = entry.Value
// 		}
// 	}

// 	return result, nil
// }
// 辅助函数：获取下一个可能的键
func (kvs *KVServer) getNextPossibleKey(key string) string {
	// 这里的实现取决于你的键的格式
	// 例如，如果键是数字字符串，你可以将其转换为整数并加1
	intKey, err := strconv.Atoi(key)
	if err == nil {
		return strconv.Itoa(intKey + 1)
	}
	panic(err)
}

// 带内存映射的，使用了哈希表存储索引的
func (kvs *KVServer) scanFromSortedFile(startKey, endKey string) (map[string]string, error) {
	index := kvs.sortedFileIndex
	paddedStartKey := kvs.persister.PadKey(startKey)
	paddedEndKey := kvs.persister.PadKey(endKey)
	currentKey := startKey
	startOffset := int64(-1)

	for startKey <= endKey {
		offset, exists := index.GetOffset(currentKey)
		if exists {
			startOffset = offset
			break
		}
		// 移动到下一个可能的键
		currentKey = kvs.getNextPossibleKey(currentKey)
	}
	if startOffset == -1 {		// map中都没有要scan查询的key
		return nil, nil
	}

	// 找到大于等于 startKey 的最小索引项
	// startOffset, exists := index.GetOffset(startKey)
	// if !exists {
	//     // 如果精确的startKey不存在，找到下一个最近的键
	//     for key, offset := range index.Entries {
	//         if kvs.persister.PadKey(key) >= paddedStartKey {
	//             startOffset = offset
	//             break
	//         }
	//     }
	// }

	// 打开文件
	file, err := os.Open(index.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// 获取文件信息
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// 创建内存映射
	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer mmap.Unmap()

	result := make(map[string]string)

	// 从startOffset开始读取和处理数据
	for offset := startOffset; offset < fileSize; {
		entry, entrySize, err := ReadEntryFromMMap(mmap[offset:])
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		if entry.Key > paddedEndKey {
			break // 已经超过了endKey，结束扫描
		}

		if entry.Key >= paddedStartKey {
			unpadKey := kvs.persister.UnpadKey(entry.Key)
			result[unpadKey] = entry.Value
		}

		offset += int64(entrySize)
	}

	return result, nil
}

func (kvs *KVServer) RegisterKVServer(ctx context.Context, address string) { // 传入的是客户端与服务器之间的代理服务器的地址
	defer wg.Done()
	util.DPrintf("RegisterKVServer: %s", address) // 打印格式化后Debug信息
	for {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			util.FPrintf("failed to listen: %v", err)
		}
		grpcServer := grpc.NewServer( // 设置自定义的grpc连接
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
				MaxConnectionAgeGrace: 30 * time.Second,
			}),
		)
		kvrpc.RegisterKVServer(grpcServer, kvs)
		reflection.Register(grpcServer)

		// 在一个新的协程中启动超时检测，如果一段时间内没有put请求发过来，则终止程序，关闭服务器，以节省资源。
		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
			fmt.Println("Server stopped due to context cancellation-kvserver.")
		}()

		// 在grpcServer.Serve(lis)之后的代码默认情况下是不会执行的，因为Serve方法会阻塞当前goroutine直到服务器停止。然而，如果Serve因为某些错误而返回，后面的代码就会执行。
		if err := grpcServer.Serve(lis); err != nil {
			// 开始监听时发生了错误
			util.FPrintf("failed to serve: %v", err)
		}
		fmt.Println("跳出kvserver的for循环")
		break
	}
}

// NewValueLog creates a new Value Log.
func NewValueLog(valueLogPath string, leveldbPath string) (*ValueLog, error) {
	vLog := &ValueLog{valueLogPath: valueLogPath}
	var err error
	vLog.file, err = os.OpenFile(valueLogPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("打开valuelog文件有问题")
		return nil, err
	}
	vLog.leveldb, err = leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		fmt.Println("打开leveldb文件有问题")
		return nil, err
	}
	return vLog, nil
}

// Put stores the key-value pair in the Value Log and updates LevelDB.
func (vl *ValueLog) Put_Pure(key []byte, value []byte) error {
	// Calculate the position where the value will be written.
	position, err := vl.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	// Write <idnex, keysize, valuesize, key, value, currentTerm, votedFor, log[]> to the Value Log.
	// 固定整数的长度，即四个字节
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	// extraSize := uint32(8) // 八个字节存储currentTerm和votedFor
	// structSize := uint32(structBuf.Len())
	buf := make([]byte, 8+keySize+valueSize)
	binary.BigEndian.PutUint32(buf[0:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valueSize)
	copy(buf[8:8+keySize], key)
	copy(buf[8+keySize:], value)
	if _, err := vl.file.Write(buf); err != nil {
		return err
	}

	// Update LevelDB with <key, position>.
	// 相当于把地址（指向keysize开始处）压缩一下
	positionBytes := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(positionBytes, position)
	return vl.leveldb.Put(key, positionBytes, nil)
}

func (vl *ValueLog) Put(key []byte, value []byte) error {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	buf := make([]byte, 8+keySize+valueSize)
	binary.BigEndian.PutUint32(buf[0:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valueSize)
	copy(buf[8:8+keySize], key)
	copy(buf[8+keySize:], value)
	if _, err := vl.file.Write(buf); err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a given key from the Value Log.
func (vl *ValueLog) Get(key []byte) ([]byte, error) {
	// Retrieve the position from LevelDB.
	positionBytes, err := vl.leveldb.Get(key, nil)
	if err != nil {
		fmt.Println("get不到数据")
		return nil, err
	}
	position, _ := binary.Varint(positionBytes)

	// Seek to the position in the Value Log.
	_, err = vl.file.Seek(position, os.SEEK_SET)
	if err != nil {
		fmt.Println("get时，seek文件的位置有问题")
		return nil, err
	}

	// Read the key size and value size.
	var keySize, valueSize uint32
	sizeBuf := make([]byte, 8)
	if _, err := vl.file.Read(sizeBuf); err != nil {
		fmt.Println("get时，读取key 和 value size时有问题")
		return nil, err
	}
	keySize = binary.BigEndian.Uint32(sizeBuf[0:4])
	valueSize = binary.BigEndian.Uint32(sizeBuf[4:8])

	// Skip over the key bytes.
	// 因为上面已经读取了keysize和valuesize，所以文件的偏移量自动往后移动了8个字节
	if _, err := vl.file.Seek(int64(keySize), os.SEEK_CUR); err != nil {
		fmt.Println("get时，跳过key时有问题")
		return nil, err
	}

	// Read the value bytes.
	value := make([]byte, valueSize)
	if _, err := vl.file.Read(value); err != nil {
		fmt.Println("get是，根据value的偏移位置，拿取value值时有问题")
		return nil, err
	}

	return value, nil
}

// 返回了一个指向KVServer类型对象的指针
func MakeKVServer(address string, internalAddress string, peers []string) *KVServer {
	kvs := new(KVServer)                // 返回一个指向新分配的、零值初始化的KVServer类型的指针
	kvs.persister = new(raft.Persister) // 实例化对数据库进行读写操作的接口对象
	kvs.address = address
	kvs.internalAddress = internalAddress
	kvs.peers = peers
	// kvs.resultCh = make(chan *kvrpc.PutInRaftResponse)
	kvs.lastPutTime = time.Now()
	// Initialize ValueLog and LevelDB (Paths would be specified here).
	// 在这个.代表的是打开的工作区或文件夹的根目录，即FlexSync。指向的是VSCode左侧侧边栏（Explorer栏）中展示的最顶层文件夹。
	// valuelog, err := NewValueLog("./kvstore/kvserver/valueLog_WiscKey.log", "./kvstore/kvserver/db_key_addr")
	// if err != nil {
	// 	fmt.Println("生成valuelog和leveldb文件有问题")
	// 	panic(err)
	// }
	// 这里不直接用kvs.valuelog接受上述NewValueLog函数的返回值，是因为需要先接受该函数的返回值，检查是否有错误发生，如果没有错误，才能将其值赋值给其他值。
	// kvs.valuelog = valuelog
	return kvs
}

// 拿到当前的server在server组中的下标，也用作后续Raft中的一系列与角色有关的Id
func FindIndexInPeers(arr []string, target string) int {
	for index, value := range arr {
		if value == target {
			return index
		}
	}
	return -1 // 如果未找到，返回-1
}

func (kvs *KVServer) applyLoop() {
	for !kvs.killed() {
		select {
		case msg := <-kvs.applyCh:
			// fmt.Printf("asdasd\n")
			// 如果是安装快照
			if msg.CommandValid {
				// fmt.Printf("aasd")
				cmd := msg.Command
				index := msg.CommandIndex
				cmdTerm := msg.CommandTerm
				offset := msg.Offset
				// index = index-2
				func() {
					kvs.mu.Lock()
					defer kvs.mu.Unlock()
					// fmt.Printf("进入了fun\n")
					// 更新已经应用到的日志
					kvs.lastAppliedIndex = index
					// fmt.Println("进入到applyLoop")
					// 操作日志
					op := cmd.(*raftrpc.DetailCod) // 操作在server端的PutAppend函数中已经调用Raft的Start函数，将请求以Op的形式存入日志。

					if op.OpType == "TermLog" { // 需要进行类型断言才能访问结构体的字段，如果是leader开始第一个Term时发起的空指令，则不用执行。
						return
					}

					opCtx, existOp := kvs.reqMap[index]          // 检查当前index对应的等待put的请求是否超时，即是否还在等待被apply
					// prevSeq, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
					// _, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
					kvs.seqMap[op.ClientId] = op.SeqId           // 更新服务器端，客户端请求的序列号
					// fmt.Printf("op:%v---index%v\n",existOp,index)
					if existOp { // 存在等待结果的apply日志的RPC, 那么判断状态是否与写入时一致，可能之前接受过该日志，但是身份不是leader了，该index对应的请求日志被别的leader同步日志时覆盖了。
						// 虽然没超时，但是如果已经和刚开始写入的请求不一致了，那也不行。
						if opCtx.op.Term != int32(cmdTerm) { //这里要用msg里面的CommandTerm而不是cmd里面的Term，因为当拿去到的是空指令时，其cmd里面的Term是0，会重复发生错误
							// fmt.Printf("这里有问题吗,opCtx.op.Term:%v,op.Term:%v\n",opCtx.op.Term,op.Term)
							opCtx.wrongLeader = true
						}
					}

					// 只处理ID单调递增的客户端写请求
					if op.OpType == OP_TYPE_PUT {
						// fmt.Printf("kaishiput")
						// if !existSeq || op.SeqId > prevSeq { // 如果是客户端第一次发请求，或者发生递增的请求ID，即比上次发来请求的序号大，那么接受它的变更
						// if !existSeq {	//	如果要改就是改这个了，就不管序号，直接先执行。
							// kvs.kvStore[op.Key] = op.Value		// ----------------------------------------------
							if op.SeqId%10000 == 0 {
								fmt.Println("底层执行了Put请求，以及重置put操作时间")
							}
							kvs.lastPutTime = time.Now() // 更新put操作时间

							// 将整数编码为字节流并存入 LevelDB
							// indexKey := make([]byte, 4)                            // 假设整数是 int32 类型
							// kvs.persister.Put(op.Key,indexKey)
							// binary.BigEndian.PutUint32(indexKey, uint32(op.Index)) // 这里注意是把op.Index放进去还是对应日志的entry.Command.Index，两者应该都一样
							// kvs.persister.Put(op.Key, indexKey)                    // <key,idnex>,其中index是string类型
							// addrs := kvs.raft.GetOffsets()		// 拿到raft层的offsets，这个可以优化用通道传输
							// addr := addrs[op.Index]
							// positionBytes := make([]byte, binary.MaxVarintLen64) // 相当于把地址（指向keysize开始处）压缩一下
							// n := binary.PutVarint(positionBytes, offset)
							// 只保留实际使用的字节
							// positionBytes = positionBytes[:n]
							// fmt.Printf("此时put进去的offsetL%v\n", offset)
							// fmt.Printf("转换后的offset：%v\n", positionBytes)
							kvs.persister.Put_opt(op.Key, offset)

							// kvs.persister.Put(op.Key, []byte(op.Value))
							// fmt.Println("length:",len(positionBytes))
							// fmt.Println("length:",len([]byte(op.Value)))
						// } else if existOp { // 虽然该请求的处理还未超时，但是已经处理过了。
							// opCtx.ignored = true
						// }
					} else { // OP_TYPE_GET
						if existOp { // 如果是GET请求，只要没超时，都可以进行幂等处理
							// opCtx.value, opCtx.keyExist = kvs.kvStore[op.Key]	// --------------------------------------------
							// value := kvs.persister.Get(op.Key)		leveldb拿取value

							// 从 LevelDB 中获取键对应的值，并解码为整数
							positionBytes, err := kvs.persister.Get_opt(op.Key)
							if err != nil {
								fmt.Println("拿取value有问题")
								panic(err)
							}
							// positionBytes := kvs.persister.Get(op.Key)
							// position, _ := binary.Varint(positionBytes) // 将字节流解码为整数，拿到key对应的index
							if positionBytes == -1 { //  说明leveldb中没有该key
								opCtx.keyExist = false
								opCtx.value = raft.NoKey
							} else {
								_, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
								if err != nil {
									fmt.Println("拿取value有问题")
									panic(err)
								}
								opCtx.value = value
							}
						}
					}

					// 唤醒挂起的RPC
					if existOp { // 如果等待apply的请求还没超时
						// fmt.Printf("666")
						close(opCtx.committed)
					}
				}()
			}
		}
	}
}

func main() {
	// peers inputed by command line
	flag.Parse()
	syncTime, _ := strconv.Atoi(*syncTime_arg)
	gap, _ := strconv.Atoi(*gap_arg)
	internalAddress := *internalAddress_arg // 取出指针所指向的值，存入internalAddress变量
	address := *address_arg
	peers := strings.Split(*peers_arg, ",") // 将逗号作为分隔符传递给strings.Split函数，以便将peers_arg字符串分割成多个子字符串，并存储在peers的切片中
	kvs := MakeKVServer(address, internalAddress, peers)

	// Raft层
	kvs.applyCh = make(chan raft.ApplyMsg, 3) // 至少1个容量，启动后初始化snapshot用
	kvs.me = FindIndexInPeers(peers, internalAddress)
	// persisterRaft := &raft.Persister{} // 初始化对Raft进行持久化操作的指针
	kvs.reqMap = make(map[int]*OpContext)
	kvs.seqMap = make(map[int64]int64)
	kvs.lastAppliedIndex = 0
	InitialPersister := "/home/DYC/Gitee/FlexSync/kvstore/FlexSync/db_key_index"
	_, err := kvs.persister.Init(InitialPersister, true) // 初始化存储<key,index>的leveldb文件，true为禁用缓存。
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	kvs.startGC = false
	kvs.endGC = false                // 测试效果
	kvs.oldPersister = kvs.persister // 给old 数据库文件赋初始值

	// 初始化存储value的文件
	InitialRaftStateLog := "/home/DYC/Gitee/FlexSync/raft/RaftState.log"
	// InitialRaftStateLog, err := os.Create(currentLog)
	// if err != nil {
	// 	log.Fatalf("Failed to create new RaftState log: %v", err)
	// }
	// defer newRaftStateLog.Close()
	kvs.oldLog = InitialRaftStateLog // 给old log文件赋值

	// kvs.oldLog = "/home/DYC/Gitee/FlexSync/raft/RaftState_sorted.log"
	// kvs.currentLog = kvs.oldLog
	// _, err := kvs.oldPersister.Init(InitialPersister, true) // 初始化存储<key,index>的leveldb文件，true为禁用缓存。
	// if err != nil {
	// 	log.Fatalf("Failed to initialize database: %v", err)
	// }
	// defer persister.Close()

	go kvs.applyLoop()

	ctx, cancel := context.WithCancel(context.Background())
	// ctx, _ := context.WithCancel(context.Background())
	go kvs.RegisterKVServer(ctx, kvs.address)
	go func() {
		timeout := 250000 * time.Second
		time1 := 500000 * time.Second
		for {
			time.Sleep(timeout)
			if time.Since(kvs.lastPutTime) > timeout {
				// 检查文件是否存在并且大小是否超过4GB
				fileInfo, err := os.Stat(kvs.oldLog)
				if err != nil {
					if os.IsNotExist(err) {
						fmt.Printf("文件 %s 不存在，跳过垃圾回收\n", kvs.oldLog)
						continue
					}
					fmt.Printf("检查文件 %s 时出错: %v\n", kvs.oldLog, err)
					continue
				}

				fileSizeGB := float64(fileInfo.Size()) / (1024 * 1024 * 1024)
				if fileSizeGB <= 2 {
					fmt.Printf("文件 %s 大小为 %.2f GB，未达到垃圾回收阈值\n", kvs.oldLog, fileSizeGB)
					continue
				}

				fmt.Printf("文件 %s 大小为 %.2f GB，开始垃圾回收\n", kvs.oldLog, fileSizeGB)
				startTime := time.Now()

				err = kvs.GarbageCollection()
				if err != nil {
					fmt.Println("垃圾回收出现了错误: ", err)
				} else {
					fmt.Printf("垃圾回收完成，共花费了%v\n", time.Since(startTime))
				}

				err = kvs.CheckDatabaseContent()
				if err != nil {
					fmt.Println("检查GC后的数据库出现了错误: ", err)
				}

				err = CompareLeaderAndFollowerLogs()
				if err != nil {
					fmt.Println("检查log文件出现了错误: ", err)
				}

				fmt.Println("等五秒再停止服务器")
				time.Sleep(time1)
				cancel() // 超时后取消上下文
				fmt.Println("38秒没有请求，停止服务器")
				wg.Done()

				kvs.raft.Kill() // 关闭Raft层
				return          // 退出main函数
			}
		}
	}()
	wg.Add(1 + 1)
	kvs.raft = raft.Make(kvs.peers, kvs.me, kvs.persister, kvs.applyCh, ctx) // 开启Raft
	kvs.raft.SetCurrentLog(InitialRaftStateLog)
	kvs.raft.Gap = gap
	kvs.raft.SyncTime = syncTime

	wg.Wait()
}
