package main

import (
	"context"
	"errors"
	"io"
	"path/filepath"

	// "sort"

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

	// "gitee.com/dong-shuishui/FlexSync/kvstore/PerformanceMonitor"

	// "gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
	"gitee.com/dong-shuishui/FlexSync/pool"
	// "gitee.com/dong-shuishui/FlexSync/kvstore/GC4"
	lru "github.com/hashicorp/golang-lru"
	"github.com/linxGnu/grocksdb"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	// lru "github.com/hashicorp/golang-lru"
	"github.com/edsrzf/mmap-go"
)

var (
	internalAddress_arg = flag.String("internalAddress", "", "Input Your address") // 返回的是一个指向string类型的指针
	address_arg         = flag.String("address", "", "Input Your address")
	peers_arg           = flag.String("peers", "", "Input Your Peers")
	gap_arg             = flag.String("gap", "1000", "Input Your gap")
	syncTime_arg        = flag.String("syncTime", "", "Input Your syncTime")
	data_arg            = flag.String("data", ".", "Input Your data storage directory")
	inlineThreshold_arg = flag.Int("inlineThreshold", 512, "Value size threshold in bytes: values smaller than this are cached inline in the sorted file index")
	inlineCacheMB_arg   = flag.Int("inlineCacheMB", 256, "Memory budget in MB for the inline small-value cache (0 disables it)")
	// 默认 false，保持既有行为。开启后每批 Raft 日志写入都会 fsync，
	// 这才是共识层要求的持久化语义——也是"把两次持久化合成一次"这一收益
	// 能被测量出来的前提。
	syncWAL_arg = flag.Bool("syncWAL", false, "fsync the Raft log after each write batch (true durability)")
	// 攒批窗口，微秒。0 表示不攒批（逐条写入）。窗口越长批越大、单条延迟越高。
	// 只有开启 syncWAL 时才有意义：不 fsync 时一次写入仅 6μs，攒批无从省起。
	groupCommitUs_arg = flag.Int("groupCommitUs", 0, "group commit window in microseconds (0 = disabled)")
	// KV 分离开关。默认开启 = Nezha：value 留在 Raft 日志里，RocksDB 只存 8 字节偏移。
	// 关闭 = standard Raft + RocksDB 基线：value 随状态机写进 RocksDB，于是同一份
	// value 被持久化两次（Raft 日志一次、LSM 一次），此后还要承受 compaction 反复重写。
	// 论文把 460% 的写入优势归因于这个差异，做成开关才能量化"问题有多大"。
	// -system 按论文的系统名选配置，免去手工拼开关。
	//
	// 三个系统此前只能靠参数组合区分，其中"跑不跑 GC"还是靠把 gcThresholdGB 设成
	// 4000 这个高到永不触发的魔数来实现的。阈值一旦算错，测出来的就悄悄变成了另一
	// 个系统，而结果里看不出任何异常。
	//
	//	original    Raft 日志 + RocksDB 存完整 value，value 落盘 3 次
	//	            （Raft 日志、存储引擎 WAL、SSTable）
	//	pasv        Original 去掉存储引擎的 WAL，消除双重日志；
	//	            Raft 日志与 SSTable 的冗余仍在
	//	dwisckey    KV 分离，但 value 在 Raft 日志之外还要再落一次盘，
	//	            读路径与 nezha-nogc 相同；不做 GC
	//	lsm-raft    差异全在 follower 侧（传输 compacted SSTable 而非日志条目），
	//	            单节点下等价于 original，启动时会给出提示
	//	nezha-nogc  KV 分离，Raft 日志兼任 valuelog，value 只落盘 1 次；不跑 GC
	//	nezha       在此之上加与 Raft 日志耦合的 GC，把数据重组进排序文件
	//
	// AVP 是正交的一维，用 -inlinePlacement 叠加在上面任一系统上。
	// 留空则回落到下面几个开关各自的取值，已有脚本不受影响。
	// -vizAddr 给出监听地址才启动 AVP placement 可视化，默认不启动。
	// 热路径上因此只多一次 atomic 加法，不影响实验测量。
	vizAddr_arg = flag.String("vizAddr", "", "listen address for the AVP placement visualiser, e.g. :8080 (empty = disabled)")

	system_arg = flag.String("system", "", "system under test: original | pasv | dwisckey | lsm-raft | nezha-nogc | nezha (empty = use the individual flags below)")

	kvSeparation_arg = flag.Bool("kvSeparation", true, "keep values in the Raft log and store only offsets (false = baseline: values into RocksDB)")
	// 真正的 Adaptive Value Placement：写入时按 value 大小决定放在哪里。
	// 关闭时（默认）小值只是被额外缓存一份到内存，放置位置并未改变——
	// 那是缓存不是 placement，重启即失效、要等下次 GC 重建。
	// 开启后 value < inlineThreshold 直接存进存储引擎：读一次点查即可，
	// 与基线同路径且持久有效；GC 时这些小值也不必再搬进 sortedFile。
	inlinePlacement_arg = flag.Bool("inlinePlacement", false, "store values smaller than inlineThreshold directly in the store (true AVP)")
	// 等待 apply 回调的超时。默认 60 秒是原值。
	// 这个值直接决定吞吐指标的稳定性：一个请求撞上超时，它所在的客户端
	// goroutine 就白等这么久，而吞吐用总耗时做分母、由最慢的 goroutine 决定。
	// 实测九轮里失败 ≤23 条的全部落在 0.119 MB/S，≥24 条的全部落在 0.076，
	// 与被测模式无关——吞吐测的是"这轮撞上几次超时"，不是系统快慢。
	commitTimeoutS_arg = flag.Int("commitTimeoutS", 60, "seconds to wait for the apply callback before giving up")
	gcThresholdGB_arg  = flag.Float64("gcThresholdGB", 4000, "Value log size in GB that triggers garbage collection; lower it to exercise GC in tests")
	indexBlockKB_arg   = flag.Int("indexBlockKB", 4, "Sparse index block size in KB: one in-memory index entry per block. Larger uses less memory but scans more entries per lookup")
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
	// Sparse 是按 key 升序的稀疏块索引，每约 indexBlockBytes 一项。
	// 查找时二分定位到块，再在块内顺序扫描，内存从 O(key 数) 降为 O(块数)。
	Sparse   []SparseEntry
	FileSize int64 // sortedFile 总长度，用于界定最后一块的右边界
	// InlineValues 是小值的有界缓存，纯读加速层，命中则免去一次文件 seek。
	// 它可以为 nil、可以随时淘汰任何条目，都不影响正确性——value 始终在 sortedFile 里。
	InlineValues *InlineCache
	FilePath     string
}

// InlineCache 是按字节预算限制的小值缓存（AVP 的加速层）。
//
// 早期实现用无界 map 把 GC 时遇到的所有小值全部驻留内存，内存随数据集线性增长：
// 100GB 的 64B 小值约需 190GB 内存，普通机器无法运行。改为有界后内存变成固定预算，
// Zipf 访问下少量内存即可覆盖绝大部分请求，未命中的冷 key 退回 sortedFile 读取。
type InlineCache struct {
	mu       sync.Mutex
	lru      *lru.Cache
	curBytes int64
	maxBytes int64
	hits     uint64
	misses   uint64
}

// NewInlineCache 创建一个字节预算为 maxBytes 的缓存；maxBytes<=0 返回 nil（表示禁用）。
func NewInlineCache(maxBytes int64) *InlineCache {
	if maxBytes <= 0 {
		return nil
	}
	// 条目数上限只作兜底，真正的约束是字节预算。按每条最小开销约 128B 估算。
	countLimit := int(maxBytes / 128)
	if countLimit < 1 {
		countLimit = 1
	}
	c := &InlineCache{maxBytes: maxBytes}
	l, err := lru.NewWithEvict(countLimit, func(k, v interface{}) {
		// 由 lru 自身按条目数淘汰时同步扣减字节计数
		if b, ok := v.([]byte); ok {
			c.curBytes -= int64(len(b)) + inlineEntryOverhead
		}
	})
	if err != nil {
		return nil
	}
	c.lru = l
	return c
}

// inlineEntryOverhead 是每条缓存除 value 字节外的估算开销（key 字符串 + LRU 链表节点 + map 槽位）
const inlineEntryOverhead = 96

func (c *InlineCache) Get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.lru.Get(key); ok {
		c.hits++
		return v.([]byte), true
	}
	c.misses++
	return nil, false
}

// Add 接收 string（Entry.Value 的原生类型）；转 []byte 时 Go 自带拷贝，
// 缓存不会持有调用方缓冲区的引用。
func (c *InlineCache) Add(key string, val string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := []byte(val)
	c.lru.Add(key, cp)
	c.curBytes += int64(len(cp)) + inlineEntryOverhead
	// 超预算则淘汰最旧的，直到回到预算内（onEvict 回调负责扣减 curBytes）
	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		c.lru.RemoveOldest()
	}
}

// Stats 返回命中数、未命中数、当前字节数、条目数
func (c *InlineCache) Stats() (hits, misses uint64, bytes int64, entries int) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.curBytes, c.lru.Len()
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

	firstSortedFilePath  string // 用于存储已排序文件的位置
	firstSortedFileIndex *SortedFileIndex
	currentLog           string          // 排序后
	oldLog               string          // 排序前
	oldPersister         *raft.Persister // 排序前
	startGC              bool            // GC是否开始
	endGC                bool            // GC是否结束
	// currentPersister *raft.Persister
	// getFromFile     func(string) (string, error)			// 对应与垃圾分离前后的两种查询方法。
	// scanFromFile    func(string, string) (map[string]string, error)
	getMeasurements []time.Duration
	filePool        *FileDescriptorPool

	// multiGC
	numGC          int
	FirstGC        bool
	anotherStartGC bool
	anotherEndGC   bool
	// switchedPersister 记住已经切换上去的存储引擎实例。
	//
	// 一轮 GC 由"切换"和"搬运"两步组成，切换会把 numGC 推进一格并按新的序号建库。
	// 搬运若失败，这两个副作用已经发生：下一周期重试时又走一遍建库，路径名却基于
	// 已经增过的 numGC，于是撞上上次留下的那个库，报 lock hold by current process
	// —— GC 一旦失败，之后每次重试都必然失败。
	//
	// 记住这个实例，重试时便可跳过切换直接重做搬运。之所以不回滚，是因为切换之后
	// 落到新库的写入不能丢。
	switchedPersister *raft.Persister

	// ---- 崩溃恢复（见 docs/crash-recovery.md 与 recovery.go）----
	dataDir                string
	currentDBPath          string // kvs.persister 打开的 RocksDB 目录
	oldDBPath              string // GC 进行中时 kvs.oldPersister 的目录
	gcInProgress           bool   // 切换已生效、搬运未完成
	sortedFilePath         string // 最近一轮完成的排序文件；空表示尚未 GC
	anotherSortedFilePath  string // 用于存储已排序文件的位置
	anothersortedFileIndex *SortedFileIndex
	lastSortedFileIndex    *SortedFileIndex
	InitialRaftStateLog    string
	lastGCFinish           bool

	// AVP: adaptive value placement
	kvSeparation bool // false 时退化为 standard Raft+RocksDB 基线
	// gcEnabled 把"要不要跑 GC"从阈值大小里分离出来。
	// 原先只能靠调大 gcThresholdGB 让 GC 永不触发来模拟 Nezha-NoGC，
	// 阈值算错就会静默变成另一个被测系统。
	gcEnabled bool
	// extraPersistence：每条写入在 Raft 日志之外再落一次盘，用于 dwisckey。
	// 只写不读，读路径仍与 nezha-nogc 相同——两者的差别因此只剩那一次持久化。
	extraPersistence bool
	inlinePlacement  bool    // 写入时按大小分流放置，而非仅做读缓存
	inlineThreshold  int     // values smaller than this (bytes) are eligible for the inline cache
	inlineCacheBytes int64   // memory budget for each SortedFileIndex's inline cache
	gcThresholdGB    float64 // value log size in GB that triggers GC
	indexBlockBytes  int64   // sparse index granularity: one index entry per this many bytes
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
	if kvs.FirstGC {
		result, err := kvs.firstGCScan(in.StartKey, in.EndKey)
		if err != nil {
			reply.Err = "error in scan"
			return reply, nil
		}
		reply.KeyValuePairs = result
		return reply, nil
	}
	result, err := kvs.anotherGCScan(in.StartKey, in.EndKey)
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

// scanResult carries one branch's partial scan output together with its error.
type scanResult struct {
	data map[string]string
	err  error
}

// scanResultOf converts a StartScan_opt reply into a scanResult.
//
// Every caller used to hard-code `err: nil` and drop reply.Err on the floor, so a
// failed scan was indistinguishable from a scan that legitimately matched nothing:
// the caller merged an empty map and reported success. That is how the broken
// scanNewFile decoding stayed hidden for so long — it returned no rows and no error.
func scanResultOf(reply *kvrpc.ScanRangeResponse) scanResult {
	if reply == nil {
		return scanResult{err: errors.New("scan returned no reply")}
	}
	if reply.Err != raft.OK {
		return scanResult{err: fmt.Errorf("scan failed: %s", reply.Err)}
	}
	return scanResult{data: reply.KeyValuePairs}
}

func (kvs *KVServer) anotherGCScan(startKey, endKey string) (map[string]string, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	oldChan := make(chan scanResult, 1)
	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if !kvs.anotherStartGC {
		// GC前：并行查询上一轮新文件，上一轮排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.lastSortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			oldChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(sortedChan)
		close(oldChan)

		sortedResult := <-sortedChan
		oldResult := <-oldChan

		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if oldResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", oldResult.err)
		}

		// 合并结果，new的结果优先级高于sorted
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		for k, v := range oldResult.data {
			result[k] = v
		}
		return result, nil
	} else if kvs.anotherStartGC && !kvs.anotherEndGC {
		// GC中：并行查询上一轮新文件、上一轮排序文件和本轮new文件
		wg.Add(1) // 增加一个等待，因为要查询三个文件

		// 查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.oldPersister, kvs.oldLog)
			oldChan <- scanResultOf(result)
		}()

		// 查询已排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.lastSortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			newChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(oldChan)
		close(sortedChan)
		close(newChan)

		oldResult := <-oldChan
		sortedResult := <-sortedChan
		newResult := <-newChan

		if oldResult.err != nil {
			return nil, fmt.Errorf("error scanning old file: %v", oldResult.err)
		}
		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if newResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
		}

		// 合并结果，优先级：new > old > sorted
		result := make(map[string]string)
		// 先加入sorted的结果
		for k, v := range sortedResult.data {
			result[k] = v
		}
		// 加入old的结果，覆盖sorted的
		for k, v := range oldResult.data {
			result[k] = v
		}
		// 最后加入new的结果，覆盖之前的
		for k, v := range newResult.data {
			result[k] = v
		}
		return result, nil

	} else {
		// GC后：并行查询本轮sorted和本轮new文件
		// 查询已排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.anothersortedFileIndex)
			sortedChan <- scanResult{data: result, err: err}
		}()

		// 查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{
				StartKey: startKey,
				EndKey:   endKey,
			}, kvs.persister, kvs.currentLog)
			newChan <- scanResultOf(result)
		}()

		wg.Wait()
		close(sortedChan)
		close(newChan)

		sortedResult := <-sortedChan
		newResult := <-newChan

		if sortedResult.err != nil {
			return nil, fmt.Errorf("error scanning sorted file: %v", sortedResult.err)
		}
		if newResult.err != nil {
			return nil, fmt.Errorf("error scanning new file: %v", newResult.err)
		}

		// 合并结果，new的结果优先级高于sorted
		result := make(map[string]string)
		for k, v := range sortedResult.data {
			result[k] = v
		}
		for k, v := range newResult.data {
			result[k] = v
		}
		return result, nil
	}
}

func (kvs *KVServer) firstGCScan(startKey, endKey string) (map[string]string, error) {
	var wg sync.WaitGroup
	wg.Add(2)

	sortedChan := make(chan scanResult, 1)
	newChan := make(chan scanResult, 1)

	if kvs.startGC && !kvs.endGC {
		// 并发查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.oldPersister, kvs.oldLog)
			sortedChan <- scanResultOf(result)
		}()

		// 并发查询新文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			// if err != nil {
			//     newChan <- scanResult{data: nil, err: err}
			//     return
			// }
			newChan <- scanResultOf(result)
		}()
	}
	if kvs.startGC && kvs.endGC {
		// 并发查询排序文件
		go func() {
			defer wg.Done()
			result, err := kvs.scanFromSortedFile(startKey, endKey, kvs.firstSortedFileIndex)
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
			newChan <- scanResultOf(result)
		}()
	}
	if !kvs.startGC {
		// 只查询旧文件
		go func() {
			defer wg.Done()
			result := kvs.StartScan_opt(&kvrpc.ScanRangeRequest{StartKey: startKey, EndKey: endKey}, kvs.persister, kvs.currentLog)
			sortedChan <- scanResultOf(result)
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
	for k, v := range newResult.data {
		result[k] = v
	}
	for k, v := range sortedResult.data {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
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
	ro := grocksdb.NewDefaultReadOptions()
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

		// 存储引擎里存的是什么，取决于当前配置——不能一律当成偏移解析。
		// 三种形态由首字节的标记区分，baseline 则根本没有标记。
		value, err := kvs.decodeScanValue(iter.Value().Data(), logLocation)
		if err != nil {
			return nil, err
		}
		originalKey := kvs.persister.UnpadKey(string(key))
		result[originalKey] = value
	}

	return result, nil
}

// decodeScanValue 把存储引擎里的一条记录还原成 value。
//
// SCAN 迭代拿到的字节串有三种可能，此前这里无条件当作偏移解析，于是另外两种
// 都会读出垃圾——baseline 对照组和 AVP placement 的 SCAN 因此都跑不出正确结果。
//
//	baseline (-kvSeparation=false)  裸 value，没有标记字节
//	[TagOffset, offset8]            KV 分离，去 valuelog 取
//	[TagInline, value...]           AVP 小值内联，就地取出
//
// 注意 TagOffset 记录共 9 字节，偏移在 [1:]。原先按 [0:8] 解析，把标记字节
// 当成了偏移的最低位——算出来的是"真实偏移左移 8 位再截断"，看似合法却指向
// 文件里的任意位置。
func (kvs *KVServer) decodeScanValue(raw []byte, logLocation string) (string, error) {
	if !kvs.kvSeparation {
		return string(raw), nil
	}
	if len(raw) == 0 {
		return "", errors.New("empty record in scan")
	}
	if raw[0] == raft.TagInline {
		return string(raw[1:]), nil
	}
	off, err := raft.DecodeOffsetRecord(raw)
	if err != nil {
		return "", err
	}
	return ReadValueFromOffset(off, logLocation)
}

// ==================================================
// ReadValueFromOffset 按偏移读出 value。
// 接口收的是解码好的 int64 而不是原始字节，这样"忘记剥标记字节"这类错误
// 没法再从调用点溜进来——解码只有 raft.DecodeOffsetRecord 一个入口。
func ReadValueFromOffset(position int64, logLocation string) (string, error) {

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

func (kvs *KVServer) firstGCGet(key string, reply *kvrpc.GetInRaftResponse) *kvrpc.GetInRaftResponse {
	if !kvs.startGC { // 还未开始GC，先去旧的rocksdb查询
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			fmt.Println("去旧的rocksdb中拿取key对应的index有问题")
			panic(err)
		}
		if positionBytes == -1 {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
		read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
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

	type searchResult struct {
		found bool
		value string
		err   error
	}

	if kvs.startGC && !kvs.endGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		oldFileResult := make(chan searchResult, 1)

		// 并行搜索新文件
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索旧文件
		go func() {
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in old file")}
			}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待旧文件的结果
			result = <-oldFileResult
			if result.err != nil {
				panic("去旧的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
	}

	if kvs.startGC && kvs.endGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		sortedFileResult := make(chan searchResult, 1)

		// 并行搜索新文件
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.firstSortedFileIndex)
			if err != nil {
				sortedFileResult <- searchResult{false, "", err}
				return
			}
			sortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-sortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	return reply
}

func (kvs *KVServer) anotherGCGet(key string, reply *kvrpc.GetInRaftResponse) *kvrpc.GetInRaftResponse {
	// before-GC
	type searchResult struct {
		found bool
		value string
		err   error
	}
	if !kvs.anotherStartGC {
		// 创建用于接收结果的通道
		oldFileResult := make(chan searchResult, 1)
		lastSortedFileResult := make(chan searchResult, 1)

		// 并行搜索旧文件（上一轮的新文件），这时候还没开始第二轮GC，文件还没切换
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件，这个排序文件在第一轮GC完就已经切换，所以下面的不用改
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.lastSortedFileIndex)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果
		select {
		case result := <-oldFileResult:
			if result.err != nil {
				panic("去新的rocksdb中拿取key对应的index有问题")
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-lastSortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	// during-GC
	if !kvs.anotherEndGC {
		// 创建用于接收结果的通道
		newFileResult := make(chan searchResult, 1)
		oldFileResult := make(chan searchResult, 1)
		lastSortedFileResult := make(chan searchResult, 1)

		// 并行搜索旧文件（上一轮的新文件）
		go func() {
			positionBytes, err := kvs.oldPersister.Get_opt(key)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				oldFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.oldLog, positionBytes)
			if err != nil {
				oldFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.oldPersister.PadKey(key) {
				oldFileResult <- searchResult{true, value, nil}
			} else {
				oldFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索新文件（本轮的新文件）
		go func() {
			positionBytes, err := kvs.persister.Get_opt(key)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if positionBytes == -1 {
				newFileResult <- searchResult{false, "", nil}
				return
			}
			read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
			if err != nil {
				newFileResult <- searchResult{false, "", err}
				return
			}
			if read_key == kvs.persister.PadKey(key) {
				newFileResult <- searchResult{true, value, nil}
			} else {
				newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
			}
		}()

		// 并行搜索排序文件
		go func() {
			value, err := kvs.getFromSortedFile(key, kvs.lastSortedFileIndex)
			if err != nil {
				lastSortedFileResult <- searchResult{false, "", err}
				return
			}
			lastSortedFileResult <- searchResult{true, value, nil}
		}()

		// 首先检查新文件的结果，再旧文件，再排序文件
		select {
		case result := <-newFileResult:
			if result.err != nil {
				panic(fmt.Sprintf("去新的rocksdb中拿取key对应的index有问题: %v", result.err))
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			result = <-oldFileResult
			if result.err != nil {
				panic(fmt.Sprintf("去旧的rocksdb中拿取key对应的index有问题: %v", result.err))
			}
			if result.found {
				reply.Value = result.value
				return reply
			}
			// 如果新文件没找到，等待排序文件的结果
			result = <-lastSortedFileResult
			if result.err == nil {
				reply.Value = result.value
			} else {
				reply.Err = raft.ErrNoKey
				reply.Value = raft.NoKey
			}
			return reply
		}
	}
	// post-GC
	// 创建用于接收结果的通道
	newFileResult := make(chan searchResult, 1)
	anotherSortedFileResult := make(chan searchResult, 1)

	// 并行搜索新文件（本轮的新文件）
	go func() {
		positionBytes, err := kvs.persister.Get_opt(key)
		if err != nil {
			newFileResult <- searchResult{false, "", err}
			return
		}
		if positionBytes == -1 {
			newFileResult <- searchResult{false, "", nil}
			return
		}
		read_key, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
		if err != nil {
			newFileResult <- searchResult{false, "", err}
			return
		}
		if read_key == kvs.persister.PadKey(key) {
			newFileResult <- searchResult{true, value, nil}
		} else {
			newFileResult <- searchResult{false, "", fmt.Errorf("key mismatch in new file")}
		}
	}()

	// 并行搜索排序文件
	go func() {
		value, err := kvs.getFromSortedFile(key, kvs.anothersortedFileIndex)
		if err != nil {
			anotherSortedFileResult <- searchResult{false, "", err}
			return
		}
		anotherSortedFileResult <- searchResult{true, value, nil}
	}()

	// 首先检查新文件的结果
	select {
	case result := <-newFileResult:
		if result.err != nil {
			panic("去新的rocksdb中拿取key对应的index有问题")
		}
		if result.found {
			reply.Value = result.value
			return reply
		}
		// 如果新文件没找到，等待排序文件的结果
		result = <-anotherSortedFileResult
		if result.err == nil {
			reply.Value = result.value
		} else {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
		}
		return reply
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
	if !kvs.kvSeparation {
		// 基线：value 就在 RocksDB 里，一次点查即可，既不查偏移也不读日志文件。
		// GC 那套多路查找在这条路径上没有意义——基线没有 valuelog 需要回收。
		value, err := kvs.persister.Get(key)
		if err != nil || value == raft.ErrNoKey {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
		reply.Value = value
		return reply
	}
	if kvs.inlinePlacement {
		// 小值内联时一次点查就拿到 value，省去"查偏移 + 读日志文件"的第二次 I/O。
		// 不是内联的 key 会落回下面的多路查找，大 value 的路径完全不变。
		if v, ok := kvs.persister.GetInline(key); ok {
			reply.Value = v
			return reply
		}
	}
	if kvs.FirstGC { // 未开始第二轮GC
		reply = kvs.firstGCGet(key, reply)
		return reply
	}
	reply = kvs.anotherGCGet(key, reply)
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
		// 全部查找路径都没找到，这才是真正的"键不存在"。
		// 读的键空间大于实际写入量时，这类请求注定 miss，必须从缓存命中率里剔除，
		// 否则命中率反映的是负载怎么配的，而不是 AVP 好不好。
		avpRecordNotFound()
		// 返回客户端没有该key即可，这里先不做操作
	}
	return reply, nil
}

func (kvs *KVServer) PutInRaft(ctx context.Context, in *kvrpc.PutInRaftRequest) (*kvrpc.PutInRaftResponse, error) {
	// fmt.Println("走到了server端的put函数"
	// startTime := time.Now() // 总开始时间
	reply := kvs.StartPut(in)
	// endTime := time.Now() // 总结束时间
	// fmt.Printf("执行总时间：%v", endTime.Sub(startTime))
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
	tHandler := time.Now() // handler 全程，用于校验各阶段之和有没有漏测
	// 不再在这里记"该写进哪个文件"：这个值与实际写入时刻之间隔着一次 GC 切换，
	// 曾经就是靠它判断而错位了三条记录。现在版本号由 Raft 层在写入时与偏移一起记录
	//（ApplyMsg.FileVersion），并且这里裸读 kvs.numGC 本身也与 GC 的自增构成数据竞争。
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
	// T1开始 - Raft日志持久化阶段
	// t1Start := time.Now()
	tStart := time.Now()
	// op.Index / op.Term 由 Start 在持锁、发布进日志之前填好；这里不能再写回——
	// 返回时复制 goroutine 可能已在编码这条命令，写读同一字段就是数据竞争。
	_, _, isLeader = kvs.raft.Start(&op)
	raftStartDur := time.Since(tStart)
	// t1End := time.Now()
	// t1Duration := t1End.Sub(t1Start)
	// fmt.Printf("T1 (Raft日志持久化) duration: %v\n", t1Duration)
	if !isLeader {
		// fmt.Println("不是leader，返回")
		reply.Err = raft.ErrWrongLeader
		return reply // 如果收到客户端put请求的不是leader，需要将leader的id返回给客户端的reply中
	}
	opCtx := newOpContext(&op)
	// alreadyApplied：raft.Start 已经把这条 index 写下去了，而注册 opCtx 是之后的事。
	// 这中间 applyLoop 完全可能已经处理完这条 index——它查 reqMap 查不到，就不会
	// close(opCtx.committed)，于是下面的 select 永远等不到通知，一直挂到超时。
	// 写入本身不受影响（applyLoop 的存储分支与 existOp 无关），丢的只是那次唤醒。
	//
	// lastAppliedIndex 和这里的注册都在 kvs.mu 下，天然串行：要么注册先到、apply 时
	// 查得到 opCtx，要么 apply 先到、此处就能看见 lastAppliedIndex 已经越过 op.Index。
	var alreadyApplied bool
	func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if int(op.Index) <= kvs.lastAppliedIndex {
			alreadyApplied = true
			recordEarlyApply()
			return
		}
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kvs.reqMap[int(op.Index)] = opCtx
	}()
	if alreadyApplied {
		// 已经落盘，没有需要等待的回调
		recordPut(time.Since(tHandler), raftStartDur, 0)
		return reply
	}
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
	timer := time.NewTimer(time.Duration(*commitTimeoutS_arg) * time.Second)
	defer timer.Stop()
	tWait := time.Now()
	select {
	// 通道关闭或者有数据传入都会执行以下的分支
	case <-opCtx.committed: // ApplyLoop函数执行完后，会关闭committed通道，再根据相关的值设置请求reply的结果
		recordPut(time.Since(tHandler), raftStartDur, time.Since(tWait))
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

// getFromSortedFile 增加直接缓存value的LRU缓存功能
func (kvs *KVServer) getFromSortedFile(key string, index *SortedFileIndex) (string, error) {
	// 先检查LRU缓存
	// if value, ok := kvs.sortedFileCache.Get(key); ok {
	// 	// 缓存命中，直接返回缓存的value
	// 	return value.(string), nil
	// }
	// 增加参数检查
	if index == nil {
		return "", errors.New("invalid index: index is nil")
	}

	// 先查内联缓存，命中则免去文件 I/O
	if value, ok := index.InlineValues.Get(key); ok {
		avpRecordHit()
		return string(value), nil
	}
	avpRecordMiss()

	// 未命中：经稀疏索引二分定位到块，块内顺序扫描
	entry, err := kvs.lookupInSortedFile(index, key)
	if err != nil {
		// 这里不能记 not_found。GC 之后数据分散在多个 sortedFile 与新旧 valuelog 中，
		// 一次读会并发查这几处，"这个分片里没有"是常态而非键缺失——照此计数会把
		// 分片未命中当成键不存在（实测虚高到 37%）。真正的判定在 GetInRaft，
		// 那里是所有查找路径唯一的汇合点。
		return "", err
	}

	// 小值回填内联缓存，供后续读命中（Zipf 热点下命中率很高）
	if len(entry.Value) < kvs.inlineThreshold {
		index.InlineValues.Add(key, entry.Value)
	}

	return entry.Value, nil
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

// 带内存映射的，使用了哈希表存储索引的
func (kvs *KVServer) scanFromSortedFile(startKey, endKey string, index *SortedFileIndex) (map[string]string, error) {

	paddedStartKey := kvs.persister.PadKey(startKey)
	paddedEndKey := kvs.persister.PadKey(endKey)

	result := make(map[string]string)

	// 范围查询直接走 sortedFile 顺序读：Entries 已覆盖所有 key（含小值），
	// 且顺序读本就是范围查询的最优路径。不再遍历内联缓存——那是 O(缓存条目数)，
	// 与查询范围无关，小值场景下会让窄范围 scan 退化。
	// 用稀疏索引二分定位扫描起点。原先是从 startKey 起逐个 +1 试探直到命中，
	// 复杂度随键空间稀疏程度恶化；二分与之无关。
	startOffset, ok := index.firstBlockAtOrAfter(paddedStartKey)
	if !ok { // 索引为空，文件里没有数据
		return nil, nil
	}

	// 二分查找第一个大于等于 startkey 的索引项
	// pos := sort.Search(len(index.sortedKey), func(i int) bool {
	// 	return index.sortedKey[i].key >= paddedStartKey
	// })
	// if pos < len(index.sortedKey) {
	// 	startOffset = index.sortedKey[pos].offset
	// } else { // 没找到
	// 	return nil, nil
	// }

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
	// file, err := os.Open(index.FilePath)
	// if err != nil {
	// 	return nil, err
	// }
	// defer file.Close()
	// 由直接打开文件替换为从池中获取文件描述符
	file, err := kvs.filePool.Get()
	if err != nil {
		return nil, errors.New("获取文件描述符失败！")
	}
	defer kvs.filePool.Put(file) // 使用完毕后归还到池中

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
				// T4开始 - 实际存储操作开始
				// t4Start := time.Now()
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
						kvs.persister.SetApplied(index) // 空指令没有数据，但 applied 下标要跟上，重启才不会重放
						return
					}

					opCtx, existOp := kvs.reqMap[index] // 检查当前index对应的等待put的请求是否超时，即是否还在等待被apply
					// prevSeq, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
					// _, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
					kvs.seqMap[op.ClientId] = op.SeqId // 更新服务器端，客户端请求的序列号
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

						tRocks := time.Now()
						if kvs.inlinePlacement && len(op.Value) < kvs.inlineThreshold {
							// 小值直接落在存储引擎里，不进 valuelog：读路径因此缩短为一次点查，
							// 且 GC 无需再为它们做一次搬运。
							recordPlacement(len(op.Value), true)
							kvs.persister.PutInlineApplied(op.Key, op.Value, index)
						} else if !kvs.kvSeparation {
							// 基线：value 本身写进 RocksDB。于是同一份 value 被持久化两次
							// （Raft 日志 + LSM），而后还要被 compaction 反复搬运。
							kvs.persister.PutValueApplied(op.Key, op.Value, index)
						} else if int(msg.FileVersion) == kvs.numGC { // 对于写入日志时，又进行了 GC ，需将偏移量存新文件
							// 用 msg 带上来的版本，而不是命令自带的 op.FileVersion：
							// 后者在"决定写入"时记下，而 offset 在"实际写入"时才产生，
							// 两个时刻之间 GC 可能已经换过文件（切换走 logMu，拦不住持
							// rf.mu 的写入路径）。msg.FileVersion 与 offset 同源同锁，
							// 是唯一能保证配套的那个。
							recordPlacement(len(op.Value), false)
							kvs.persister.PutOffsetApplied(op.Key, offset, index) // 数据与 applied 下标同批
						} else { // 否则存旧文件
							kvs.oldPersister.Put_opt(op.Key, offset) //  Nezha
							// 数据在旧库、下标在当前库，两次写不原子：先数据后下标。
							// 崩在中间只会让这条在重启后重放一次，重放幂等（同 key 同偏移）。
							kvs.persister.SetApplied(index)
							// kvs.oldPersister.Put(op.Key, op.Value)		//  original
						}
						recordApplyStore(time.Since(tRocks))
						// T4结束 - 存储操作完成
						// t4End := time.Now()
						// t4Duration := t4End.Sub(t4Start)
						// fmt.Println("T4 (存储操作) 持续时间:", t4Duration)
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

var (
	logPathToCheck string
	dbPathToCheck  string
)

func main() {
	// peers inputed by command line
	flag.Parse()
	syncTime, _ := strconv.Atoi(*syncTime_arg)
	gap, _ := strconv.Atoi(*gap_arg)
	internalAddress := *internalAddress_arg // 取出指针所指向的值，存入internalAddress变量
	address := *address_arg
	peers := strings.Split(*peers_arg, ",") // 将逗号作为分隔符传递给strings.Split函数，以便将peers_arg字符串分割成多个子字符串，并存储在peers的切片中
	dataDir := *data_arg                    // 获取用户指定的数据目录

	// 如果dataDir是相对路径"."，转换为绝对路径
	if dataDir == "." {
		var err error
		dataDir, err = os.Getwd()
		if err != nil {
			log.Fatalf("Failed to get current directory: %v", err)
		}
	}

	// 确保数据目录存在
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory %s: %v", dataDir, err)
	}

	kvs := MakeKVServer(address, internalAddress, peers)

	// Raft层
	// 容量要能容下 applyLogLoop 一次取走的一批（maxApplyBatch=64），再留些余量。
	// 原先是 3：applyLogLoop 持锁往里发，通道一满就持锁阻塞，把所有 raft.Start
	// 一起堵住——发送已改到锁外，但容量太小仍会让两端频繁互等。
	kvs.applyCh = make(chan raft.ApplyMsg, 256)
	kvs.me = FindIndexInPeers(peers, internalAddress)
	// persisterRaft := &raft.Persister{} // 初始化对Raft进行持久化操作的指针
	kvs.reqMap = make(map[int]*OpContext)
	kvs.seqMap = make(map[int64]int64)
	kvs.lastAppliedIndex = 0

	// 使用用户指定的数据目录构建路径
	logPathToCheck = filepath.Join(dataDir, "data", "valuelog")
	dbPathToCheck = filepath.Join(dataDir, "data", "dbfile")

	// 检查并创建 logPathToCheck
	if err := ensurePathExists(logPathToCheck); err != nil {
		fmt.Printf("Error with log path: %v\n", err)
		return
	}

	// 检查并创建 dbPathToCheck
	if err := ensurePathExists(dbPathToCheck); err != nil {
		fmt.Printf("Error with db path: %v\n", err)
		return
	}

	// 使用用户指定的数据目录构建完整路径
	InitialPersister := filepath.Join(dataDir, "data", "dbfile", "keyIndex")
	kvs.kvSeparation = *kvSeparation_arg
	kvs.gcEnabled = true
	// -system 覆盖 kvSeparation 与 gcEnabled；-inlinePlacement 不受影响，
	// 它是正交的一维，可叠加在任一系统上。
	switch *system_arg {
	case "":
		// 未指定，沿用各开关自身的取值（含 gcThresholdGB 的原有语义）
	case "original":
		kvs.kvSeparation, kvs.gcEnabled = false, false
	case "pasv":
		kvs.kvSeparation, kvs.gcEnabled = false, false
		raft.SetDisableWAL(true)
	case "dwisckey":
		kvs.kvSeparation, kvs.gcEnabled = true, false
		kvs.extraPersistence = true
	case "lsm-raft":
		// follower 侧才有差异，单节点跑不出与 original 的区别。
		kvs.kvSeparation, kvs.gcEnabled = false, false
		if len(peers) <= 1 {
			fmt.Println("[SYSTEM] 提示：lsm-raft 的差异全在 follower 侧，" +
				"单节点下等价于 original，需多节点才有意义")
		}
	case "nezha-nogc":
		kvs.kvSeparation, kvs.gcEnabled = true, false
	case "nezha":
		kvs.kvSeparation, kvs.gcEnabled = true, true
	default:
		log.Fatalf("unknown -system %q: want original, pasv, dwisckey, lsm-raft, nezha-nogc or nezha", *system_arg)
	}
	// 必须在 persister.Init 之前解析：PASV 靠 raft.SetDisableWAL 关掉存储引擎的
	// 预写日志，而那个设置只在建库时读取一次。放在 Init 之后设置，库已经带着
	// WAL 建好了，PASV 与 Original 会写出字节数完全相同的 WAL——实测两者
	// 1920391 字节分毫不差，开关形同虚设。
	// RocksDB 的打开推迟到 recoverOrInit：恢复时要打开的是状态文件指向的那个库，不一定是初始库。
	kvs.startGC = false
	kvs.endGC = false // 测试效果
	kvs.numGC = 0
	kvs.anotherStartGC = false
	kvs.anotherEndGC = false
	kvs.FirstGC = true
	kvs.inlinePlacement = *inlinePlacement_arg
	kvs.inlineThreshold = *inlineThreshold_arg
	kvs.inlineCacheBytes = int64(*inlineCacheMB_arg) * 1024 * 1024
	// AVP 机理指标定期进日志，实验结束后从节点日志里抓最后一行
	StartAVPStatsReporter(15 * time.Second)
	StartWriteStatsReporter(15 * time.Second)
	kvs.gcThresholdGB = *gcThresholdGB_arg

	// 把生效的配置打进日志开头。三个系统的差别全在这几个开关上，而开关又可以
	// 来自 -system 或来自单独指定，日志里留下这一行，事后就能确认某份结果到底
	// 测的是哪个系统，不必去比对当时的脚本。
	systemName := *system_arg
	if systemName == "" {
		switch {
		case !kvs.kvSeparation:
			systemName = "original(inferred)"
		case kvs.gcEnabled:
			systemName = "nezha(inferred)"
		default:
			systemName = "nezha-nogc(inferred)"
		}
	}
	StartAVPViz(*vizAddr_arg, systemName, kvs.inlineThreshold)

	fmt.Printf("[SYSTEM] %s | kvSeparation=%v gcEnabled=%v gcThresholdGB=%g extraPersistence=%v syncWAL=%v | inlinePlacement=%v inlineThreshold=%dB inlineCacheMB=%d\n",
		systemName, kvs.kvSeparation, kvs.gcEnabled, kvs.gcThresholdGB,
		kvs.extraPersistence, *syncWAL_arg,
		kvs.inlinePlacement, kvs.inlineThreshold, *inlineCacheMB_arg)
	kvs.indexBlockBytes = int64(*indexBlockKB_arg) * 1024

	// 初始化存储value的文件，使用用户指定的数据目录
	kvs.InitialRaftStateLog = filepath.Join(dataDir, "data", "valuelog", "RaftState.log")
	kvs.currentLog = kvs.InitialRaftStateLog

	InitGCPaths(dataDir)
	InitAnotherGCPaths(dataDir)
	kvs.dataDir = dataDir
	// 全新节点在这里打开初始库并写下初始状态；重启节点则接回 GC 状态、排序文件索引与
	// applied 下标，并拿到 Raft 需要扫描的日志文件列表。此时任何 loop 都还没启动。
	recoveredFiles, recoveredApplied := kvs.recoverOrInit(InitialPersister)
	kvs.lastAppliedIndex = recoveredApplied

	go kvs.applyLoop()
	// 服务端随进程存活，没有任何地方会取消它；之前 WithCancel 丢掉 cancel 与此等价，
	// 只是让 go vet 报"context 泄漏"。
	ctx := context.Background()
	// 对客户端开放（RegisterKVServer）放到 Raft 恢复并启动之后，见函数末尾。
	go func() {
		// defer kvs.filePool.Close() // 程序退出时关闭池中的所有文件描述符
		timeout := 5 * time.Second
		// time1 := 500000 * time.Second
		for {
			time.Sleep(timeout)
			// if time.Since(kvs.lastPutTime) > timeout {
			// 检查文件是否存在并且大小是否超过4GB
			fileInfo, err := os.Stat(kvs.currentLog)
			if err != nil {
				if os.IsNotExist(err) {
					// fmt.Printf("文件 %s 不存在，跳过垃圾回收\n", kvs.currentLog)
					continue
				}
				fmt.Printf("检查文件 %s 时出错: %v\n", kvs.currentLog, err)
				continue
			}

			if !kvs.gcEnabled {
				// Nezha-NoGC：只做 KV 分离，不回收 valuelog。
				continue
			}
			if !kvs.kvSeparation {
				// 基线（standard Raft+RocksDB）没有 valuelog，也就没有垃圾要回收。
				// 让它走 GC 会当场出错：RocksDB 里存的是裸 value，GC 却按偏移记录
				// 解析（unknown record tag: 0x76 —— 那是 value 的首字符）。
				// 更要命的是 GC 在搬运之前就把 persister 换成了新的空库，
				// 于是失败之后所有 GET 都返回 NOKEY。
				continue
			}

			fileSizeGB := float64(fileInfo.Size()) / (1024 * 1024 * 1024)
			if fileSizeGB <= kvs.gcThresholdGB {
				// fmt.Printf("文件 %s 大小为 %.2f GB，未达到垃圾回收阈值\n", kvs.currentLog, fileSizeGB)
				continue
			}
			if kvs.numGC >= 2 {
				// fmt.Printf("已经进行了 %d 轮垃圾回收，停止进一步的垃圾回收\n", kvs.numGC)
				continue
			}
			if kvs.gcInProgress {
				continue // 上一轮（含重启后重做的那一轮）还没收尾，不能再起一轮
			}
			// 第一轮GC
			if kvs.FirstGC {
				fmt.Printf("文件 %s 大小为 %.2f GB，开始垃圾回收\n", kvs.currentLog, fileSizeGB)
				startTime := time.Now()
				err = kvs.FirstGarbageCollection()
				if err != nil {
					// 失败就停在这里：状态不推进、旧文件不删。此前的做法是照样推进 numGC
					// 并且 os.Remove(kvs.oldLog)——可数据还没完整搬进排序文件，删掉源文件
					// 就是永久丢数据。下一轮 5 秒检查会重试。
					fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
					continue
				}
				if kvs.firstSortedFileIndex == nil {
					fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
					continue
				}
				kvs.finishFirstGC(startTime)
			} else if kvs.lastGCFinish {
				if kvs.lastSortedFileIndex == nil {
					fmt.Println("缺少上一轮排序文件索引，跳过本轮迭代 GC")
					continue
				}
				kvs.lastGCFinish = false // make sure last gc process is finished
				startTime := time.Now()
				err = kvs.AnotherGarbageCollection()
				if err != nil {
					fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
					kvs.lastGCFinish = true
					continue
				}
				if kvs.anothersortedFileIndex == nil {
					fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
					kvs.lastGCFinish = true
					continue
				}
				kvs.finishAnotherGC(startTime)
			}

			// fmt.Println("等五秒再停止服务器")
			// time.Sleep(time1)
			// cancel() // 超时后取消上下文
			// fmt.Println("38秒没有请求，停止服务器")
			// wg.Done()

			// kvs.raft.Kill() // 关闭Raft层
			// return          // 退出main函数
			// }

		}
	}()
	// 在服务器代码中临时添加
	fmt.Printf("MaxSendMsgSize: %d bytes (%d GB)\n", pool.MaxSendMsgSize, pool.MaxSendMsgSize>>30)
	fmt.Printf("MaxRecvMsgSize: %d bytes (%d GB)\n", pool.MaxRecvMsgSize, pool.MaxRecvMsgSize>>30)

	// monitor, _ := performancemonitor.NewPerformanceMonitor("performance_metrics.csv", 100)
	// monitor.Start()
	// defer monitor.Stop()

	wg.Add(1 + 1)
	raftStateFile := filepath.Join(dataDir, "data", "raft_state.json")
	kvs.raft = raft.Make(kvs.peers, kvs.me, kvs.persister, kvs.applyCh, raftStateFile) // 只构造，不启动
	if kvs.extraPersistence {
		// Dwisckey：value 在 Raft 日志之外再落一次盘。文件与 valuelog 同目录，
		// 只写不读，纯粹为了把那一次持久化的代价计入测量。
		extraPath := filepath.Join(dataDir, "data", "valuelog", "dwisckey_extra.log")
		if err := kvs.raft.EnableExtraPersistence(extraPath); err != nil {
			log.Fatalf("启用 dwisckey 的第二份持久化失败：%v", err)
		}
	}
	// 必须在 raft.Make 之后：此前放在 flag 解析处会对 nil 指针调用而 panic。
	kvs.raft.SetSyncOnWrite(*syncWAL_arg)
	if *groupCommitUs_arg > 0 {
		kvs.raft.EnableGroupCommit(time.Duration(*groupCommitUs_arg) * time.Microsecond)
	}
	if len(recoveredFiles) > 0 {
		if _, err := kvs.raft.RecoverLog(recoveredFiles, recoveredApplied); err != nil {
			log.Fatalf("[RECOVER] 重建 Raft 日志失败：%v", err)
		}
	}
	// 当前日志文件带版本号挂接（全新节点是 RaftState.log / 版本 0），写入位置接在文件末尾。
	kvs.raft.SetCurrentLogVersioned(kvs.currentLog, int32(kvs.numGC))
	kvs.raft.Gap = gap
	kvs.raft.SyncTime = syncTime
	kvs.raft.StartLoops(ctx)
	go kvs.RegisterKVServer(ctx, kvs.address)
	if kvs.gcInProgress {
		go kvs.resumeInterruptedGC()
	}

	wg.Wait()
}
