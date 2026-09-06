// Package kvstore is the Nezha node: a Raft-replicated key-value store that keeps values in
// the Raft log (key-value separation), with garbage collection into sorted files, crash
// recovery and the baselines selected with Config.System. cmd/nezha is the thin binary
// around New and Run.
package kvstore

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

const (
	OP_TYPE_PUT = "Put"
	OP_TYPE_GET = "Get"
)

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
	commitTimeout    time.Duration
	cfg              Config
	lsm              *lsmRaft // LSM-Raft baseline state; nil unless -system lsm-raft
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
	filePool *FileDescriptorPool

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

	// ---- crash recovery (see docs/crash-recovery.md and recovery.go) ----
	dataDir                string
	currentDBPath          string // RocksDB directory opened by kvs.persister
	oldDBPath              string // directory of kvs.oldPersister while a GC round is in flight
	gcInProgress           bool   // switch done, migration not yet finished
	sortedFilePath         string // latest completed sorted file; empty until the first GC
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

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kvs *KVServer) RegisterKVServer(ctx context.Context, address string) { // 传入的是客户端与服务器之间的代理服务器的地址
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
		util.DPrintf("KV gRPC server stopped")
		break
	}
}

// 拿到当前的server在server组中的下标，也用作后续Raft中的一系列与角色有关的Id
func indexInPeers(arr []string, target string) int {
	for index, value := range arr {
		if value == target {
			return index
		}
	}
	return -1 // 如果未找到，返回-1
}

// New builds a node from cfg: resolves the system preset, prepares the data directory,
// opens (or recovers) the store and constructs Raft. Nothing runs until Run.
func New(cfg Config) (*KVServer, error) {
	extraPersistence, disableWAL, lsmRaft, err := cfg.applyPreset()
	if err != nil {
		return nil, err
	}
	if cfg.DataDir == "" || cfg.DataDir == "." {
		if cfg.DataDir, err = os.Getwd(); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("data directory %s: %w", cfg.DataDir, err)
	}
	me := indexInPeers(cfg.Peers, cfg.InternalAddress)
	if me < 0 {
		return nil, fmt.Errorf("internal address %s is not among the peers %v", cfg.InternalAddress, cfg.Peers)
	}

	kvs := &KVServer{
		persister:       new(raft.Persister),
		address:         cfg.Address,
		internalAddress: cfg.InternalAddress,
		peers:           cfg.Peers,
		me:              me,
		lastPutTime:     time.Now(),
		// Room for one applyLogLoop batch (maxApplyBatch=64) and then some: a full
		// channel used to block Raft's apply loop while it held its lock.
		applyCh:          make(chan raft.ApplyMsg, 256),
		reqMap:           make(map[int]*OpContext),
		seqMap:           make(map[int64]int64),
		cfg:              cfg,
		commitTimeout:    cfg.commitTimeout(),
		kvSeparation:     cfg.KVSeparation,
		gcEnabled:        cfg.GCEnabled,
		extraPersistence: extraPersistence,
		inlinePlacement:  cfg.InlinePlacement,
		inlineThreshold:  cfg.InlineThreshold,
		inlineCacheBytes: int64(cfg.InlineCacheMB) << 20,
		indexBlockBytes:  int64(cfg.IndexBlockKB) << 10,
		gcThresholdGB:    cfg.GCThresholdGB,
		FirstGC:          true,
		dataDir:          cfg.DataDir,
	}
	// PASV switches off the storage engine's WAL. RocksDB reads that option once, when
	// the store is opened, so it must be set before recoverOrInit.
	raft.SetDisableWAL(disableWAL)
	if lsmRaft {
		kvs.lsm = newLSMRaft(cfg.DataDir, int64(cfg.SSTSpanMB)<<20, time.Duration(cfg.SSTIdleMs)*time.Millisecond, &kvs.mu)
		if len(cfg.Peers) <= 1 {
			fmt.Println("[SYSTEM] lsm-raft differs from original only on followers; a single node behaves like original")
		}
	}

	for _, dir := range []string{filepath.Join(cfg.DataDir, "data", "valuelog"), filepath.Join(cfg.DataDir, "data", "dbfile")} {
		if err := ensurePathExists(dir); err != nil {
			return nil, err
		}
	}
	kvs.InitialRaftStateLog = filepath.Join(cfg.DataDir, "data", "valuelog", "RaftState.log")
	kvs.currentLog = kvs.InitialRaftStateLog
	InitGCPaths(cfg.DataDir)
	InitAnotherGCPaths(cfg.DataDir)

	// Mechanism metrics go to the log periodically; experiments read the last line.
	StartAVPStatsReporter(15 * time.Second)
	StartWriteStatsReporter(15 * time.Second)
	StartAVPViz(cfg.VizAddr, cfg.systemName(), kvs.inlineThreshold)
	// One line that says which system this log measured: the switches can come from the
	// preset or be set individually, and results must be attributable afterwards.
	fmt.Printf("[SYSTEM] %s | kvSeparation=%v gcEnabled=%v gcThresholdGB=%g extraPersistence=%v syncWAL=%v | inlinePlacement=%v inlineThreshold=%dB inlineCacheMB=%d\n",
		cfg.systemName(), kvs.kvSeparation, kvs.gcEnabled, kvs.gcThresholdGB,
		kvs.extraPersistence, cfg.SyncWAL,
		kvs.inlinePlacement, kvs.inlineThreshold, cfg.InlineCacheMB)

	// A fresh node opens the initial store; a restarted node restores GC state, the
	// sorted-file index and the applied index, and returns the log files Raft must replay.
	initialStore := filepath.Join(cfg.DataDir, "data", "dbfile", "keyIndex")
	recoveredFiles, recoveredApplied := kvs.recoverOrInit(initialStore)
	kvs.lastAppliedIndex = recoveredApplied

	raftStateFile := filepath.Join(cfg.DataDir, "data", "raft_state.json")
	kvs.raft = raft.Make(kvs.peers, kvs.me, kvs.persister, kvs.applyCh, raftStateFile)
	if kvs.lsm != nil {
		kvs.raft.SetSSTableInstaller(kvs.lsm.incomingDir, kvs.lsmInstall)
	}
	if kvs.extraPersistence {
		// Dwisckey: the value is persisted once more outside the Raft log. Written, never
		// read; it only makes the cost of that persistence measurable.
		extraPath := filepath.Join(cfg.DataDir, "data", "valuelog", "dwisckey_extra.log")
		if err := kvs.raft.EnableExtraPersistence(extraPath); err != nil {
			return nil, fmt.Errorf("dwisckey extra persistence: %w", err)
		}
	}
	kvs.raft.SetSyncOnWrite(cfg.SyncWAL)
	if cfg.GroupCommitUs > 0 {
		kvs.raft.EnableGroupCommit(time.Duration(cfg.GroupCommitUs) * time.Microsecond)
	}
	if len(recoveredFiles) > 0 {
		if _, err := kvs.raft.RecoverLog(recoveredFiles, recoveredApplied); err != nil {
			return nil, fmt.Errorf("rebuild Raft log: %w", err)
		}
	}
	// Attach the current log file with its version; appends continue at its end.
	kvs.raft.SetCurrentLogVersioned(kvs.currentLog, int32(kvs.numGC))
	kvs.raft.Gap = cfg.Gap
	kvs.raft.SyncTime = cfg.SyncTime
	return kvs, nil
}

// Run starts the apply loop, the GC trigger, Raft and the client-facing server, then
// blocks until ctx is cancelled. The gRPC servers stop gracefully on cancellation.
func (kvs *KVServer) Run(ctx context.Context) {
	go kvs.applyLoop()
	go kvs.gcLoop(ctx)
	kvs.raft.StartLoops(ctx)
	if kvs.lsm != nil {
		go kvs.lsmTicker()
	}
	go kvs.RegisterKVServer(ctx, kvs.address)
	if kvs.gcInProgress {
		go kvs.resumeInterruptedGC()
	}
	<-ctx.Done()
	kvs.Kill()
}
