// Package kvstore is the Nezha node: a Raft-replicated key-value store that keeps values in
// the Raft log (key-value separation), with garbage collection into sorted files, crash
// recovery and the baselines selected with Config.System. cmd/nezha is the thin binary
// around New and Run.
package kvstore

import (
	"context"
	"fmt"
	"log"
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

	firstSortedFilePath string // 第一轮 GC 产物的基名，分区文件是 <基名>.p0、.p1 …
	firstPartitions     *PartitionSet
	currentLog          string          // 排序后
	oldLog              string          // 排序前
	oldPersister        *raft.Persister // 排序前
	startGC             bool            // 第一轮 GC 是否已经切换过文件
	// currentPersister *raft.Persister
	// getFromFile     func(string) (string, error)			// 对应与垃圾分离前后的两种查询方法。
	// scanFromFile    func(string, string) (map[string]string, error)

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

	// ---- crash recovery (see recovery.go) ----
	dataDir               string
	currentDBPath         string // RocksDB directory opened by kvs.persister
	oldDBPath             string // directory of kvs.oldPersister while a GC round is in flight
	gcInProgress          bool   // switch done, migration not yet finished
	sortedFilePath        string // 最近一轮完成的分区组基名；第一轮 GC 之前为空
	anotherSortedFilePath string // 归并轮产物的基名
	anotherPartitions     *PartitionSet
	// lastPartitions 是当前对外可读的那组分区，读路径与下一轮归并的输入都取自它。
	lastPartitions *PartitionSet
	// retiredPartitions 是已被取代、但引用还没归零的分区组。快照传输会钉住它读到的那一组，
	// 期间 GC 不能删它的文件。见 partition.go 的"生命周期"一节。
	retiredPartitions []*PartitionSet
	// stateMu 保护"状态机被整体换掉"这一件事，只有装快照会做。
	//
	// 读路径与 apply 路径按**读锁**持有它，装快照按**写锁**。为什么 kvs.mu 不够：
	// 一次读要同时用到 persister、currentLog 和 lastPartitions 三样，而装快照把三样
	// 一起换掉。只用 kvs.mu 护住各自的赋值，读路径仍可能取到新的 persister 配旧的
	// currentLog——那读出来是别的记录的 value，而且不报错。
	stateMu sync.RWMutex
	// gcActive 与 installing 让 GC 与装快照互斥，两者都在 kvs.mu 下读写。
	// 不用 stateMu 让它们互相等：GC 一轮要数秒到数分钟，装快照等在写锁上会把读也一起
	// 挡住（Go 的 RWMutex 在有写者等待时不再放新读者进来）。所以改成"看见对方在跑就
	// 退回、让对方稍后重试"——leader 侧本来就有重发退避。
	gcActive            bool
	installing          bool
	InitialRaftStateLog string
	lastGCFinish        bool

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
	leaderCheck      bool    // 非 leader 上的读直接让客户端改投 leader，见 requireLeader
	leaseRead        bool    // 读前要求持有 leader 租约，否则退回 ReadIndex，见 requireLeader
	verifyPartitions bool    // 装载分区时强制扫描重建索引并与旁挂索引比对，见 loadOrRebuildSparseIndex
	inlineThreshold  int     // values smaller than this (bytes) are eligible for the inline cache
	inlineCacheBytes int64   // memory budget for one partition set's shared inline cache
	gcThresholdGB    float64 // value log size in GB that triggers GC
	indexBlockBytes  int64   // sparse index granularity: one index entry per this many bytes
	// partitionTargetBytes 是 GC 产物中单个分区的目标大小，见 partition.go 的取舍说明。
	partitionTargetBytes int64
	// absorbRatio 决定何时吸收：尾部日志达到分区总量的这个比例就开一轮。
	// 用比例而不是绝对阈值，是为了让"重写多少"与"因此吸收了多少新数据"成正比——
	// 否则第 k 轮重写 k×阈值 却只吸收一个阈值，总代价 O(n²)。见 gcloop.go 的注释。
	absorbRatio float64
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
	// 没有重试循环。这里原先是 `for { ... break }`，一次都不会重转——staticcheck 的
	// SA4004 正是指它。而更要紧的是 listen 失败之后的路：util.FPrintf 的前缀写着
	// [Fatalf]，但它**只打印、不退出**，于是 lis 是 nil，接着 Serve(nil) 空指针 panic。
	// 节点绑不上自己的端口就无法服务，这里让它明确地死，而不是先打一行看起来致命的日志
	// 再炸在别处。固定端口 + pkill 重启的测试脚本撞上残留进程走的就是这条。
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("KV server cannot listen on %s: %v", address, err)
	}
	{
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
		leaderCheck:      cfg.LeaderCheck,
		leaseRead:        cfg.LeaseRead,
		verifyPartitions: cfg.VerifyPartitions,
		inlineThreshold:  cfg.InlineThreshold,
		inlineCacheBytes: int64(cfg.InlineCacheMB) << 20,
		indexBlockBytes:  int64(cfg.IndexBlockKB) << 10,
		gcThresholdGB:    cfg.GCThresholdGB,
		FirstGC:          true,
		dataDir:          cfg.DataDir,

		partitionTargetBytes: int64(cfg.PartitionTargetMB) << 20,
		absorbRatio:          cfg.AbsorbRatio,
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
	// 上次没走完的快照导出留下的 store.sst 在这里作废：传输不可续传（三家业界实现都选
	// 幂等可重来），半成品没有用处，留着只占盘。必须在恢复之前清，否则一份属于上一轮
	// GC 的导出会被下一次传输当成本轮产物。
	kvs.clearSnapshotWork()
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
	// 一个安装器，按载荷类型分派：span 归基线，snapshot 归状态机整体替换。
	// 收件目录必须与分区、日志同一个文件系统——安装靠 rename 落位，跨文件系统的
	// rename 会失败，退化成拷字节就把"原子落位"这个性质丢了。
	incoming := filepath.Join(cfg.DataDir, "data", "incoming")
	if kvs.lsm != nil {
		incoming = kvs.lsm.incomingDir
	}
	// 上次没收完的留在盘上没有用处：传输不可续传，一律丢弃重来。
	if err := os.RemoveAll(incoming); err != nil {
		return nil, fmt.Errorf("clear the incoming dir %s: %w", incoming, err)
	}
	kvs.raft.SetSSTableInstaller(incoming, kvs.installPayload)
	// 发快照的能力。限速默认 100 MiB/s，与实测的 GC 搬运速率 110 MB/s 同量级，
	// 所以它不会成为新的瓶颈；也与 TiKV 的 snap-io-max-bytes-per-sec 默认值一致。
	kvs.raft.SetSnapshotSource(kvs.snapshotForRaft, int64(cfg.SnapshotRateMB)<<20)
	kvs.raft.SetLogBudget(int64(cfg.RaftLogBudgetMB) << 20)
	if kvs.extraPersistence {
		// Dwisckey: the value is persisted once more outside the Raft log. Written, never
		// read; it only makes the cost of that persistence measurable.
		extraPath := filepath.Join(cfg.DataDir, "data", "valuelog", "dwisckey_extra.log")
		if err := kvs.raft.EnableExtraPersistence(extraPath); err != nil {
			return nil, fmt.Errorf("dwisckey extra persistence: %w", err)
		}
	}
	kvs.raft.SetSyncOnWrite(cfg.SyncWAL)
	// 攒批只在 syncWAL 打开时启用。窗口的全部意义是把一批写入摊到**一次 fsync** 上，
	// 不开 syncWAL 时落盘那一步不 fsync（见 WriteEntryToFile 里的 syncOnWrite），
	// 攒批于是只剩代价：每条最多多等一个窗口。
	// 这个条件在默认值是 0 的时候无所谓，2026-09-15 把默认改成 100us 之后就必须有——
	// 否则所有不开 syncWAL 的实验都会凭空多出延迟，而且不报错。
	if cfg.GroupCommitUs > 0 && cfg.SyncWAL {
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
	// "上一轮 GC 被打断了"必须在**启动 gcLoop 之前**取走，而且要在锁下取。
	//
	// 原先是在下面那几个 go 之后才读 kvs.gcInProgress，有两重问题：
	//  1. **数据竞态**（-race 在三节点崩溃恢复场景实测抓到一次）：gcLoop 一旦开跑一轮，
	//     AnotherSwitchToNewFiles / SwitchToNewFiles 就会在 kvs.mu 下把它置 true，
	//     而这里的读不持锁。gcInProgress 的其余七处访问全都在 kvs.mu 下，只有这里漏了。
	//  2. **读到的可能是错的那一轮**。即使不撕裂，等 gcLoop 起了一轮之后再读，读到的
	//     是**刚开始的这一轮**，而不是崩溃前那一轮——于是凭一个正在进行的 GC 去触发
	//     "重做被打断的那一轮"。恢复语义要的是 kv_state.json 里那个值，它只在构造期
	//     被写入，取它的唯一正确时机就是任何 GC 协程启动之前。
	kvs.mu.Lock()
	resumeGC := kvs.gcInProgress
	kvs.mu.Unlock()

	go kvs.applyLoop()
	go kvs.gcLoop(ctx)
	kvs.raft.StartLoops(ctx)
	if kvs.lsm != nil {
		go kvs.lsmTicker()
	}
	go kvs.RegisterKVServer(ctx, kvs.address)
	if resumeGC {
		go kvs.resumeInterruptedGC()
	}
	<-ctx.Done()
	kvs.Kill()
}
