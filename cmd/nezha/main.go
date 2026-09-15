// Command nezha runs one node of the Nezha key-value store. All behaviour lives in
// internal/kvstore; this file only turns flags into a kvstore.Config and runs the node
// until SIGINT or SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gitee.com/dong-shuishui/FlexSync/internal/kvstore"
)

func main() {
	var cfg kvstore.Config
	var peers string
	flag.StringVar(&cfg.Address, "address", "", "client-facing gRPC address, e.g. 192.168.1.240:3099")
	flag.StringVar(&cfg.InternalAddress, "internalAddress", "", "this node's Raft address; must be one of -peers")
	flag.StringVar(&peers, "peers", "", "comma-separated Raft addresses of all nodes; the order defines node ids")
	flag.StringVar(&cfg.DataDir, "data", ".", "data directory; all files go under <data>/data")
	flag.IntVar(&cfg.Gap, "gap", 1000, "Raft log gap")
	flag.IntVar(&cfg.SyncTime, "syncTime", 0, "Raft sync interval")
	// Off by default to keep the historical behaviour. On, every Raft log batch is fsynced,
	// which is the durability consensus requires and the precondition for measuring the
	// gain of merging two persistence steps into one.
	flag.BoolVar(&cfg.SyncWAL, "syncWAL", false, "fsync the Raft log after each write batch (true durability)")
	// 默认仍是 0（关闭）：改默认值会改变此后每一次实验的含义，而主表还没跑完。
	// 实测的最优窗口约 100us——批大小在那里饱和，p50 与最小值无实质差别，p99 明确更好；
	// 再大只是让每条多等（node55 上 5000us 的 p50 是 200us 的 6.3 倍、吞吐是它的 1/5.6）。
	// 数据与取舍见 results/groupcommit/2026-09-15-window-low-winbox/meta.txt。
	flag.IntVar(&cfg.GroupCommitUs, "groupCommitUs", 0, "group commit window in microseconds (0 = disabled); only meaningful with -syncWAL; measured optimum is about 100 (see results/groupcommit/)")
	flag.IntVar(&cfg.SnapshotRateMB, "snapshotRateMB", 100, "rate limit for shipping a snapshot to a lagging replica, MiB/s (0 = unlimited)")
	flag.IntVar(&cfg.RaftLogBudgetMB, "raftLogBudgetMB", 256, "byte budget for the in-memory Raft log, MiB; past it a lagging replica is truncated past and repaired by snapshot")
	// -system selects the configuration by the name used in the paper (see
	// kvstore.Config.System); the individual switches below apply when it is empty.
	flag.StringVar(&cfg.System, "system", "", "system under test: original | pasv | dwisckey | lsm-raft | nezha-nogc | nezha (empty = use the individual flags)")
	flag.BoolVar(&cfg.KVSeparation, "kvSeparation", true, "keep values in the Raft log and store only offsets (false = baseline: values into RocksDB)")
	// AVP proper: values are placed by size at write time. Off, small values are only
	// cached in memory (lost on restart, rebuilt by the next GC); on, values below the
	// threshold go straight into the store and GC never moves them.
	flag.BoolVar(&cfg.InlinePlacement, "inlinePlacement", false, "store values smaller than inlineThreshold directly in the store (true AVP)")
	flag.IntVar(&cfg.InlineThreshold, "inlineThreshold", 512, "value size threshold in bytes for inline placement and the inline cache")
	flag.IntVar(&cfg.InlineCacheMB, "inlineCacheMB", 256, "memory budget in MB for the inline small-value cache (0 disables it)")
	flag.IntVar(&cfg.IndexBlockKB, "indexBlockKB", 4, "sparse index block size in KB: one in-memory index entry per block")
	flag.Float64Var(&cfg.GCThresholdGB, "gcThresholdGB", 4000, "value log size in GB that triggers garbage collection; lower it to exercise GC in tests")
	flag.IntVar(&cfg.PartitionTargetMB, "partitionTargetMB", 128, "target size in MB of one GC output partition; lower it to exercise multi-partition reads in tests")
	flag.Float64Var(&cfg.AbsorbRatio, "absorbRatio", 0.25, "start a GC round once the tail log reaches this fraction of the partitions' total size")
	// A request that hits this timeout stalls its client goroutine for the whole period,
	// and throughput is decided by the slowest goroutine, so the value shapes the
	// stability of throughput numbers more than the speed of the system does.
	flag.IntVar(&cfg.CommitTimeoutS, "commitTimeoutS", 60, "seconds to wait for the apply callback before giving up")
	// On by default: a read served by a follower silently returns whatever that node has
	// applied so far, which is not an error the caller can detect. Single-node runs are
	// unaffected, the node holds the leader role. Turn it off to inspect a follower's own
	// state, which the verification tools do on purpose.
	flag.BoolVar(&cfg.LeaderCheck, "leaderCheck", true, "reject reads on a non-leader and redirect the client")
	// Lease read closes the gap that -leaderCheck alone leaves open: a deposed leader keeps
	// the role until it learns otherwise, and answers stale reads meanwhile. With the lease
	// held a read still costs no RPC; only a cold lease falls back to a ReadIndex round.
	flag.BoolVar(&cfg.LeaseRead, "leaseRead", true, "serve reads only while the leader lease is held, else fall back to ReadIndex (needs -leaderCheck)")
	// 装载分区时扫描重建索引并与落盘的旁挂索引比对。默认关：那是正比于数据量的启动耗时
	// （实测约 110MB/s，100GB 要 15 分钟）。排查索引可疑时打开。
	flag.BoolVar(&cfg.VerifyPartitions, "verifyPartitions", false, "on load, rebuild each partition's sparse index by scanning and check the persisted one against it")
	flag.StringVar(&cfg.VizAddr, "vizAddr", "", "listen address for the AVP placement visualiser, e.g. :8080 (empty = disabled)")
	flag.IntVar(&cfg.SSTSpanMB, "sstSpanMB", 32, "lsm-raft: applied value bytes per shipped SSTable span")
	flag.IntVar(&cfg.SSTIdleMs, "sstIdleMs", 1000, "lsm-raft: cut the open span after this many ms without writes")
	flag.Parse()
	cfg.Peers = strings.Split(peers, ",")
	if cfg.Address == "" || cfg.InternalAddress == "" || peers == "" {
		fmt.Fprintln(os.Stderr, "nezha: -address, -internalAddress and -peers are required")
		flag.Usage()
		os.Exit(2)
	}

	node, err := kvstore.New(cfg)
	if err != nil {
		log.Fatalf("nezha: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	node.Run(ctx)
}
