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
	// 默认 100us：实测的最优窗口——批大小在那里饱和，p50 与最小值无实质差别，
	// 而 p99 明确更好（100us 的三轮 4.078/4.024/4.006 全部低于 50us 的每一轮）；
	// 再大只是让每条白等（node55 上 5000us 的 p50 是 200us 的 6.3 倍、吞吐是它的 1/5.6）。
	// 数据与取舍见 results/groupcommit/2026-09-15-window-low-winbox/meta.txt。
	//
	// 注意它**只在 -syncWAL 打开时才生效**（见 server.go 的接线）。窗口的意义是把一批
	// 写入摊到一次 fsync 上；不开 syncWAL 时根本不 fsync，攒批就只剩"每条多等 100us"。
	// 默认是 0 的时候这条路走不到，改成 100 就必须把启用条件挂到 syncWAL 上，
	// 否则所有不开 syncWAL 的运行会凭空多出延迟。
	flag.IntVar(&cfg.GroupCommitUs, "groupCommitUs", 100, "group commit window in microseconds (0 = disabled); takes effect only with -syncWAL; 100 is the measured optimum (see results/groupcommit/)")
	flag.IntVar(&cfg.SnapshotRateMB, "snapshotRateMB", 100, "rate limit for shipping a snapshot to a lagging replica, MiB/s (0 = unlimited)")
	flag.IntVar(&cfg.RaftLogBudgetMB, "raftLogBudgetMB", 256, "byte budget for the in-memory Raft log, MiB; past it a lagging replica is truncated past and repaired by snapshot")
	// -system selects the configuration by the name used in the paper (see
	// kvstore.Config.System); the individual switches below apply when it is empty.
	flag.StringVar(&cfg.System, "system", "", "system under test: original | pasv | dwisckey | lsm-raft | nezha-nogc | nezha (empty = use the individual flags)")
	flag.BoolVar(&cfg.KVSeparation, "kvSeparation", true, "keep values in the Raft log and store only offsets (false = baseline: values into RocksDB)")
	// GCEnabled 此前**没有** flag，只能由 -system 经 applyPreset 设定，而 -system 的帮助
	// 文本写着 "empty = use the individual flags"——那句话对 GC 是假的：不存在这个开关。
	// 后果是 `-gcThresholdGB 0.005` 被接受、被打印进 [SYSTEM] 行、然后被完全忽略，
	// GC 一轮都不跑。2026-09-17 实测：valuelog 涨到 18.8MB（阈值 5.24MB），
	// gcEnabled=false，inline-cache-e2e 的实验组因此"什么也没验证到"，
	// 另有两个脚本（memory-scale、avp-compare）也在数 GC 轮数却永远数到 0。
	// 默认取 false 是为了不改变任何现有调用的含义——真正要 GC 的写 -system nezha 或 -gc。
	flag.BoolVar(&cfg.GCEnabled, "gc", false, "rewrite the value log into sorted files past -gcThresholdGB; only needed when -system is empty (a preset sets it)")
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

	// **静默失效变响。** 显式给了 -gcThresholdGB 却没有任何东西打开 GC，几乎一定是
	// 期望 GC 会跑：一次这样的运行会跑满全程、产出看起来正常的数据，而 GC 路径一次
	// 都没走过。2026-09-17 就是这样丢了一整轮 inline-cache-e2e。
	// 只在 -system 为空时拦：显式选了 nezha-nogc / original 这类预设的人是故意不要 GC 的。
	if cfg.System == "" && !cfg.GCEnabled {
		thresholdSet := false
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "gcThresholdGB" {
				thresholdSet = true
			}
		})
		if thresholdSet {
			log.Fatalf("nezha: -gcThresholdGB=%g 给了，但 GC 是关的（-system 为空且未给 -gc），"+
				"阈值会被完全忽略、GC 一轮都不会跑。要跑 GC 请加 -system nezha 或 -gc；"+
				"确实不要 GC 请写 -system nezha-nogc，别只靠把阈值调大。", cfg.GCThresholdGB)
		}
	}

	node, err := kvstore.New(cfg)
	if err != nil {
		log.Fatalf("nezha: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	node.Run(ctx)
}
