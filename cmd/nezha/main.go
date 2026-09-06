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
	flag.IntVar(&cfg.GroupCommitUs, "groupCommitUs", 0, "group commit window in microseconds (0 = disabled); only meaningful with -syncWAL")
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
	// A request that hits this timeout stalls its client goroutine for the whole period,
	// and throughput is decided by the slowest goroutine, so the value shapes the
	// stability of throughput numbers more than the speed of the system does.
	flag.IntVar(&cfg.CommitTimeoutS, "commitTimeoutS", 60, "seconds to wait for the apply callback before giving up")
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
