package kvstore

import (
	"fmt"
	"time"
)

// Config is everything a node needs to start. cmd/nezha fills it from flags; tests fill
// it directly.
type Config struct {
	Address         string   // client-facing gRPC address
	InternalAddress string   // this node's Raft address; must appear in Peers
	Peers           []string // Raft addresses of the whole cluster, index = node id
	DataDir         string   // all files live under <DataDir>/data

	Gap      int // Raft log gap (see raft.Raft.Gap)
	SyncTime int // Raft sync interval (see raft.Raft.SyncTime)

	// Durability of the Raft log: fsync after each write batch, optionally with a group
	// commit window (microseconds, 0 = off). GroupCommit only matters with SyncWAL.
	SyncWAL       bool
	GroupCommitUs int

	// System selects a preset of the switches below by the name used in the paper:
	// original, pasv, dwisckey, lsm-raft, nezha-nogc, nezha. Empty keeps the switches
	// as given.
	System       string
	KVSeparation bool // values stay in the Raft log, the store holds offsets (Nezha)
	GCEnabled    bool // rewrite the value log into sorted files once it exceeds GCThresholdGB

	// AVP (adaptive value placement) is orthogonal to System.
	InlinePlacement bool // values below InlineThreshold are stored inline in the store
	InlineThreshold int  // bytes
	InlineCacheMB   int  // memory budget of the inline cache per sorted-file index
	IndexBlockKB    int  // sparse index granularity over sorted files

	GCThresholdGB  float64
	CommitTimeoutS int    // how long a Put waits for its apply before giving up
	VizAddr        string // AVP placement visualiser listen address, "" = off

	// LSM-Raft baseline (lsmraft.go): span size and idle cut.
	SSTSpanMB int
	SSTIdleMs int
}

// systemName is the label logged at startup: the preset, or a name inferred from the
// switches so a log always says which system it measured.
func (c *Config) systemName() string {
	if c.System != "" {
		return c.System
	}
	switch {
	case !c.KVSeparation:
		return "original(inferred)"
	case c.GCEnabled:
		return "nezha(inferred)"
	default:
		return "nezha-nogc(inferred)"
	}
}

// applyPreset resolves Config.System into the individual switches. It returns the
// preset-specific hooks that New must honour: extraPersistence (dwisckey), disableWAL
// (pasv) and lsmRaft (lsm-raft).
func (c *Config) applyPreset() (extraPersistence, disableWAL, lsmRaft bool, err error) {
	switch c.System {
	case "":
	case "original":
		c.KVSeparation, c.GCEnabled = false, false
	case "pasv":
		// Original without the storage engine's own WAL: the Raft log is the only log.
		c.KVSeparation, c.GCEnabled = false, false
		disableWAL = true
	case "dwisckey":
		// Key-value separation, but the value is persisted once more outside the Raft
		// log; the read path equals nezha-nogc. No GC.
		c.KVSeparation, c.GCEnabled = true, false
		extraPersistence = true
	case "lsm-raft":
		// Original plus SSTable shipping to followers (lsmraft.go).
		c.KVSeparation, c.GCEnabled = false, false
		lsmRaft = true
	case "nezha-nogc":
		c.KVSeparation, c.GCEnabled = true, false
	case "nezha":
		c.KVSeparation, c.GCEnabled = true, true
	default:
		err = fmt.Errorf("unknown system %q: want original, pasv, dwisckey, lsm-raft, nezha-nogc or nezha", c.System)
	}
	return
}

func (c *Config) commitTimeout() time.Duration { return time.Duration(c.CommitTimeoutS) * time.Second }
