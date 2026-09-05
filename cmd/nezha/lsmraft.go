package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

// LSM-Raft baseline (-system lsm-raft).
//
// LSM-Raft (Zhang et al., PACMMOD 2025) lets a Raft leader replicate compacted SSTables
// instead of raw entries to followers whose LSM-tree would otherwise redo the leader's
// work. No implementation is public, so this is the smallest faithful equivalent on our
// stack:
//
//   - The Raft log is untouched; every entry is replicated and persisted as before.
//   - The leader applies entries to RocksDB exactly like Original and, in addition, keeps
//     the rows of the open span in memory. Every -sstSpanMB of applied values (or after
//     -sstIdleMs without writes) it writes them as one SSTable with RocksDB's
//     SstFileWriter, labelled with the span of log indexes it covers, and ships it to each
//     follower (InstallSSTable, see internal/raft/sstable.go). RocksDB cannot ingest its
//     own flush output, so the span file is built rather than reused; the leader pays one
//     extra sequential write per span for it.
//   - A follower never replays entries into its own RocksDB while a leader is shipping.
//     It holds committed entries in memory and ingests the leader's files instead, so the
//     follower's LSM-tree sees neither WAL, memtable nor flush for that data. This is the
//     "always ship SSTables" end of LSM-Raft's cost-aware policy; the leader keeps its
//     full redundant writes, as the paper states.
//
// Ordering rule that keeps a follower's store equal to replaying the log: a span
// [a, b] is ingested only when a-1 <= lastApplied < b, so ingestion is strictly in log
// order and each ingested file receives a newer sequence number than everything before
// it. A span the follower has passed is skipped; a span too far ahead is refused (GAP)
// and the leader falls back to an older one. When the leader has nothing older
// (OldestAvailable == a), the follower replays its held entries up to a-1 itself. Held
// entries are also replayed when the node becomes leader, or when they exceed
// heldLimit as a liveness guard against a leader that never ships.

type lsmRaft struct {
	spanLimit int64         // bytes of applied values per span
	idle      time.Duration // cut a span after this long without writes
	heldLimit int64         // replay held entries beyond this many bytes

	spansDir    string // leader: shipped span files, one directory per span
	incomingDir string // follower: spans being received

	// leader state
	spanOpen   bool              // an entry has been applied since the last cut
	spanStart  int               // first index of the open span
	spanBytes  int64             // applied value bytes in the open span
	rows       map[string][]byte // store key -> value as written, last version per key
	lastApply  time.Time
	catalog    []raft.SSTableSpan
	catalogMax int
	senders    map[int]bool // peer -> a sender goroutine is running for it
	cond       *sync.Cond

	// follower state
	held        []raft.ApplyMsg
	heldBytes   int64
	replayUntil int // entries at or below this index are applied directly (gap fallback)
}

func newLSMRaft(dataDir string, spanBytes int64, idle time.Duration, mu *sync.Mutex) *lsmRaft {
	l := &lsmRaft{
		spanLimit:   spanBytes,
		idle:        idle,
		heldLimit:   8 * spanBytes,
		spansDir:    filepath.Join(dataDir, "data", "sst_spans"),
		incomingDir: filepath.Join(dataDir, "data", "sst_incoming"),
		catalogMax:  16,
		rows:        map[string][]byte{},
		senders:     map[int]bool{},
		cond:        sync.NewCond(mu),
	}
	// Spans are rebuilt from scratch by a new leader; nothing on disk is reusable.
	os.RemoveAll(l.spansDir)
	os.RemoveAll(l.incomingDir)
	os.MkdirAll(l.spansDir, 0o755)
	os.MkdirAll(l.incomingDir, 0o755)
	return l
}

// lsmHoldOrApply is the follower-side gate in the apply loop. It returns true when the
// message was consumed here (held) and must not be applied. Caller holds kvs.mu.
func (kvs *KVServer) lsmHoldOrApply(msg raft.ApplyMsg) bool {
	l := kvs.lsm
	if kvs.raft.IsLeader() {
		if len(l.held) > 0 {
			util.DPrintf("[LSM-Raft] leader: replaying %d held entries", len(l.held))
			kvs.lsmReplayHeld(int(^uint(0) >> 1))
		}
		return false
	}
	kvs.lsmResetLeaderState()
	if msg.CommandIndex <= kvs.lastAppliedIndex {
		return true // already covered by an ingested span; Raft delivered it late
	}
	if msg.CommandIndex <= l.replayUntil {
		return false
	}
	l.held = append(l.held, msg)
	if op, ok := msg.Command.(*raftrpc.DetailCod); ok {
		l.heldBytes += int64(len(op.Value))
	}
	if l.heldBytes > l.heldLimit {
		util.EPrintf("[LSM-Raft] follower: %d MB held without a span from the leader; replaying locally",
			l.heldBytes>>20)
		kvs.lsmReplayHeld(msg.CommandIndex)
	}
	return true
}

// lsmReplayHeld applies held entries with index <= until through the normal path and
// keeps the rest. Caller holds kvs.mu.
func (kvs *KVServer) lsmReplayHeld(until int) {
	l := kvs.lsm
	n := 0
	for n < len(l.held) && l.held[n].CommandIndex <= until {
		n++
	}
	for _, m := range l.held[:n] {
		if op, ok := m.Command.(*raftrpc.DetailCod); ok {
			l.heldBytes -= int64(len(op.Value))
		}
		if m.CommandIndex <= kvs.lastAppliedIndex {
			continue // an ingested span already covers it; replaying would resurrect an older value
		}
		kvs.applyCommand(m)
	}
	l.held = append(l.held[:0], l.held[n:]...)
}

// lsmResetLeaderState forgets the open span and the catalog once the node is no longer
// leader. Both describe this node's store at its own term; after another leader has
// shipped spans in between, re-labelling or re-offering them would send followers older
// versions than they already hold. A re-elected leader starts from an empty catalog and
// followers fall back to replaying the gap. Caller holds kvs.mu.
func (kvs *KVServer) lsmResetLeaderState() {
	l := kvs.lsm
	if !l.spanOpen && len(l.catalog) == 0 {
		return
	}
	l.spanOpen, l.spanBytes, l.rows = false, 0, map[string][]byte{}
	for _, s := range l.catalog {
		os.RemoveAll(filepath.Dir(s.Files[0]))
	}
	l.catalog = nil
	util.DPrintf("[LSM-Raft] no longer leader: open span and catalog dropped")
}

// lsmDropHeld discards held entries covered by an ingested span. Caller holds kvs.mu.
func (kvs *KVServer) lsmDropHeld(through int) {
	l := kvs.lsm
	n := 0
	for n < len(l.held) && l.held[n].CommandIndex <= through {
		if op, ok := l.held[n].Command.(*raftrpc.DetailCod); ok {
			l.heldBytes -= int64(len(op.Value))
		}
		n++
	}
	l.held = append(l.held[:0], l.held[n:]...)
}

// lsmAfterApply records one applied entry on the leader (storeKey == "" for a no-op)
// and cuts the span when it is full. Caller holds kvs.mu.
func (kvs *KVServer) lsmAfterApply(index int, storeKey string, value []byte) {
	l := kvs.lsm
	if !l.spanOpen {
		l.spanOpen, l.spanStart = true, index
	}
	if storeKey != "" {
		l.rows[storeKey] = value
		l.spanBytes += int64(len(value))
	}
	l.lastApply = time.Now()
	if l.spanBytes >= l.spanLimit {
		kvs.lsmCutSpan()
	}
}

// lsmCutSpan closes the open span [spanStart, lastAppliedIndex]: write its rows as one
// SSTable into the catalog and wake the senders. Caller holds kvs.mu.
func (kvs *KVServer) lsmCutSpan() {
	l := kvs.lsm
	if !l.spanOpen {
		return
	}
	start, end := l.spanStart, kvs.lastAppliedIndex
	rows := l.rows
	l.spanOpen, l.spanBytes, l.rows = false, 0, map[string][]byte{}
	if len(rows) == 0 {
		return // no-ops only: nothing to ship, the next span's marker covers them
	}
	t0 := time.Now()
	dir := filepath.Join(l.spansDir, fmt.Sprintf("%d-%d", start, end))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		util.EPrintf("[LSM-Raft] span dir: %v", err)
		return
	}
	file := filepath.Join(dir, "span.sst")
	if err := kvs.persister.WriteSpanSST(file, rows, end); err != nil {
		util.EPrintf("[LSM-Raft] span [%d,%d]: %v; followers will replay it", start, end, err)
		os.RemoveAll(dir)
		return
	}
	l.catalog = append(l.catalog, raft.SSTableSpan{Start: start, End: end, Files: []string{file}})
	for len(l.catalog) > l.catalogMax {
		os.RemoveAll(filepath.Dir(l.catalog[0].Files[0]))
		l.catalog = l.catalog[1:]
	}
	util.DPrintf("[LSM-Raft] span [%d,%d] cut: %d rows in %v", start, end, len(rows), time.Since(t0))
	for p := range kvs.peers {
		if p != kvs.me && !l.senders[p] {
			l.senders[p] = true
			go kvs.lsmShipLoop(p)
		}
	}
	l.cond.Broadcast()
}

// lsmShipLoop sends spans to one follower in order until this node stops being leader.
// A peer that cannot be reached is retried with a backoff of up to 10 s; the failure is
// logged on the first attempt and then every tenth one.
func (kvs *KVServer) lsmShipLoop(peer int) {
	l := kvs.lsm
	next := 0 // index the follower is believed to need next; 0 = unknown
	failures := 0
	retry := func(err error, span raft.SSTableSpan) {
		if failures%10 == 0 {
			util.EPrintf("[LSM-Raft] ship [%d,%d] to %s failed %d time(s): %v", span.Start, span.End, kvs.peers[peer], failures+1, err)
		}
		failures++
		time.Sleep(time.Duration(1<<min(failures-1, 3)) * time.Second) // 1, 2, 4, 8, 8, ... s
	}
	for !kvs.killed() {
		kvs.mu.Lock()
		for {
			if !kvs.raft.IsLeader() {
				l.senders[peer] = false
				kvs.mu.Unlock()
				return
			}
			if s, ok := l.pickSpan(next); ok {
				span := s
				kvs.mu.Unlock()
				resp, err := kvs.raft.SendSSTable(peer, span)
				if err != nil {
					retry(err, span)
					break
				}
				switch resp.Status {
				case raftrpc.InstallSSTableStatus_INGESTED, raftrpc.InstallSSTableStatus_SKIPPED:
					failures = 0
					next = int(resp.Applied) + 1
				case raftrpc.InstallSSTableStatus_GAP:
					failures = 0
					next = int(resp.Applied) + 1
					time.Sleep(200 * time.Millisecond) // the follower may still be receiving entries
				case raftrpc.InstallSSTableStatus_STALE_TERM:
					util.DPrintf("[LSM-Raft] ship to %s: stale term, stopping", kvs.peers[peer])
					kvs.mu.Lock()
					l.senders[peer] = false
					kvs.mu.Unlock()
					return
				default:
					retry(fmt.Errorf("follower answered %s", resp.Status), span)
				}
				break
			}
			l.cond.Wait()
		}
	}
}

// pickSpan chooses what to send to a follower that needs index next. Caller holds kvs.mu.
func (l *lsmRaft) pickSpan(next int) (raft.SSTableSpan, bool) {
	if len(l.catalog) == 0 {
		return raft.SSTableSpan{}, false
	}
	oldest := l.catalog[0]
	if next == 0 || next < oldest.Start {
		// Unknown position, or behind everything we still have: offer the oldest span.
		// The follower either skips it (it is further along than we thought) or, told
		// that nothing older exists, replays up to Start-1 itself.
		s := oldest
		s.OldestAvailable = oldest.Start
		return s, true
	}
	for _, s := range l.catalog {
		if next >= s.Start && next <= s.End {
			s.OldestAvailable = oldest.Start
			return s, true
		}
	}
	return raft.SSTableSpan{}, false // caught up
}

// lsmInstall is the follower's SSTableInstaller.
func (kvs *KVServer) lsmInstall(span raft.SSTableSpan) (int, raftrpc.InstallSSTableStatus) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	l := kvs.lsm
	la := kvs.lastAppliedIndex
	if la >= span.End {
		return la, raftrpc.InstallSSTableStatus_SKIPPED
	}
	if la < span.Start-1 {
		if span.OldestAvailable != span.Start {
			return la, raftrpc.InstallSSTableStatus_GAP
		}
		// Nothing older will come: replay what we hold up to the span, then continue
		// replaying anything Raft still delivers below it.
		l.replayUntil = span.Start - 1
		kvs.lsmReplayHeld(span.Start - 1)
		la = kvs.lastAppliedIndex
		if la < span.Start-1 {
			return la, raftrpc.InstallSSTableStatus_GAP
		}
	}
	t0 := time.Now()
	if err := kvs.persister.IngestSSTables(span.Files); err != nil {
		util.EPrintf("[LSM-Raft] ingest span [%d,%d]: %v", span.Start, span.End, err)
		return la, raftrpc.InstallSSTableStatus_FAILED
	}
	if got, ok, err := kvs.persister.GetApplied(); err != nil || !ok || got != span.End {
		// The file carries the leader's applied index; anything else means the span
		// label and the file disagree. Correct the marker so recovery stays consistent.
		util.EPrintf("[LSM-Raft] span [%d,%d]: applied index in file = %d (ok=%v err=%v)", span.Start, span.End, got, ok, err)
		kvs.persister.SetApplied(span.End)
	}
	kvs.lastAppliedIndex = span.End
	kvs.lsmDropHeld(span.End)
	os.RemoveAll(filepath.Dir(span.Files[0])) // RocksDB consumed the files; drop the span directory
	util.DPrintf("[LSM-Raft] ingested span [%d,%d] (%d files) in %v; held=%d", span.Start, span.End, len(span.Files), time.Since(t0), len(l.held))
	return span.End, raftrpc.InstallSSTableStatus_INGESTED
}

// lsmTicker cuts idle spans on the leader and keeps the senders from sleeping forever.
func (kvs *KVServer) lsmTicker() {
	l := kvs.lsm
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for !kvs.killed() {
		<-tick.C
		kvs.mu.Lock()
		if l.spanOpen && l.spanBytes > 0 && kvs.raft.IsLeader() && time.Since(l.lastApply) >= l.idle {
			kvs.lsmCutSpan()
		}
		l.cond.Broadcast()
		kvs.mu.Unlock()
	}
}
