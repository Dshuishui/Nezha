package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func catalog(spans ...[2]int) *lsmRaft {
	l := &lsmRaft{catalogMax: 16}
	for _, s := range spans {
		l.catalog = append(l.catalog, raft.SSTableSpan{Start: s[0], End: s[1], Files: []string{"x"}})
	}
	return l
}

// TestPickSpan covers the leader's choice for every position a follower can report.
func TestPickSpan(t *testing.T) {
	l := catalog([2]int{101, 200}, [2]int{201, 300}, [2]int{301, 400})
	cases := []struct {
		next      int
		wantStart int
		ok        bool
	}{
		{0, 101, true},   // unknown position: start with the oldest span
		{50, 101, true},  // behind everything we keep: oldest, follower replays up to 100
		{101, 101, true}, // exactly at a boundary
		{150, 101, true}, // inside a span: that span (follower ingests, its data is newer)
		{201, 201, true},
		{400, 301, true},
		{401, 0, false}, // caught up
		{900, 0, false},
	}
	for _, c := range cases {
		s, ok := l.pickSpan(c.next)
		if ok != c.ok || (ok && s.Start != c.wantStart) {
			t.Errorf("pickSpan(%d) = (%d, %v), want (%d, %v)", c.next, s.Start, ok, c.wantStart, c.ok)
		}
		if ok && s.OldestAvailable != 101 {
			t.Errorf("pickSpan(%d): OldestAvailable = %d, want 101", c.next, s.OldestAvailable)
		}
	}
	if _, ok := catalog().pickSpan(0); ok {
		t.Error("empty catalog must return nothing")
	}
}

// newLSMTestServer builds the smallest KVServer that can apply baseline PUTs and ingest
// spans: a real store, no Raft (nothing in these paths reaches it), spans never cut.
func newLSMTestServer(t *testing.T) *KVServer {
	t.Helper()
	kvs := &KVServer{persister: new(raft.Persister), reqMap: map[int]*OpContext{}, seqMap: map[int64]int64{}}
	if _, err := kvs.persister.Init(filepath.Join(t.TempDir(), "db"), true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kvs.persister.Close)
	kvs.lsm = newLSMRaft(t.TempDir(), 1<<40, time.Hour, &kvs.mu)
	return kvs
}

func putMsg(index int, key, value string) raft.ApplyMsg {
	return raft.ApplyMsg{CommandValid: true, CommandIndex: index, CommandTerm: 1,
		Command: &raftrpc.DetailCod{Index: int32(index), Term: 1, OpType: OP_TYPE_PUT, Key: key, Value: value, SeqId: int64(index)}}
}

func spanForTest(t *testing.T, kvs *KVServer, start, end int, kv map[string]string) raft.SSTableSpan {
	t.Helper()
	rows := map[string][]byte{}
	for k, v := range kv {
		rows[kvs.persister.PadKey(k)] = []byte(v)
	}
	dir := filepath.Join(kvs.lsm.incomingDir, fmt.Sprintf("%d-%d", start, end))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, "span.sst")
	if err := kvs.persister.WriteSpanSST(file, rows, end); err != nil {
		t.Fatal(err)
	}
	return raft.SSTableSpan{Start: start, End: end, Files: []string{file}, OldestAvailable: start}
}

// TestInstallSkipAndGap: the follower rule a-1 <= lastApplied < b in its two refusals.
func TestInstallSkipAndGap(t *testing.T) {
	kvs := newLSMTestServer(t)
	kvs.lastAppliedIndex = 30
	if applied, st := kvs.lsmInstall(spanForTest(t, kvs, 21, 30, map[string]string{"k": "v"})); st != raftrpc.InstallSSTableStatus_SKIPPED || applied != 30 {
		t.Fatalf("passed span: got (%d, %s), want (30, SKIPPED)", applied, st)
	}
	kvs.lastAppliedIndex = 10
	far := spanForTest(t, kvs, 21, 30, map[string]string{"k": "v"})
	far.OldestAvailable = 11 // the leader still has older spans: no fallback yet
	if applied, st := kvs.lsmInstall(far); st != raftrpc.InstallSSTableStatus_GAP || applied != 10 {
		t.Fatalf("span ahead: got (%d, %s), want (10, GAP)", applied, st)
	}
	if _, err := kvs.persister.Get("k"); err == nil {
		t.Fatal("a refused span must not touch the store")
	}
}

// TestInstallGapFallbackReplaysHeld: when the leader has nothing older, the follower
// replays its held entries up to the span and then ingests it.
func TestInstallGapFallbackReplaysHeld(t *testing.T) {
	kvs := newLSMTestServer(t)
	kvs.lastAppliedIndex = 10
	for i := 11; i <= 16; i++ {
		kvs.lsm.held = append(kvs.lsm.held, putMsg(i, fmt.Sprintf("k%d", i), fmt.Sprintf("held%d", i)))
	}
	span := spanForTest(t, kvs, 14, 20, map[string]string{"k14": "span14", "k20": "span20"})
	applied, st := kvs.lsmInstall(span)
	if st != raftrpc.InstallSSTableStatus_INGESTED || applied != 20 {
		t.Fatalf("got (%d, %s), want (20, INGESTED)", applied, st)
	}
	for k, want := range map[string]string{"k11": "held11", "k13": "held13", "k14": "span14", "k20": "span20"} {
		if v, err := kvs.persister.Get(k); err != nil || v != want {
			t.Errorf("%s = %q err=%v, want %q", k, v, err, want)
		}
	}
	if _, err := kvs.persister.Get("k15"); err == nil {
		t.Error("k15 lies inside the span but was not in the span file: it must come from the span, not from replay")
	}
	if got, _, _ := kvs.persister.GetApplied(); got != 20 || kvs.lastAppliedIndex != 20 {
		t.Errorf("applied = %d / %d, want 20", got, kvs.lastAppliedIndex)
	}
	if len(kvs.lsm.held) != 0 || kvs.lsm.replayUntil != 13 {
		t.Errorf("held=%d replayUntil=%d, want 0 and 13", len(kvs.lsm.held), kvs.lsm.replayUntil)
	}
}

// TestReplayHeldPartial: only entries up to the requested index are replayed.
func TestReplayHeldPartial(t *testing.T) {
	kvs := newLSMTestServer(t)
	for i := 1; i <= 5; i++ {
		kvs.lsm.held = append(kvs.lsm.held, putMsg(i, fmt.Sprintf("k%d", i), "v"))
	}
	kvs.lsmReplayHeld(3)
	if kvs.lastAppliedIndex != 3 || len(kvs.lsm.held) != 2 || kvs.lsm.held[0].CommandIndex != 4 {
		t.Fatalf("lastApplied=%d held=%d first=%d", kvs.lastAppliedIndex, len(kvs.lsm.held), kvs.lsm.held[0].CommandIndex)
	}
	if _, err := kvs.persister.Get("k4"); err == nil {
		t.Fatal("k4 must still be held")
	}
}
