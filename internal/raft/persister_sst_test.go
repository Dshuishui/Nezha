package raft

import (
	"fmt"
	"path/filepath"
	"testing"
)

func openStore(t *testing.T, name string) *Persister {
	t.Helper()
	p := new(Persister)
	if _, err := p.Init(filepath.Join(t.TempDir(), name), true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

// spanFile writes the rows of one span the way the leader does (store keys padded).
func spanFile(t *testing.T, p *Persister, name string, applied int, kv map[string]string) string {
	t.Helper()
	rows := map[string][]byte{}
	for k, v := range kv {
		rows[p.PadKey(k)] = []byte(v)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := p.WriteSpanSST(path, rows, applied); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestSpanRoundTrip: a span written on the leader and ingested on a follower leaves the
// same rows and the same applied index as replaying the entries would.
func TestSpanRoundTrip(t *testing.T) {
	leader, follower := openStore(t, "leader"), openStore(t, "follower")
	kv := map[string]string{}
	for i := 1; i <= 100; i++ {
		k, v := fmt.Sprintf("k%03d", i), fmt.Sprintf("v%d", i)
		leader.PutValueApplied(k, v, i)
		kv[k] = v
	}
	if err := follower.IngestSSTables([]string{spanFile(t, leader, "1-100.sst", 100, kv)}); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := follower.GetApplied(); err != nil || !ok || got != 100 {
		t.Fatalf("applied after ingest = %d ok=%v err=%v, want 100", got, ok, err)
	}
	for i := 1; i <= 100; i += 7 {
		k := fmt.Sprintf("k%03d", i)
		lv, _ := leader.Get(k)
		fv, err := follower.Get(k)
		if err != nil || fv != lv || fv != kv[k] {
			t.Fatalf("%s: follower=%q leader=%q err=%v", k, fv, lv, err)
		}
	}
	if _, err := follower.Get("missing"); err == nil {
		t.Fatal("a key never written must stay absent after ingestion")
	}
}

// TestSpanOrderLastWins: spans ingested in log order make the later span's value of a
// key visible, and the applied index follows the last span.
func TestSpanOrderLastWins(t *testing.T) {
	leader, follower := openStore(t, "leader"), openStore(t, "follower")
	s1 := spanFile(t, leader, "1-1.sst", 1, map[string]string{"k": "old", "only-in-1": "x"})
	s2 := spanFile(t, leader, "2-2.sst", 2, map[string]string{"k": "new"})
	for _, f := range []string{s1, s2} {
		if err := follower.IngestSSTables([]string{f}); err != nil {
			t.Fatal(err)
		}
	}
	if v, err := follower.Get("k"); err != nil || v != "new" {
		t.Fatalf("k = %q err=%v, want new", v, err)
	}
	if v, err := follower.Get("only-in-1"); err != nil || v != "x" {
		t.Fatalf("only-in-1 = %q err=%v, want x", v, err)
	}
	if got, _, _ := follower.GetApplied(); got != 2 {
		t.Fatalf("applied = %d, want 2", got)
	}
}

// TestSpanOverReplayedRows: a follower that replayed part of a span itself (gap fallback)
// and then ingests the whole span ends up with the span's values, not older ones.
func TestSpanOverReplayedRows(t *testing.T) {
	leader, follower := openStore(t, "leader"), openStore(t, "follower")
	follower.PutValueApplied("a", "replayed-a", 5)
	follower.PutValueApplied("b", "stale-b", 6) // entry 7 in the span overwrites b
	span := spanFile(t, leader, "5-7.sst", 7, map[string]string{"a": "replayed-a", "b": "final-b"})
	if err := follower.IngestSSTables([]string{span}); err != nil {
		t.Fatal(err)
	}
	if v, _ := follower.Get("b"); v != "final-b" {
		t.Fatalf("b = %q, want final-b", v)
	}
	if got, _, _ := follower.GetApplied(); got != 7 {
		t.Fatalf("applied = %d, want 7", got)
	}
}

// TestSpanMetaKeySortsFirst: the marker key starts with 0x00 and must not break the
// ascending-order requirement of SstFileWriter for any user key.
func TestSpanMetaKeySortsFirst(t *testing.T) {
	leader := openStore(t, "leader")
	spanFile(t, leader, "meta.sst", 3, map[string]string{"": "empty-key", "\x00zz": "nul-prefixed", "0": "digit"})
}
