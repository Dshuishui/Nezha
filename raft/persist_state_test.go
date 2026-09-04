package raft

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHardStateRoundTrip(t *testing.T) {
	dir := t.TempDir()
	rf := &Raft{stateFile: filepath.Join(dir, "raft_state.json"), currentTerm: 7, votedFor: 2, fileBaseIndex: 15001, fileBaseTerm: 3}
	rf.persistHardState()
	hs, ok, err := loadHardState(rf.stateFile)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if hs.CurrentTerm != 7 || hs.VotedFor != 2 || hs.BaseIndex != 15001 || hs.BaseTerm != 3 {
		t.Fatalf("round trip mismatch: %+v", hs)
	}
	if _, ok, err := loadHardState(filepath.Join(dir, "missing.json")); ok || err != nil {
		t.Fatalf("missing file should be (false, nil), got ok=%v err=%v", ok, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raft_state.json.tmp-")); err == nil {
		t.Fatal("temp file left behind")
	}
}
