package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// hardState is the part of Raft state that must be durable before any RPC is answered,
// plus the base of the on-disk log. The log itself is the value log, so it is not
// duplicated here.
//
//	CurrentTerm / VotedFor  standard Raft persistent state; written on elections and
//	                        whenever a higher term is observed
//	BaseIndex / BaseTerm    index/term of the entry just before the oldest log file still
//	                        on disk. Advanced when GC deletes an old log. On restart
//	                        lastIncludedIndex/Term start here and are cross-checked
//	                        against the first record actually found in the files.
type hardState struct {
	CurrentTerm int   `json:"current_term"`
	VotedFor    int   `json:"voted_for"`
	BaseIndex   int   `json:"base_index"`
	BaseTerm    int32 `json:"base_term"`
}

// WriteFileAtomic writes to a temp file, fsyncs it, renames it over path, then fsyncs the
// directory. A crash at any step leaves the previous file intact.
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { tmp.Close(); os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// persistHardState writes term, vote and log base to disk. The caller holds rf.mu.
// This is one small fsync per term or vote change, on the election path only; the
// write path never calls it.
func (rf *Raft) persistHardState() {
	if rf.stateFile == "" {
		return // persistence disabled (unit tests, or callers that never set a path)
	}
	hs := hardState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		BaseIndex:   rf.fileBaseIndex,
		BaseTerm:    rf.fileBaseTerm,
	}
	data, err := json.Marshal(hs)
	if err != nil {
		panic(fmt.Sprintf("marshal raft hard state: %v", err))
	}
	if err := WriteFileAtomic(rf.stateFile, data); err != nil {
		// Continuing as leader or voter with unpersisted state would break Raft's
		// safety argument; stopping is the only correct option.
		panic(fmt.Sprintf("persist raft hard state to %s: %v", rf.stateFile, err))
	}
}

// loadHardState reads the state file. A missing file means a fresh node and returns
// (zero, false, nil).
func loadHardState(path string) (hardState, bool, error) {
	var hs hardState
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return hs, false, nil
	}
	if err != nil {
		return hs, false, err
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return hs, false, fmt.Errorf("parse %s: %v", path, err)
	}
	return hs, true, nil
}
