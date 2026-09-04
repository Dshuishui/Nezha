package raft

import (
	"os"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/rpc/raftrpc"
)

// 写日志用的最小 Raft：只需要 persister（PadKey）和 logMu/日志句柄。
func newLogWriter(t *testing.T, path string, version int32) *Raft {
	t.Helper()
	rf := &Raft{persister: &Persister{}}
	rf.SetCurrentLogVersioned(path, version)
	return rf
}

func putEntry(index, term int, key, value string) *Entry {
	return &Entry{Index: uint32(index), CurrentTerm: uint32(term), Key: key, Value: value}
}

func noopEntry(index, term int) *Entry {
	return &Entry{Index: uint32(index), CurrentTerm: uint32(term), NoOp: true}
}

// 一段有代表性的日志：上任空指令、若干写入、换届空指令、再写入。
func writeSampleLog(t *testing.T, rf *Raft) {
	t.Helper()
	rf.WriteEntryToFile([]*Entry{noopEntry(1, 1)}, 0)
	for i := 2; i <= 6; i++ {
		rf.WriteEntryToFile([]*Entry{putEntry(i, 1, "k"+string(rune('0'+i)), "value-"+string(rune('0'+i)))}, 0)
	}
	rf.WriteEntryToFile([]*Entry{noopEntry(7, 2)}, 0)
	rf.WriteEntryToFile([]*Entry{putEntry(8, 2, "8", "v8"), putEntry(9, 2, "9", "v9")}, 0)
}

func TestRecoverLogRebuildsEntriesAndPendingOffsets(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	writeSampleLog(t, w)
	w.CloseLogFile()
	wantOffsets := append([]int64(nil), w.Offsets...)
	if len(wantOffsets) != 9 {
		t.Fatalf("writer recorded %d offsets, want 9", len(wantOffsets))
	}

	rf := &Raft{persister: &Persister{}}
	last, err := rf.RecoverLog([]LogFile{{Path: logPath, Version: 0}}, 3)
	if err != nil {
		t.Fatalf("RecoverLog: %v", err)
	}
	if last != 9 {
		t.Fatalf("lastIndex = %d, want 9", last)
	}
	if len(rf.log) != 9 || rf.lastIncludedIndex != 0 {
		t.Fatalf("log len %d base %d, want 9 / 0", len(rf.log), rf.lastIncludedIndex)
	}
	if rf.lastApplied != 3 || rf.commitIndex != 3 || rf.shotOffset != 3 {
		t.Fatalf("applied/commit/shot = %d/%d/%d, want 3/3/3", rf.lastApplied, rf.commitIndex, rf.shotOffset)
	}
	// 只有 index > 3 的条目回到待 apply 队列，偏移与写入时一致
	if len(rf.Offsets) != 6 {
		t.Fatalf("pending offsets = %d, want 6", len(rf.Offsets))
	}
	for i, off := range rf.Offsets {
		if off != wantOffsets[3+i] {
			t.Fatalf("Offsets[%d] = %d, want %d", i, off, wantOffsets[3+i])
		}
		if rf.offsetVersions[i] != 0 {
			t.Fatalf("offsetVersions[%d] = %d, want 0", i, rf.offsetVersions[i])
		}
	}
	// 空指令与普通条目、任期都要还原
	checks := []struct {
		pos    int
		op     string
		term   int32
		key    string
		value  string
		cmdIdx int32
	}{
		{0, "TermLog", 1, "", "", 1},
		{1, "Put", 1, "k2", "value-2", 2},
		{6, "TermLog", 2, "", "", 7},
		{8, "Put", 2, "9", "v9", 9},
	}
	for _, c := range checks {
		e := rf.log[c.pos]
		if e.Term != c.term || e.Command.OpType != c.op || e.Command.Key != c.key || e.Command.Value != c.value || e.Command.Index != c.cmdIdx {
			t.Fatalf("log[%d] = term %d %s %q=%q idx %d, want term %d %s %q=%q idx %d",
				c.pos, e.Term, e.Command.OpType, e.Command.Key, e.Command.Value, e.Command.Index,
				c.term, c.op, c.key, c.value, c.cmdIdx)
		}
	}
	// 只留在 rf.log 里的已 apply 条目，term 也要对（一致性检查会用）
	if rf.termAt(9) != 2 || rf.termAt(3) != 1 {
		t.Fatalf("termAt(9)=%d termAt(3)=%d, want 2 / 1", rf.termAt(9), rf.termAt(3))
	}
}

func TestRecoverLogTruncatesHalfWrittenTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	writeSampleLog(t, w)
	w.CloseLogFile()
	info, _ := os.Stat(logPath)
	goodSize := info.Size()

	// 模拟崩溃：最后一条只写了一半（头部完整、正文不全）
	f, _ := os.OpenFile(logPath, os.O_APPEND|os.O_WRONLY, 0666)
	partial := make([]byte, recordHeader+5)
	partial[0], partial[12], partial[16] = 10, 10, 100 // index=10, keySize=10, valueSize=100，正文只给 5 字节
	f.Write(partial)
	f.Close()

	rf := &Raft{persister: &Persister{}}
	last, err := rf.RecoverLog([]LogFile{{Path: logPath, Version: 0}}, 9)
	if err != nil {
		t.Fatalf("RecoverLog: %v", err)
	}
	if last != 9 || len(rf.log) != 9 {
		t.Fatalf("recovered %d entries (last %d), want 9", len(rf.log), last)
	}
	info, _ = os.Stat(logPath)
	if info.Size() != goodSize {
		t.Fatalf("file not truncated back: size %d, want %d", info.Size(), goodSize)
	}
	if len(rf.Offsets) != 0 {
		t.Fatalf("everything applied, but %d offsets pending", len(rf.Offsets))
	}
}

func TestRecoverLogAcrossGCFiles(t *testing.T) {
	dir := t.TempDir()
	logA := filepath.Join(dir, "RaftState.log")
	logB := filepath.Join(dir, "newRaftState_1")
	w := newLogWriter(t, logA, 0)
	w.WriteEntryToFile([]*Entry{noopEntry(1, 1)}, 0)
	for i := 2; i <= 5; i++ {
		w.WriteEntryToFile([]*Entry{putEntry(i, 1, "a", "x")}, 0)
	}
	w.SetCurrentLogVersioned(logB, 1) // GC 切换
	for i := 6; i <= 9; i++ {
		w.WriteEntryToFile([]*Entry{putEntry(i, 1, "b", "y")}, 0)
	}
	w.CloseLogFile()
	if w.pendingBaseIndex != 5 || w.pendingBaseTerm != 1 {
		t.Fatalf("pending base = (%d,%d), want (5,1)", w.pendingBaseIndex, w.pendingBaseTerm)
	}

	rf := &Raft{persister: &Persister{}}
	last, err := rf.RecoverLog([]LogFile{{Path: logA, Version: 0}, {Path: logB, Version: 1}}, 5)
	if err != nil {
		t.Fatalf("RecoverLog: %v", err)
	}
	if last != 9 || len(rf.Offsets) != 4 {
		t.Fatalf("last %d pending %d, want 9 / 4", last, len(rf.Offsets))
	}
	for i, v := range rf.offsetVersions {
		if v != 1 {
			t.Fatalf("offsetVersions[%d] = %d, want 1 (all pending entries live in the new file)", i, v)
		}
	}
	// 新文件里的偏移是相对新文件的：第一条待 apply 的条目就在新文件开头
	if rf.Offsets[0] != 0 {
		t.Fatalf("first pending offset = %d, want 0", rf.Offsets[0])
	}
}

func TestRecoverLogRejectsGap(t *testing.T) {
	dir := t.TempDir()
	logA := filepath.Join(dir, "a.log")
	logB := filepath.Join(dir, "b.log")
	w := newLogWriter(t, logA, 0)
	w.WriteEntryToFile([]*Entry{putEntry(1, 1, "a", "x"), putEntry(2, 1, "b", "y")}, 0)
	w.SetCurrentLogVersioned(logB, 1)
	w.WriteEntryToFile([]*Entry{putEntry(4, 1, "d", "z")}, 0) // 少了 3
	w.CloseLogFile()

	rf := &Raft{persister: &Persister{}}
	if _, err := rf.RecoverLog([]LogFile{{Path: logA}, {Path: logB, Version: 1}}, 0); err == nil {
		t.Fatal("expected an error for a non-contiguous log, got nil")
	}
}

func TestRecoverLogRejectsBaseMismatchWithState(t *testing.T) {
	dir := t.TempDir()
	logA := filepath.Join(dir, "a.log")
	w := newLogWriter(t, logA, 0)
	w.WriteEntryToFile([]*Entry{putEntry(6, 1, "a", "x")}, 0) // 文件从 6 开始，基址应为 5
	w.CloseLogFile()

	rf := &Raft{persister: &Persister{}, stateLoaded: true, lastIncludedIndex: 3}
	if _, err := rf.RecoverLog([]LogFile{{Path: logA}}, 5); err == nil {
		t.Fatal("expected base mismatch error, got nil")
	}
}

func TestOverwriteTruncatesStaleTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	w.WriteEntryToFile([]*Entry{putEntry(1, 1, "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}, 0)
	w.WriteEntryToFile([]*Entry{putEntry(2, 1, "b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")}, 0)
	w.WriteEntryToFile([]*Entry{putEntry(3, 1, "c", "cccccccccccccccccccccccccccccccccccccccccc")}, 0)
	// follower 冲突：从 index 2 起用一条更短的记录覆盖，3 应随之消失
	w.Offsets = w.Offsets[:1]
	w.offsetVersions = w.offsetVersions[:1]
	w.WriteEntryToFile([]*Entry{putEntry(2, 2, "b", "short")}, 0+w.Offsets[0]+int64(recordHeader)+10+36)
	w.CloseLogFile()

	rf := &Raft{persister: &Persister{}}
	last, err := rf.RecoverLog([]LogFile{{Path: logPath}}, 0)
	if err != nil {
		t.Fatalf("RecoverLog: %v", err)
	}
	if last != 2 || rf.log[1].Term != 2 || rf.log[1].Command.Value != "short" {
		t.Fatalf("after overwrite: last %d, log[1] term %d value %q; want 2 / 2 / short", last, rf.log[1].Term, rf.log[1].Command.Value)
	}
}

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

var _ = raftrpc.LogEntry{}
