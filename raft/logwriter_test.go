package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// 日志文件的写入位置此前靠每次 Seek(0, END) 现取，现在改为常驻句柄 + 自行维护
// rf.logOffset。offset 一旦算错，apply 时存进 RocksDB 的位置就指向错误的记录，
// 读出来是别的 key 的 value——而且不会报错，只会静默返回错数据。
// 这几个用例锁死 offset 的计算。

func newTestRaft(t *testing.T, dir string) *Raft {
	t.Helper()
	p := &Persister{}
	rf := &Raft{persister: p}
	rf.currentLog = filepath.Join(dir, "raft.log")
	rf.logMu.Lock()
	err := rf.openLogFile(rf.currentLog)
	rf.logMu.Unlock()
	if err != nil {
		t.Fatalf("openLogFile: %v", err)
	}
	t.Cleanup(func() { rf.CloseLogFile() })
	return rf
}

func entryOf(index uint32, key, val string) *Entry {
	return &Entry{Index: index, CurrentTerm: 1, VotedFor: 0, Key: key, Value: val}
}

// recordLen 复现写入端的编码长度：20 字节头 + padding 后的 key + value
func recordLen(rf *Raft, e *Entry) int64 {
	return int64(20 + len(rf.persister.PadKey(e.Key)) + len(e.Value))
}

func TestAppendOffsetsAreContiguous(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	entries := []*Entry{
		entryOf(1, "k1", "v1"),
		entryOf(2, "k2", "value-two"),
		entryOf(3, "k3", ""),
	}
	rf.WriteEntryToFile(entries, 0)

	if len(rf.Offsets) != 3 {
		t.Fatalf("Offsets 长度 = %d, want 3", len(rf.Offsets))
	}
	var want int64
	for i, e := range entries {
		if rf.Offsets[i] != want {
			t.Fatalf("第 %d 条 offset = %d, want %d", i, rf.Offsets[i], want)
		}
		want += recordLen(rf, e)
	}
	if rf.logOffset != want {
		t.Fatalf("logOffset = %d, want %d（下一条的写入位置）", rf.logOffset, want)
	}

	fi, err := os.Stat(rf.currentLog)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != want {
		t.Fatalf("文件大小 = %d, want %d —— 缓冲区未刷净或位置算错", fi.Size(), want)
	}
}

// 分多次追加，offset 必须跨调用连续：这正是复用句柄后最容易断掉的地方，
// 旧实现每次 Seek(0,END) 天然连续，新实现依赖 rf.logOffset 自己接上。
func TestAppendOffsetsSurviveAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	first := entryOf(1, "a", "1111")
	rf.WriteEntryToFile([]*Entry{first}, 0)
	after := recordLen(rf, first)

	second := entryOf(2, "b", "22")
	rf.WriteEntryToFile([]*Entry{second}, 0)

	if rf.Offsets[1] != after {
		t.Fatalf("第二次调用的 offset = %d, want %d", rf.Offsets[1], after)
	}
	if rf.logOffset != after+recordLen(rf, second) {
		t.Fatalf("logOffset = %d, want %d", rf.logOffset, after+recordLen(rf, second))
	}
}

// 冲突覆盖写：follower 收到与 leader 冲突的日志时会回退位置重写。
// 覆盖之后再追加，必须回到文件真实末尾，不能接着覆盖点往下写。
func TestOverwriteThenAppendResumesAtEnd(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	e1 := entryOf(1, "a", "AAAA")
	e2 := entryOf(2, "b", "BBBB")
	rf.WriteEntryToFile([]*Entry{e1, e2}, 0)
	sizeAfterTwo := recordLen(rf, e1) + recordLen(rf, e2)

	// 覆盖第二条
	rf.Offsets = rf.Offsets[:1]
	e2b := entryOf(2, "b", "CCCC")
	rf.WriteEntryToFile([]*Entry{e2b}, recordLen(rf, e1))

	if rf.logOffset != sizeAfterTwo {
		t.Fatalf("覆盖后 logOffset = %d, want %d（文件末尾）", rf.logOffset, sizeAfterTwo)
	}

	e3 := entryOf(3, "c", "DDDD")
	rf.WriteEntryToFile([]*Entry{e3}, 0)
	if got := rf.Offsets[len(rf.Offsets)-1]; got != sizeAfterTwo {
		t.Fatalf("覆盖后追加的 offset = %d, want %d", got, sizeAfterTwo)
	}
}

// 写进去的字节必须能按 offset 原样读回来——offset 对不上时这里会解出乱码
func TestRecordAtOffsetDecodes(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	entries := []*Entry{entryOf(1, "k1", "hello"), entryOf(2, "k2", "world!!")}
	rf.WriteEntryToFile(entries, 0)

	raw, err := os.ReadFile(rf.currentLog)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range entries {
		off := rf.Offsets[i]
		keySize := binary.LittleEndian.Uint32(raw[off+12 : off+16])
		valSize := binary.LittleEndian.Uint32(raw[off+16 : off+20])
		gotVal := string(raw[off+20+int64(keySize) : off+20+int64(keySize)+int64(valSize)])
		if gotVal != e.Value {
			t.Fatalf("第 %d 条按 offset %d 读出 %q, want %q", i, off, gotVal, e.Value)
		}
	}
}

// 换文件（GC 切到新日志）后，offset 必须从新文件的末尾重新起算
func TestSwitchFileResetsOffset(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	rf.WriteEntryToFile([]*Entry{entryOf(1, "a", "xxxx")}, 0)

	newLog := filepath.Join(dir, "raft2.log")
	rf.SetCurrentLog(newLog)
	if rf.logOffset != 0 {
		t.Fatalf("切到空文件后 logOffset = %d, want 0", rf.logOffset)
	}

	before := len(rf.Offsets)
	rf.WriteEntryToFile([]*Entry{entryOf(2, "b", "yy")}, 0)
	if rf.Offsets[before] != 0 {
		t.Fatalf("新文件第一条 offset = %d, want 0", rf.Offsets[before])
	}
}

// GC 在自己的 goroutine 里调 SetCurrentLog 换日志文件，而写入在另一批 goroutine 里
// 进行。句柄常驻之后这两者会争用同一个 *os.File——早先没加锁时，换文件关掉句柄
// 导致正在写的一方报 "file already closed"，节点在 GC 中途崩溃。
func TestConcurrentWriteAndLogSwitch(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() { // 持续写入
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			rf.WriteEntryToFile([]*Entry{entryOf(uint32(i+1), "k", "vvvv")}, 0)
		}
	}()

	for i := 0; i < 20; i++ { // 反复换文件
		rf.SetCurrentLog(filepath.Join(dir, fmt.Sprintf("raft-%d.log", i)))
	}
	close(stop)
	wg.Wait()
	// 走到这里没有 panic 或 Fatalf 即通过：崩溃会直接终止进程
}

func TestOverwriteTruncatesStaleTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	w.WriteEntryToFile([]*Entry{putEntry(1, 1, "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")}, 0)
	w.WriteEntryToFile([]*Entry{putEntry(2, 1, "b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")}, 0)
	w.WriteEntryToFile([]*Entry{putEntry(3, 1, "c", "cccccccccccccccccccccccccccccccccccccccccc")}, 0)
	// follower conflict: overwrite from index 2 with a shorter record; index 3 must disappear
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
