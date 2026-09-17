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

// recordLen 复现写入端的编码长度：20 字节头 + key + value。
// key 原样写入（存储层不再补齐），所以这里用它本身的长度。
func recordLen(rf *Raft, e *Entry) int64 {
	return int64(20 + len(e.Key) + len(e.Value))
}

func TestAppendOffsetsAreContiguous(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	entries := []*Entry{
		entryOf(1, "k1", "v1"),
		entryOf(2, "k2", "value-two"),
		entryOf(3, "k3", ""),
	}
	rf.AppendToLogFile(entries)

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
	rf.AppendToLogFile([]*Entry{first})
	after := recordLen(rf, first)

	second := entryOf(2, "b", "22")
	rf.AppendToLogFile([]*Entry{second})

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
	rf.AppendToLogFile([]*Entry{e1, e2})
	sizeAfterTwo := recordLen(rf, e1) + recordLen(rf, e2)

	// 覆盖第二条
	rf.Offsets = rf.Offsets[:1]
	e2b := entryOf(2, "b", "CCCC")
	rf.OverwriteLogFileFrom([]*Entry{e2b}, recordLen(rf, e1))

	if rf.logOffset != sizeAfterTwo {
		t.Fatalf("覆盖后 logOffset = %d, want %d（文件末尾）", rf.logOffset, sizeAfterTwo)
	}

	e3 := entryOf(3, "c", "DDDD")
	rf.AppendToLogFile([]*Entry{e3})
	if got := rf.Offsets[len(rf.Offsets)-1]; got != sizeAfterTwo {
		t.Fatalf("覆盖后追加的 offset = %d, want %d", got, sizeAfterTwo)
	}
}

// 写进去的字节必须能按 offset 原样读回来——offset 对不上时这里会解出乱码
func TestRecordAtOffsetDecodes(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	entries := []*Entry{entryOf(1, "k1", "hello"), entryOf(2, "k2", "world!!")}
	rf.AppendToLogFile(entries)

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

	rf.AppendToLogFile([]*Entry{entryOf(1, "a", "xxxx")})

	newLog := filepath.Join(dir, "raft2.log")
	rf.SetCurrentLog(newLog)
	if rf.logOffset != 0 {
		t.Fatalf("切到空文件后 logOffset = %d, want 0", rf.logOffset)
	}

	before := len(rf.Offsets)
	rf.AppendToLogFile([]*Entry{entryOf(2, "b", "yy")})
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
			rf.AppendToLogFile([]*Entry{entryOf(uint32(i+1), "k", "vvvv")})
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
	w.AppendToLogFile([]*Entry{putEntry(1, 1, "a", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")})
	w.AppendToLogFile([]*Entry{putEntry(2, 1, "b", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")})
	w.AppendToLogFile([]*Entry{putEntry(3, 1, "c", "cccccccccccccccccccccccccccccccccccccccccc")})
	// follower conflict: overwrite from index 2 with a shorter record; index 3 must disappear
	w.Offsets = w.Offsets[:1]
	w.offsetVersions = w.offsetVersions[:1]
	// 覆盖点 = 第一条记录的末尾。记录 = recordHeader + key + value，key 原样写入，
	// 所以宽度就是 key 自己的长度——不要写死任何常数（这里曾写死 10，存储层把宽度改成 24
	// 之后偏移落在记录中间，恢复把尾部读成损坏的 header，测试以 index out of range 崩掉）。
	firstRecordEnd := w.Offsets[0] + int64(recordHeader) + int64(len("a")) + int64(len("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	w.OverwriteLogFileFrom([]*Entry{putEntry(2, 2, "b", "short")}, firstRecordEnd)
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

// 快照要发的是当前日志的一个**记录对齐**的前缀。长度必须取自写入端自己维护的
// rf.logOffset，不能 os.Stat 那个文件：冲突覆盖会 Seek 回退再写，覆盖点之后的字节
// 已经不属于日志了，而文件长度在截断之前仍然把它们算在内。按 os.Stat 的长度发过去，
// 接收端就会把那些陈旧字节当成记录读出来。
func TestCutLogIsRecordAligned(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)
	rf.fileBaseIndex, rf.fileBaseTerm = 15000, 3

	var entries []*Entry
	var want int64
	for i := 1; i <= 20; i++ {
		e := entryOf(uint32(15000+i), fmt.Sprintf("key%02d", i), "value-bytes")
		entries = append(entries, e)
		want += recordLen(rf, e)
	}
	rf.mu.Lock()
	rf.AppendToLogFile(entries)
	rf.mu.Unlock()

	cut, err := rf.CutLog()
	if err != nil {
		t.Fatalf("CutLog: %v", err)
	}
	if cut.Bytes != want {
		t.Errorf("切点 %d 字节; want %d（20 字节头 + key + value 逐条累加）", cut.Bytes, want)
	}
	st, err := os.Stat(rf.currentLog)
	if err != nil {
		t.Fatal(err)
	}
	if st.Size() != cut.Bytes {
		t.Errorf("切点说 %d 字节，文件里实际 %d 字节——切点必须已经刷净", cut.Bytes, st.Size())
	}
	if cut.Path != rf.currentLog {
		t.Errorf("切点指向 %s; want %s", cut.Path, rf.currentLog)
	}
	if cut.BaseIndex != 15000 || cut.BaseTerm != 3 {
		t.Errorf("切点基址 (%d,%d); want (15000,3)——这就是快照的 lastIncludedIndex/Term",
			cut.BaseIndex, cut.BaseTerm)
	}
	if cut.LastIndex != 15020 {
		t.Errorf("切点最后一条 index = %d; want 15020", cut.LastIndex)
	}
}

// 冲突覆盖之后再取切点：长度必须是覆盖后的真实日志长度，不是覆盖前的文件长度。
// 这是"别用 os.Stat"那条理由的用例化。
func TestCutLogAfterOverwrite(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)

	var long []*Entry
	for i := 1; i <= 30; i++ {
		long = append(long, entryOf(uint32(i), fmt.Sprintf("k%02d", i), "aaaaaaaaaa"))
	}
	rf.mu.Lock()
	rf.AppendToLogFile(long)
	rf.mu.Unlock()
	full, err := rf.CutLog()
	if err != nil {
		t.Fatal(err)
	}

	// 从第 11 条的位置起覆盖 3 条（leader 与本节点在那里分叉）
	startPos := int64(0)
	for i := 0; i < 10; i++ {
		startPos += recordLen(rf, long[i])
	}
	replacement := []*Entry{
		entryOf(11, "k11", "bb"), entryOf(12, "k12", "bb"), entryOf(13, "k13", "bb"),
	}
	rf.mu.Lock()
	rf.OverwriteLogFileFrom(replacement, startPos)
	rf.mu.Unlock()

	cut, err := rf.CutLog()
	if err != nil {
		t.Fatal(err)
	}
	var want int64 = startPos
	for _, e := range replacement {
		want += recordLen(rf, e)
	}
	if cut.Bytes != want {
		t.Errorf("覆盖之后切点 %d 字节; want %d", cut.Bytes, want)
	}
	if cut.Bytes >= full.Bytes {
		t.Errorf("覆盖之后的日志应当比覆盖前短（%d vs %d）——否则陈旧字节还在日志里",
			cut.Bytes, full.Bytes)
	}
	if cut.LastIndex != 13 {
		t.Errorf("覆盖之后最后一条 index = %d; want 13", cut.LastIndex)
	}
}

// 冲突点正好落在文件的第一条记录（偏移 0）时，也必须按覆盖写处理。
//
// 这是"追加/覆盖不能从 startPos 的取值去猜"那条的用例化：旧实现把 startPos == 0
// 当作追加，于是这一情形下新记录接在旧记录后面、陈旧字节一条没截，重启时
// RecoverLog 顺序回放撞上旧记录，索引不连续，节点起不来。
//
// 偏移 0 在两种寻常情况下就会出现：全新节点收到的第一条，以及 GC 换文件之后写进
// 新文件的第一条（SetCurrentLog 把 logOffset 归零）。
func TestOverwriteAtOffsetZeroTruncatesStaleTail(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	// term 1 的 leader 写了三条就被隔离，这三条都没提交
	w.AppendToLogFile([]*Entry{putEntry(1, 1, "a", "aaaaaaaaaaaaaaaa")})
	w.AppendToLogFile([]*Entry{putEntry(2, 1, "b", "bbbbbbbbbbbbbbbb")})
	w.AppendToLogFile([]*Entry{putEntry(3, 1, "c", "cccccccccccccccc")})
	if w.Offsets[0] != 0 {
		t.Fatalf("第一条的偏移 = %d, want 0——这个用例的前提就是它在 0", w.Offsets[0])
	}

	// term 2 的新 leader 从 index 1 起就与本节点分叉：冲突点 = 偏移 0
	w.Offsets = w.Offsets[:0]
	w.offsetVersions = w.offsetVersions[:0]
	w.OverwriteLogFileFrom([]*Entry{putEntry(1, 2, "z", "zz")}, 0)
	w.CloseLogFile()

	wantSize := int64(recordHeader + len("z") + len("zz"))
	if info, err := os.Stat(logPath); err != nil {
		t.Fatal(err)
	} else if info.Size() != wantSize {
		t.Fatalf("覆盖后文件 %d 字节, want %d——被截断的三条字节还留在文件里", info.Size(), wantSize)
	}
	if len(w.Offsets) != 1 || w.Offsets[0] != 0 {
		t.Fatalf("覆盖后 Offsets = %v, want [0]", w.Offsets)
	}

	rf := &Raft{persister: &Persister{}}
	last, err := rf.RecoverLog([]LogFile{{Path: logPath}}, 0)
	if err != nil {
		t.Fatalf("RecoverLog: %v", err) // 旧实现在这里报 log not contiguous
	}
	if last != 1 || len(rf.log) != 1 {
		t.Fatalf("恢复出 last=%d、%d 条日志, want 1 / 1", last, len(rf.log))
	}
	if rf.log[0].Term != 2 || rf.log[0].Command.Value != "zz" {
		t.Fatalf("恢复出的第一条 term=%d value=%q, want 2 / \"zz\"", rf.log[0].Term, rf.log[0].Command.Value)
	}
}

// 覆盖写不带任何条目，等于"把冲突点之后全部丢掉"。日志文件必须就此截到冲突点。
func TestOverwriteWithNoEntriesTruncates(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "RaftState.log")
	w := newLogWriter(t, logPath, 0)
	first := putEntry(1, 1, "a", "aaaa")
	w.AppendToLogFile([]*Entry{first})
	w.AppendToLogFile([]*Entry{putEntry(2, 1, "b", "bbbb")})
	cut := int64(recordHeader + len(first.Key) + len(first.Value))

	w.Offsets = w.Offsets[:1]
	w.offsetVersions = w.offsetVersions[:1]
	w.OverwriteLogFileFrom(nil, cut)
	w.CloseLogFile()

	if info, err := os.Stat(logPath); err != nil {
		t.Fatal(err)
	} else if info.Size() != cut {
		t.Fatalf("文件 %d 字节, want %d", info.Size(), cut)
	}
	if w.logOffset != cut {
		t.Fatalf("logOffset = %d, want %d（下一条应接在冲突点上）", w.logOffset, cut)
	}
}
