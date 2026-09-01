package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
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

// 攒批必须保持 index 升序，且每条记录的内容各自独立。
// 原实现用包级的 entry_global 取地址，逐条写入时无碍；攒批后同一批里的指针
// 会全部指向它，写出去的是最后一条重复 N 次——这个用例专门钉住这一点。
func TestGroupCommitPreservesOrderAndContent(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)
	rf.EnableGroupCommit(2 * time.Millisecond)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			e := entryOf(uint32(k+1), fmt.Sprintf("k%03d", k), fmt.Sprintf("v%03d", k))
			b, first := func() (*flushBatch, bool) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				return rf.enqueueForFlush(e)
			}()
			if first {
				select {
				case rf.flushSignal <- struct{}{}:
				default:
				}
			}
			<-b.done
		}(i)
	}
	wg.Wait()

	raw, err := os.ReadFile(rf.currentLog)
	if err != nil {
		t.Fatal(err)
	}
	// 顺序解析整个文件，校验每条记录的 key 与 value 配对正确、且互不相同
	seen := map[string]string{}
	var off int64
	for off < int64(len(raw)) {
		keySize := binary.LittleEndian.Uint32(raw[off+12 : off+16])
		valSize := binary.LittleEndian.Uint32(raw[off+16 : off+20])
		key := string(raw[off+20 : off+20+int64(keySize)])
		val := string(raw[off+20+int64(keySize) : off+20+int64(keySize)+int64(valSize)])
		seen[strings.TrimLeft(key, "0")] = val
		off += 20 + int64(keySize) + int64(valSize)
	}
	if len(seen) != n {
		t.Fatalf("文件中有 %d 条不同的 key，want %d —— 攒批可能写重了同一条", len(seen), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%03d", i)
		want := fmt.Sprintf("v%03d", i)
		if seen[k] != want {
			t.Fatalf("key %s 的值为 %q, want %q —— entry 之间串了内容", k, seen[k], want)
		}
	}
	// Offsets 必须与写入顺序一一对应
	if len(rf.Offsets) != n {
		t.Fatalf("Offsets 有 %d 项, want %d", len(rf.Offsets), n)
	}
	for i := 1; i < len(rf.Offsets); i++ {
		if rf.Offsets[i] <= rf.Offsets[i-1] {
			t.Fatalf("Offsets 非递增：第 %d 项 %d <= 第 %d 项 %d",
				i, rf.Offsets[i], i-1, rf.Offsets[i-1])
		}
	}
}
