package kvstore

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// GET 是多路查找：当前 valuelog、上一轮 valuelog、分区文件并发查，按优先级取结果。
// 这里钉住两条规矩。它们此前只有第二条、且只在有序文件那一路上成立——
// valuelog/RocksDB 那几路是 panic，一次读失败带走整个节点。

// 某一路出错，而另一路找到了：必须返回找到的那个值。
// 原先第一路出错就 panic，连"继续问下一路"的机会都没有。
func TestReadOutcomeErrorInOnePathDoesNotHideAnother(t *testing.T) {
	var out readOutcome
	out.note("当前 valuelog", errors.New("transient read error"))
	// 第二路找到了 → 调用方直接返回，不该走到 finish
	if out.err == nil {
		t.Fatal("note 没记下错误")
	}
	// 走到 finish 才是"全都没找到"
	reply := &kvrpc.GetInRaftResponse{Err: raft.OK}
	out.finish(reply, "k")
	if reply.Err != raft.ErrInternal {
		t.Errorf("出过错却报 %v; want ErrInternal——把读失败报成 NOKEY 会让丢数据看起来像负载配置问题", reply.Err)
	}
}

// 全都没找到、且一路都没出错：这才是真正的键不存在。
func TestReadOutcomeCleanMissIsNoKey(t *testing.T) {
	var out readOutcome
	out.note("当前 valuelog", nil)
	out.note("上一轮分区", fmt.Errorf("wrapped: %w", ErrKeyAbsent))
	reply := &kvrpc.GetInRaftResponse{Err: raft.OK}
	out.finish(reply, "k")
	if reply.Err != raft.ErrNoKey {
		t.Errorf("干净的未命中报 %v; want ErrNoKey", reply.Err)
	}
	if reply.Value != raft.NoKey {
		t.Errorf("Value = %q; want %q", reply.Value, raft.NoKey)
	}
}

// ErrKeyAbsent 是"这一处没有"，是常态，不能被当成错误记下来——
// 否则每个注定 miss 的读都会报 ErrInternal。
func TestReadOutcomeKeyAbsentIsNotAnError(t *testing.T) {
	var out readOutcome
	out.note("分区 A", ErrKeyAbsent)
	out.note("分区 B", fmt.Errorf("在分区 B 里没有: %w", ErrKeyAbsent))
	if out.err != nil {
		t.Fatalf("ErrKeyAbsent 被当成了错误: %v", out.err)
	}
}

// 只留第一个错误：后面同类的错误补充不了新信息，而现场要指向最先出问题的那一路。
func TestReadOutcomeKeepsFirstError(t *testing.T) {
	var out readOutcome
	out.note("第一路", errors.New("first"))
	out.note("第二路", errors.New("second"))
	if out.err == nil || out.err.Error() != "first" {
		t.Fatalf("留下的错误 = %v; want first", out.err)
	}
	if out.where != "第一路" {
		t.Fatalf("现场 = %q; want 第一路", out.where)
	}
}

// logReader 在一次扫描里复用同一个句柄和缓冲区，所以**必须**在每次 Seek 之后 Reset
// bufio：不 Reset 的话缓冲区里还压着上一个偏移读出来的字节，第二个 key 会读到第一个
// key 的记录——值错了而且不报错。
func TestLogReaderRereadsAfterSeek(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "RaftState.log")

	rf := &raft.Raft{}
	rf.SetCurrentLog(path)
	entries := []*raft.Entry{
		{Index: 1, CurrentTerm: 1, Key: "k1", Value: "value-one"},
		{Index: 2, CurrentTerm: 1, Key: "k2", Value: "value-two-longer"},
		{Index: 3, CurrentTerm: 1, Key: "k3", Value: "v3"},
	}
	rf.AppendToLogFile(entries)
	offsets := append([]int64(nil), rf.GetOffsets()...)
	rf.CloseLogFile()
	if len(offsets) != 3 {
		t.Fatalf("写入后有 %d 个偏移，want 3", len(offsets))
	}

	lr := &logReader{path: path}
	defer lr.Close()
	// 乱序读，并且把同一个偏移读两次：顺序依赖或缓冲区没重置都会在这里露出来
	for _, i := range []int{2, 0, 1, 0, 2} {
		got, err := lr.valueAt(offsets[i])
		if err != nil {
			t.Fatalf("valueAt(offset[%d]): %v", i, err)
		}
		if got != entries[i].Value {
			t.Fatalf("按偏移 %d 读出 %q，want %q——Seek 之后缓冲区没重置",
				offsets[i], got, entries[i].Value)
		}
	}
}

// 惰性打开：不需要读 valuelog 的扫描（baseline 裸 value、全内联）一次 open 都不该做。
// 判据取"文件不存在时构造 logReader 不报错、也不打开"。
func TestLogReaderOpensLazily(t *testing.T) {
	lr := &logReader{path: filepath.Join(t.TempDir(), "does-not-exist.log")}
	if lr.f != nil {
		t.Fatal("构造时就打开了文件")
	}
	lr.Close() // 没打开过也要能安全关闭
	if _, err := lr.valueAt(0); err == nil {
		t.Fatal("文件不存在却读成功了")
	}
}
