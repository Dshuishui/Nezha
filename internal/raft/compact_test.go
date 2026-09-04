package raft

import (
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

func mkLog(n int, term int32) []*raftrpc.LogEntry {
	l := make([]*raftrpc.LogEntry, n)
	for i := range l {
		l[i] = &raftrpc.LogEntry{Term: term, Command: &raftrpc.DetailCod{Index: int32(i + 1)}}
	}
	return l
}

// 未压缩时行为必须与改动前完全一致
func TestIndexMathNoCompaction(t *testing.T) {
	rf := &Raft{log: mkLog(10, 3)}
	if got := rf.index2LogPos(1); got != 0 {
		t.Fatalf("index2LogPos(1) = %d, want 0", got)
	}
	if got := rf.index2LogPos(10); got != 9 {
		t.Fatalf("index2LogPos(10) = %d, want 9", got)
	}
	if got := rf.lastIndex(); got != 10 {
		t.Fatalf("lastIndex = %d, want 10", got)
	}
	if got := rf.firstIndex(); got != 1 {
		t.Fatalf("firstIndex = %d, want 1", got)
	}
	if got := rf.termAt(5); got != 3 {
		t.Fatalf("termAt(5) = %d, want 3", got)
	}
	if got := rf.termAt(11); got != -1 {
		t.Fatalf("termAt(11) = %d, want -1 (不存在)", got)
	}
}

// 压缩后：index 语义不变，越界访问返回 -1 而非 panic
func TestIndexMathAfterCompaction(t *testing.T) {
	rf := &Raft{log: mkLog(10, 3), lastIncludedIndex: 100, lastIncludedTerm: 2}
	// rf.log[0] 对应 index 101
	if got := rf.index2LogPos(101); got != 0 {
		t.Fatalf("index2LogPos(101) = %d, want 0", got)
	}
	if got := rf.index2LogPos(110); got != 9 {
		t.Fatalf("index2LogPos(110) = %d, want 9", got)
	}
	if got := rf.lastIndex(); got != 110 {
		t.Fatalf("lastIndex = %d, want 110", got)
	}
	if got := rf.firstIndex(); got != 101 {
		t.Fatalf("firstIndex = %d, want 101", got)
	}
	if got := rf.termAt(105); got != 3 {
		t.Fatalf("termAt(105) = %d, want 3", got)
	}
	// 基址本身的 term 必须可查（PrevLogTerm 一致性检查依赖它）
	if got := rf.termAt(100); got != 2 {
		t.Fatalf("termAt(100) = %d, want 2 (lastIncludedTerm)", got)
	}
	// 已压缩区间与未来区间都返回 -1，不 panic
	for _, idx := range []int{1, 50, 99, 111, 999} {
		if got := rf.termAt(idx); got != -1 {
			t.Fatalf("termAt(%d) = %d, want -1", idx, got)
		}
	}
}

// 全部压缩后 lastTerm 退回 lastIncludedTerm
func TestLastTermFullyCompacted(t *testing.T) {
	rf := &Raft{log: nil, lastIncludedIndex: 100, lastIncludedTerm: 7}
	if got := rf.lastTerm(); got != 7 {
		t.Fatalf("lastTerm = %d, want 7", got)
	}
	if got := rf.lastIndex(); got != 100 {
		t.Fatalf("lastIndex = %d, want 100", got)
	}
}

// 核心：make+copy 截断确实释放底层数组；对照 reslice 不释放
func TestCompactionActuallyFreesMemory(t *testing.T) {
	orig := mkLog(1000, 1)

	resliced := orig[900:]
	if cap(resliced) != 100 {
		t.Fatalf("reslice cap = %d, want 100 (底层数组仍被引用)", cap(resliced))
	}

	pos := 899
	compacted := make([]*raftrpc.LogEntry, len(orig)-pos-1)
	copy(compacted, orig[pos+1:])
	if cap(compacted) != 100 {
		t.Fatalf("compacted cap = %d, want 100", cap(compacted))
	}
	if compacted[0] != orig[900] {
		t.Fatal("copy 后首元素不匹配")
	}
}
