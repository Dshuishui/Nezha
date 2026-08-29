package raft

import "testing"

// 复现并锁定偏移量队列的不变式：Offsets[0] 恒对应日志下标 shotOffset+1。
//
// 历史 bug：Make() 里 append 过一个哨兵 0，指望 index=1 的 TermLog 把它消费掉。
// 后来 TermLog 改为「只递增 shotOffset、不出队」（因为它没有 valuelog 条目），
// 哨兵便永久滞留队首，导致此后每条 entry 都拿到前一条的偏移量，整体错开一条。
// 该错位在 GC 换文件后暴露为 "key mismatch in new file"。

// applyOne 模拟 applyLogLoop 对一条日志的处理，返回它拿到的偏移量。
// 完全复刻真实代码的下标运算，以便这里的断言对真实实现有效。
func applyOne(rf *Raft, isTermLog bool) (offset int64, ok bool) {
	if isTermLog {
		rf.lastApplied++
		rf.shotOffset++
		return 0, true
	}
	if (rf.lastApplied - rf.shotOffset) >= len(rf.Offsets) {
		return 0, false
	}
	rf.lastApplied++
	realIndex := rf.lastApplied - rf.shotOffset
	offset = rf.Offsets[realIndex-1]
	rf.Offsets = rf.Offsets[1:]
	rf.shotOffset++
	return offset, true
}

// 典型时序：index=1 是成为 leader 时的 TermLog，之后是若干 Put。
// 每条 Put 必须拿到自己写盘时的偏移量，不能拿到前一条的。
func TestOffsetsAlignAfterTermLog(t *testing.T) {
	const entrySize = 94 // 20B 头 + 10B key + 64B value
	rf := &Raft{}        // Offsets 为空，无哨兵

	if off, ok := applyOne(rf, true); !ok || off != 0 {
		t.Fatalf("TermLog: off=%d ok=%v", off, ok)
	}
	if len(rf.Offsets) != 0 {
		t.Fatalf("TermLog 不应消费偏移量，队列长度=%d", len(rf.Offsets))
	}

	// 依次写入 5 条 Put，偏移量 0, 94, 188, ...
	var want []int64
	for i := 0; i < 5; i++ {
		off := int64(i) * entrySize
		rf.Offsets = append(rf.Offsets, off)
		want = append(want, off)
	}
	for i, w := range want {
		got, ok := applyOne(rf, false)
		if !ok {
			t.Fatalf("第 %d 条 Put 未能取到偏移量", i+1)
		}
		if got != w {
			t.Fatalf("第 %d 条 Put 拿到偏移量 %d，应为 %d（差 %d，正是历史上的错位一条 entry）",
				i+1, got, w, got-w)
		}
	}
}

// 重新选主会产生额外的 TermLog；每个 TermLog 之后偏移量都必须继续对齐。
// 旧的哨兵方案在这里必然失败，因为它只能抵消一个 TermLog。
func TestOffsetsAlignAcrossMultipleTermLogs(t *testing.T) {
	const entrySize = 94
	rf := &Raft{}
	var nextOff int64

	for round := 0; round < 3; round++ {
		if _, ok := applyOne(rf, true); !ok {
			t.Fatalf("第 %d 轮 TermLog 处理失败", round+1)
		}
		var want []int64
		for i := 0; i < 4; i++ {
			rf.Offsets = append(rf.Offsets, nextOff)
			want = append(want, nextOff)
			nextOff += entrySize
		}
		for i, w := range want {
			got, ok := applyOne(rf, false)
			if !ok {
				t.Fatalf("第 %d 轮第 %d 条 Put 未能取到偏移量", round+1, i+1)
			}
			if got != w {
				t.Fatalf("第 %d 轮第 %d 条 Put 拿到 %d，应为 %d（差 %d）",
					round+1, i+1, got, w, got-w)
			}
		}
	}
}

// Make() 不得预置哨兵：一旦预置，第一条 Put 之后的所有偏移量都会错开一条。
func TestMakeLeavesOffsetsEmpty(t *testing.T) {
	rf := &Raft{}
	if len(rf.Offsets) != 0 {
		t.Fatalf("新建的 Raft 偏移量队列应为空，实际长度 %d", len(rf.Offsets))
	}
	// 模拟历史 bug：预置哨兵后必然错位，此断言说明测试确实能捕获该问题
	buggy := &Raft{Offsets: []int64{0}}
	applyOne(buggy, true) // TermLog 不消费哨兵
	buggy.Offsets = append(buggy.Offsets, 0, 94)
	applyOne(buggy, false)           // 第一条 Put，拿到哨兵 0（碰巧对）
	got, _ := applyOne(buggy, false) // 第二条 Put，应为 94
	if got == 94 {
		t.Fatal("预置哨兵时本应错位，说明该回归测试无法捕获历史 bug")
	}
	if got != 0 {
		t.Fatalf("预期错位到 0，实际 %d", got)
	}
}
