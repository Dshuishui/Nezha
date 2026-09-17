package raft

import (
	"strings"
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

// ---- 字节计量 ----
//
// 截断预算按字节，所以 rf.logBytes 必须精确。它由四个入口维护，散在各处逐个加减迟早会漏，
// 而漏掉是**静默**的：计数偏小 → 预算失效 → 内存又无界。这个用例就是那道防线：
// 每一步都重算一遍再比对。

func recomputeLogBytes(rf *Raft) int64 {
	var n int64
	for _, e := range rf.log {
		n += logEntryBytes(e)
	}
	return n
}

func entryWith(index int, term int32, key, value string) *raftrpc.LogEntry {
	return &raftrpc.LogEntry{Term: term, Command: &raftrpc.DetailCod{
		Index: int32(index), Term: term, OpType: "Put", Key: key, Value: value}}
}

func TestLogBytesTracksMutations(t *testing.T) {
	rf := &Raft{}
	check := func(step string) {
		t.Helper()
		if want := recomputeLogBytes(rf); rf.logBytes != want {
			t.Fatalf("%s 之后 logBytes=%d，重算得 %d——预算会按错的数生效", step, rf.logBytes, want)
		}
	}

	// 追加：长短不一的 key/value，免得等长掩盖了加减错位
	for i := 1; i <= 50; i++ {
		rf.appendLog(entryWith(i, 1, strings.Repeat("k", i%7+1), strings.Repeat("v", i*3)))
	}
	check("追加 50 条")
	if rf.logBytes <= 50*logEntryOverheadBytes {
		t.Errorf("logBytes=%d，连固定开销都不够——key/value 没被算进去", rf.logBytes)
	}

	// follower 发现冲突：丢掉 pos 及其后
	rf.truncateLogFrom(30)
	check("从下标 30 截断")
	if len(rf.log) != 30 {
		t.Fatalf("截断后 %d 条; want 30", len(rf.log))
	}

	// 压缩：丢掉最前面若干条
	rf.dropLogPrefix(10)
	check("丢掉前 10 条")
	if len(rf.log) != 20 {
		t.Fatalf("压缩后 %d 条; want 20", len(rf.log))
	}
	// 必须重新分配底层数组，否则一个字节都不会释放——这是整件事的起点
	if cap(rf.log) != 20 {
		t.Errorf("cap=%d; want 20（dropLogPrefix 必须 make+copy，不能只移头指针）", cap(rf.log))
	}

	// 整体替换（恢复 / 装快照）
	rf.setLog([]*raftrpc.LogEntry{entryWith(1, 1, "a", "bb"), entryWith(2, 1, "ccc", "d")})
	check("整体替换")

	rf.setLog(nil)
	check("清空")
	if rf.logBytes != 0 {
		t.Errorf("清空后 logBytes=%d; want 0", rf.logBytes)
	}

	// 边界：越界的下标不该改动任何东西
	rf.appendLog(entryWith(1, 1, "k", "v"))
	before := rf.logBytes
	rf.truncateLogFrom(-1)
	rf.truncateLogFrom(99)
	rf.dropLogPrefix(0)
	rf.dropLogPrefix(-5)
	if rf.logBytes != before || len(rf.log) != 1 {
		t.Errorf("越界下标改动了状态：logBytes %d→%d，条数 %d", before, rf.logBytes, len(rf.log))
	}
	// 丢掉全部
	rf.dropLogPrefix(1)
	check("丢掉全部")
}

// budgetFloorLocked 的工作量必须正比于**预算**而不是日志长度（它在压缩循环里持着 rf.mu），
// 而语义上：日志在预算内就不构成约束。
func TestBudgetFloor(t *testing.T) {
	rf := &Raft{lastIncludedIndex: 1000}
	for i := 1; i <= 100; i++ {
		rf.appendLog(entryWith(1000+i, 1, "k", strings.Repeat("v", 84))) // 每条 216+1+84 = 301 B
	}
	per := logEntryBytes(rf.log[0])

	// 预算大到装得下整个日志：不构成约束
	if got := rf.budgetFloorLocked(per * 1000); got != 1000 {
		t.Errorf("预算充裕时 floor=%d; want 1000（即 lastIncludedIndex，不构成约束）", got)
	}
	// 预算刚好 10 条：应当保留最后 10 条，压缩点是倒数第 10 条的前一条
	// 最后一条的 index 是 1100，倒数第 10 条是 1091，所以压缩点是 1090
	if got := rf.budgetFloorLocked(per * 10); got != 1090 {
		t.Errorf("预算 10 条时 floor=%d; want 1090", got)
	}
	// 预算连一条都装不下：只保留最后一条
	if got := rf.budgetFloorLocked(1); got != 1099 {
		t.Errorf("预算 1 字节时 floor=%d; want 1099", got)
	}
}

// ---- 三档规则 ----

// 三节点 leader：日志 1001..1100 在内存里，每条 301 B。
func leaderForTruncTest(budget int64) *Raft {
	rf := &Raft{me: 0, peers: make([]string, 3), role: ROLE_LEADER,
		lastIncludedIndex: 1000, lastIncludedTerm: 1, lastApplied: 1100,
		logBudgetBytes: budget}
	rf.nextIndex = make([]int, 3)
	rf.matchIndex = make([]int, 3)
	for i := 1; i <= 100; i++ {
		rf.appendLog(entryWith(1000+i, 1, "k", strings.Repeat("v", 84)))
	}
	return rf
}

// 第一档：日志在预算内时，落后的副本被无条件保护——和改造之前一样。
// 这一档存在的理由是"别把只是稍慢的活跃副本推去走快照"，那比补日志贵得多。
func TestTruncationTierOneProtectsWithinBudget(t *testing.T) {
	rf := leaderForTruncTest(1 << 30) // 预算远大于日志
	rf.matchIndex[1] = 1010           // 严重落后
	rf.matchIndex[2] = 1100
	rf.notePeerActive(1)
	rf.notePeerActive(2)

	d := rf.compactDecisionLocked(5)
	if d.overBudget {
		t.Fatal("预算充裕却判成超预算")
	}
	if d.point != 1010 {
		t.Errorf("压缩点 = %d; want 1010（保护到落后副本的 Match）", d.point)
	}
	if d.pinnedBy != 1 {
		t.Errorf("pinnedBy = %d; want 1", d.pinnedBy)
	}
	if d.cappedBy != -1 {
		t.Errorf("预算充裕却报了被截断的副本 %d", d.cappedBy)
	}
}

// 第二档：超预算时，**不活跃**的副本不再被保护——它回来时去要快照。
// 活跃的仍然保护（只要不撞第三档）。
func TestTruncationTierTwoDropsInactivePeer(t *testing.T) {
	// 预算只够 20 条，日志 100 条 → 超预算
	rf := leaderForTruncTest(logEntryBytes(entryWith(0, 1, "k", strings.Repeat("v", 84))) * 20)
	rf.matchIndex[1] = 1010 // 落后很多且**不活跃**（从没回执）
	rf.matchIndex[2] = 1090 // 落后一点但活跃，且仍在 floor(1080) 之后
	rf.notePeerActive(2)

	d := rf.compactDecisionLocked(5)
	if !d.overBudget {
		t.Fatal("日志 100 条、预算 20 条，应判超预算")
	}
	// peer1 不活跃且超预算 → 不为它保留；peer2 活跃且在 floor 之后 → 保护到 1090
	if d.point != 1090 {
		t.Errorf("压缩点 = %d; want 1090（只保护活跃的那个）", d.point)
	}
	if d.pinnedBy != 2 {
		t.Errorf("pinnedBy = %d; want 2", d.pinnedBy)
	}
	if d.cappedBy != -1 {
		t.Errorf("cappedBy = %d; want -1（被丢下的那个是第二档，不是被预算截的）", d.cappedBy)
	}

	// 同一个副本，同样的落后程度，只是变成**活跃**：第二档就不该丢下它了，
	// 于是换成第三档兜住——压缩点落到 floor 而不是一路跟到 1010。
	rf.notePeerActive(1)
	d = rf.compactDecisionLocked(5)
	if d.point != d.floor {
		t.Errorf("压缩点 = %d; want floor=%d（活跃了就由第三档兜）", d.point, d.floor)
	}
	if d.cappedBy != 1 {
		t.Errorf("cappedBy = %d; want 1", d.cappedBy)
	}
}

// 第三档：连**活跃**副本也不能无界地钉住压缩点。
// CockroachDB 不需要这一档（它有提案配额、日志在盘上），我们必须有——
// 否则实验机上那种"活跃但持续落后"的副本会把 leader 的内存拖到 OOM。
func TestTruncationTierThreeCapsActivePeer(t *testing.T) {
	per := logEntryBytes(entryWith(0, 1, "k", strings.Repeat("v", 84)))
	rf := leaderForTruncTest(per * 20) // 预算 20 条
	rf.matchIndex[1] = 1010            // 落后很多，但**活跃**
	rf.matchIndex[2] = 1100
	rf.notePeerActive(1)
	rf.notePeerActive(2)

	d := rf.compactDecisionLocked(5)
	// floor：保留最后 20 条 → 压缩点 1080
	if d.floor != 1080 {
		t.Fatalf("floor = %d; want 1080", d.floor)
	}
	if d.point != 1080 {
		t.Errorf("压缩点 = %d; want 1080（活跃副本也被预算截住）", d.point)
	}
	if d.cappedBy != 1 || d.cappedAt != 1010 {
		t.Errorf("cappedBy=%d cappedAt=%d; want 1 / 1010——被截断的副本必须报出来，"+
			"它接下来要靠快照补齐", d.cappedBy, d.cappedAt)
	}
}

// follower 上没有 matchIndex，压缩只受 lastApplied 约束；单节点同理（循环为空）。
// 预算比 catchUpEntries 那个固定窗口更紧时，压缩点必须由预算决定。
//
// 这一档此前**没有任何测试**，而它正是 2026-09-17 那个 bug 藏身的地方：d.floor 只用来
// 抬高 peer 的保护位置，从没推进过 d.point，于是写死的 catchUpEntries 成了预算管不到的
// 底线。条数看起来完全正常（恒为 catchUpEntries 条），只有换算成字节才露出来——
// 16KB value 时 5000 条就是 83MB，而预算给的是 16MB。
//
// 没有 peer 参与，正是为了把这一条和第三档（截住某个落后副本）分开：即使所有副本都
// 追平，固定窗口本身也不许超预算。
func TestBudgetCapsRetentionWindowWithNoLaggingPeer(t *testing.T) {
	per := logEntryBytes(entryWith(0, 1, "k", strings.Repeat("v", 84)))
	rf := leaderForTruncTest(per * 20) // 预算只装得下 20 条
	// 所有副本都追平到最新，第一、二、三档都不会因为某个 peer 而动压缩点。
	rf.matchIndex[1] = 1100
	rf.matchIndex[2] = 1100
	rf.notePeerActive(1)
	rf.notePeerActive(2)

	// 固定窗口要求保留 500 条（远超预算的 20 条）。
	d := rf.compactDecisionLocked(500)

	if d.floor != 1080 {
		t.Fatalf("floor = %d; want 1080（保留最后 20 条）", d.floor)
	}
	if d.point != 1080 {
		t.Errorf("压缩点 = %d; want 1080——预算比固定窗口紧时必须由预算决定；"+
			"修复前这里是 lastApplied-500=600，即驻留 500 条 / %dB，是预算的 %d 倍",
			d.point, int64(500)*per, int64(500)*per/(per*20))
	}
	if !d.budgetCapsWindow {
		t.Errorf("budgetCapsWindow = false; want true——这一步必须能被日志和测试看见，"+
			"否则「为什么慢副本这么快就要快照」没有线索")
	}
	if d.cappedBy != -1 {
		t.Errorf("cappedBy = %d; want -1：没有落后的副本，收窄纯粹来自预算，"+
			"不该报成「某个副本被截住」", d.cappedBy)
	}
}

// 反向：预算宽松时，固定窗口说话，budgetCapsWindow 必须是 false。
// 没有这一条，上面那个测试用"永远让预算说话"也能过。
func TestFixedWindowWinsWhenBudgetIsAmple(t *testing.T) {
	per := logEntryBytes(entryWith(0, 1, "k", strings.Repeat("v", 84)))
	rf := leaderForTruncTest(per * 10000) // 预算远大于整个日志
	rf.matchIndex[1] = 1100
	rf.matchIndex[2] = 1100
	rf.notePeerActive(1)
	rf.notePeerActive(2)

	d := rf.compactDecisionLocked(5)
	if d.budgetCapsWindow {
		t.Errorf("budgetCapsWindow = true；预算宽松时不该由它决定")
	}
	if d.point != rf.lastApplied-5 {
		t.Errorf("压缩点 = %d; want %d（固定窗口）", d.point, rf.lastApplied-5)
	}
}

func TestTruncationWithoutPeers(t *testing.T) {
	rf := leaderForTruncTest(1 << 30)
	rf.role = ROLE_FOLLOWER
	if d := rf.compactDecisionLocked(5); d.point != 1095 || d.pinnedBy != -1 {
		t.Errorf("follower 上压缩点 = %d pinnedBy = %d; want 1095 / -1", d.point, d.pinnedBy)
	}

	single := &Raft{me: 0, peers: make([]string, 1), role: ROLE_LEADER,
		lastApplied: 1100, matchIndex: make([]int, 1), logBudgetBytes: 1 << 30}
	if d := single.compactDecisionLocked(5); d.point != 1095 {
		t.Errorf("单节点压缩点 = %d; want 1095", d.point)
	}
}
