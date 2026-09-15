package raft

import (
	"fmt"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

// logPinWarnEntries 是"压缩点被 follower 按住多少条才值得报"的门槛。
// 取得比 catchUpEntries 大一截：保留窗口内的落后是正常的。
const logPinWarnEntries = 50000

// defaultLogBudgetBytes 是内存日志的字节预算：超过它，落后的副本就不再被无条件保护，
// 压缩点按预算往前推，它们改由快照补齐。
//
// 256 MiB 与业界同量级：TiKV 的 raft-log-gc-size-limit 是 192 MiB
// （另有一个条数上限 196608，两者谁先到算谁），CockroachDB 的
// RaftLogTruncationThreshold 默认 16 MiB 并取 min(它, zone RangeMaxBytes)。
// 我们比 CockroachDB 宽是因为它的 Raft 日志在盘上，超预算只是多占盘；我们的在内存里，
// 但也正因为在内存里，这个上限是硬的而不是建议值。
//
// CockroachDB 还有一条联动值得记一句：RaftProposalQuota = threshold/2，改截断阈值会连带
// 改提案配额。我们没有提案配额机制，所以不受影响——而"没有提案配额"恰好也是下面
// 第三档存在的原因。
const defaultLogBudgetBytes = 256 << 20

// In-memory log compaction and the index arithmetic that depends on it.

// compactLog 定期物理截断 rf.log，把已应用且所有 follower 都已复制的条目从内存中删除。
//
// 原先的 memoryControlLoop 只把已应用条目的 Value 置为 "NULL"，但 protobuf 三层结构
// （[]*LogEntry 槽位 + LogEntry + DetailCod）本身就占约 216B/条，与 value 大小无关。
// 实测：16KB value 能省 99%，64B value 只能省 23%，小值场景下内存仍随写入量线性增长。
// 因此这里改为物理删除条目，使 rf.log 内存变为 O(保留窗口)，与写入总量无关。
func (rf *Raft) compactLog() {
	const (
		checkInterval  = 10 * time.Second // 检查间隔
		logThreshold   = 20000            // 超过这么多条才触发压缩
		catchUpEntries = 5000             // 压缩点之后保留的条数，供慢 follower 追赶
	)

	for !rf.killed() {
		time.Sleep(checkInterval)

		rf.mu.Lock()

		if len(rf.log) <= logThreshold {
			rf.mu.Unlock()
			continue
		}

		// 快照在飞的时候整个跳过压缩，传输结束之后还要再保持一段（见
		// releaseDelayAfterSnapshot）。etcd 在 inflightSnapshots != 0 时同样是直接跳过
		// 整轮压缩，而 CockroachDB 为此给快照速率设了**下限**，理由就是发送方在传输期间
		// 会挡住日志截断。
		//
		// 省掉这一条的后果 CockroachDB 的注释里有原话：快照做完、发完、装完期间日志继续
		// 截断，落后节点追到的位点又落在新的 first index 之前，于是 "likely entering a
		// never ending loop of snapshots"（cockroachdb#8629）。
		if rf.snapshotsInFlightLocked() {
			rf.mu.Unlock()
			continue
		}

		d := rf.compactDecisionLocked(catchUpEntries)
		safeIndex := d.point

		// 预算把某个副本截住了：它会落到压缩点之前，接下来由快照补齐。
		// 这是**有意为之**的一步，所以要说清楚是谁、在什么水位上被截的。
		if d.cappedBy >= 0 {
			fmt.Printf("[LOG-TRUNCATE] peer[%d] 只复制到 %d，而内存日志已达 %dMB（预算 %dMB），"+
				"压缩点按预算推到 %d——它将通过快照补齐\n",
				d.cappedBy, d.cappedAt, rf.logBytes>>20, d.budget>>20, d.floor)
		}
		// 没被预算截住、但仍在按住压缩点的副本：日志是有界的（预算兜着），
		// 所以这只是一条值得知道的事件，不再是"内存会一直涨"。
		if d.pinnedBy >= 0 && d.cappedBy < 0 && d.pinnedAt-safeIndex >= logPinWarnEntries {
			fmt.Printf("[LOG-PINNED] peer[%d] 只复制到 %d，压缩点被从 %d 按到 %d："+
				"内存日志驻留 %d 条 / %dMB（预算 %dMB，未超）\n",
				d.pinnedBy, rf.matchIndex[d.pinnedBy], d.pinnedAt, safeIndex,
				len(rf.log), rf.logBytes>>20, d.budget>>20)
		}

		if safeIndex <= rf.lastIncludedIndex {
			rf.mu.Unlock()
			continue
		}

		pos := rf.index2LogPos(safeIndex)
		if pos < 0 || pos >= len(rf.log) {
			rf.mu.Unlock()
			continue
		}

		before := len(rf.log)
		newBase := safeIndex
		newTerm := rf.log[pos].Term

		rf.dropLogPrefix(pos + 1)

		rf.lastIncludedIndex = newBase
		rf.lastIncludedTerm = newTerm

		util.DPrintf("RaftNode[%d] compactLog: %d -> %d 条, lastIncludedIndex[%d] lastApplied[%d]",
			rf.me, before, len(rf.log), rf.lastIncludedIndex, rf.lastApplied)

		rf.mu.Unlock()
	}
}

// 最后的index
func (rf *Raft) lastIndex() int {
	return rf.lastIncludedIndex + len(rf.log)
}

// 最后的term
func (rf *Raft) lastTerm() (lastLogTerm int) {
	if len(rf.log) != 0 {
		lastLogTerm = int(rf.log[len(rf.log)-1].Term)
	} else {
		lastLogTerm = int(rf.lastIncludedTerm) // 日志已被全部压缩
	}
	return
}

// 日志index转化成log数组下标
func (rf *Raft) index2LogPos(index int) (pos int) {
	return index - rf.lastIncludedIndex - 1
}

// termAt 返回 index 处日志的 term。
// 若该 index 已被压缩（且不等于 lastIncludedIndex）或尚不存在，返回 -1。
func (rf *Raft) termAt(index int) int32 {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	pos := rf.index2LogPos(index)
	if pos < 0 || pos >= len(rf.log) {
		return -1
	}
	return rf.log[pos].Term
}

// ---- rf.log 的字节计量 ----
//
// 截断预算要按**字节**而不是条数，因为条目大小跨我们自己的实验矩阵差 60 倍：
// 64B value 一条约 280 B，16KB value 一条约 16.2KB。取一个条数预算（比如 100 万条），
// 在 64B 档是 268MB，在 16KB 档就是 16GB——同一个旋钮在两档上根本不是一件事。
//
// 而按字节就必须有一个**精确**的计量。此前诊断里用的是一个"每条 280 B"的估算常数，
// 拿它来换算预算是不行的：那是个**下界**（条目本身约 216 B 与 value 无关，再加 value），
// 用下界换算会允许比预算名义值更多的内存——256B value 下超出约 70%，方向刚好是错的。
// 实测过一次：150000 条 × 256B，RSS 涨 139MB，而按 280 B/条估出来只有 38MB。
// 每次压缩时 O(n) 求和也不行：压缩持着 rf.mu，而 n 正是要被这个预算约束住的量，
// 在它生效之前 n 可以很大。
//
// 所以维护一个运行计数。**改动 rf.log 的唯一入口是下面四个方法**——散在各处逐个加减
// 迟早会漏，而漏掉是静默的（计数偏小 → 预算失效 → 内存又无界了）。
// TestLogBytesTracksMutations 用"重算一遍再比对"钉住它。

// logEntryOverheadBytes 是一条日志条目除 key/value 之外的常驻开销。
//
// 来源是实测：protobuf 三层结构（[]*LogEntry 的槽位 + LogEntry + DetailCod）约 216 B，
// 与 value 大小无关——这正是"把已应用条目的 Value 置为 NULL"那个旧做法在小值场景下
// 只能省 23% 的原因。
const logEntryOverheadBytes = 216

func logEntryBytes(e *raftrpc.LogEntry) int64 {
	if e == nil {
		return 0
	}
	n := int64(logEntryOverheadBytes)
	if c := e.GetCommand(); c != nil {
		n += int64(len(c.Key)) + int64(len(c.Value))
	}
	return n
}

// setLog 整体替换内存日志并重算字节数。只在恢复与装快照时调用，那两处本来就是 O(n)。
// 调用方须持有 rf.mu。
func (rf *Raft) setLog(entries []*raftrpc.LogEntry) {
	rf.log = entries
	var n int64
	for _, e := range entries {
		n += logEntryBytes(e)
	}
	rf.logBytes = n
}

// appendLog 追加一条。调用方须持有 rf.mu。
func (rf *Raft) appendLog(e *raftrpc.LogEntry) {
	rf.log = append(rf.log, e)
	rf.logBytes += logEntryBytes(e)
}

// truncateLogFrom 丢掉下标 pos 及其之后的全部条目（follower 发现冲突时）。
// 调用方须持有 rf.mu。
func (rf *Raft) truncateLogFrom(pos int) {
	if pos < 0 || pos > len(rf.log) {
		return
	}
	for _, e := range rf.log[pos:] {
		rf.logBytes -= logEntryBytes(e)
	}
	rf.log = rf.log[:pos]
}

// dropLogPrefix 丢掉最前面的 n 条（压缩）。
//
// 必须 make + copy 重新分配：rf.log = rf.log[n:] 只移动切片头指针，底层数组仍被引用，
// 一个字节都不会释放——这条是整件事的起点。
// 调用方须持有 rf.mu。
func (rf *Raft) dropLogPrefix(n int) {
	if n <= 0 {
		return
	}
	if n >= len(rf.log) {
		rf.log = nil
		rf.logBytes = 0
		return
	}
	for _, e := range rf.log[:n] {
		rf.logBytes -= logEntryBytes(e)
	}
	newLog := make([]*raftrpc.LogEntry, len(rf.log)-n)
	copy(newLog, rf.log[n:])
	rf.log = newLog
}

// LogFootprint 报告内存日志的条数与字节数，供诊断与测试。
func (rf *Raft) LogFootprint() (entries int, bytes int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log), rf.logBytes
}

// budgetFloorLocked 返回"至少保留最近 budget 字节"所允许的最小压缩点。
//
// 从日志末尾往前累加，攒够预算就停，所以工作量正比于**预算本身**而不是日志长度——
// 这一点是它能放在压缩循环里（持着 rf.mu）的前提。
//
// 日志整体在预算内时返回 lastIncludedIndex，也就是不构成任何约束；调用方因此不必再
// 单独判断"是否超预算"。调用方须持有 rf.mu。
func (rf *Raft) budgetFloorLocked(budget int64) int {
	var acc int64
	for i := len(rf.log) - 1; i >= 0; i-- {
		acc += logEntryBytes(rf.log[i])
		if acc >= budget {
			// 下标 i 这一条是预算之内最早的一条（它的 index 是 lastIncludedIndex+i+1），
			// 它之前的都可以压缩掉，所以压缩点是它的前一条。
			return rf.lastIncludedIndex + i
		}
	}
	return rf.lastIncludedIndex
}

// SetLogBudget 设定内存日志的字节预算。<= 0 表示用默认值。需在节点开始服务前调用。
func (rf *Raft) SetLogBudget(bytes int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.logBudgetBytes = bytes
}

// compactDecision 是一次压缩点判定的结果。把它独立出来是为了能直接测那三档规则——
// 它们是这次改造里最容易静默出错的地方：定得太紧会把一个只是稍慢的活跃副本推去走快照
// （比补日志贵得多），定得太松内存就又无界了，而两种错都不报错。
type compactDecision struct {
	point      int   // 新的压缩点：lastIncludedIndex 将变成它
	budget     int64 // 本次判定用的字节预算
	overBudget bool
	floor      int // 预算允许的最小压缩点
	pinnedBy   int // 按住压缩点的 peer，-1 = 没有
	pinnedAt   int // 若没有任何 peer 约束，本该压到哪
	cappedBy   int // 被预算截住的 peer，-1 = 没有
	cappedAt   int // 它复制到了哪
}

// compactDecisionLocked 按三档规则算出压缩点。调用方须持有 rf.mu。
//
// ---- 三档截断规则 ----
//
// 前两档是 CockroachDB 的 computeTruncateDecision：
//
//	第一档 最近活跃的 follower，保护到它的 Match。稍慢的活跃副本不该被推去走快照，
//	       补日志比搬整个状态便宜得多。
//	第二档 最近不活跃的 follower，只在日志未超预算时保护。已经掉线的副本不该把
//	       leader 的内存拖死；超预算就截断过它，让它回来时去要快照。
//
// 第三档是我们**必须加而 CockroachDB 不需要**的：压缩点不得比 lastApplied 落后超过
// 预算那么多字节，活跃副本也不例外。
//
// 为什么它们不需要：CockroachDB 有提案配额（RaftProposalQuota = threshold/2）给最慢的
// 活跃副本施加反压，而且它的 Raft 日志在盘上、内存里只有一个独立受限的 entry cache。
// 我们两者都没有——rf.log 常驻内存，写路径对慢副本**没有任何反压**，而三节点提交只等
// 中位数（leader + 任意一个 follower），慢的那个永远被落下、差距被放大而不是收敛。
// 于是"活跃就无条件保护"在我们这里等于内存无界，而那正是要修的东西：2026-09-15 三台
// 实验机实测两台各 4.6GB、第三台 463MB，而第三台是**活跃**的。
func (rf *Raft) compactDecisionLocked(catchUpEntries int) compactDecision {
	d := compactDecision{pinnedBy: -1, cappedBy: -1}
	// 压缩上界：只能压缩已应用的条目
	d.point = rf.lastApplied - catchUpEntries
	d.pinnedAt = d.point

	d.budget = rf.logBudgetBytes
	if d.budget <= 0 {
		d.budget = defaultLogBudgetBytes
	}
	d.overBudget = rf.logBytes > d.budget
	// 日志整体在预算内时 floor 退化为 lastIncludedIndex，也就是不构成约束——
	// 所以第三档不必再判一次是否超预算。
	d.floor = rf.budgetFloorLocked(d.budget)

	// matchIndex 仅在成为 leader 时分配；follower 上为 nil，此时无需 peer 约束。
	// 单节点时该循环为空，压缩仅受 lastApplied 约束。
	if rf.role != ROLE_LEADER || rf.matchIndex == nil {
		return d
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		m := rf.matchIndex[i]
		if m >= d.point {
			continue
		}
		if !rf.peerRecentlyActiveLocked(i) && d.overBudget {
			continue // 第二档：不活跃且超预算，不再为它保留
		}
		if m < d.floor {
			// 第三档：预算截住了它。它会因此落到压缩点之前，改由快照补齐。
			d.cappedBy, d.cappedAt = i, m
			m = d.floor
		}
		if m < d.point {
			d.point = m
			d.pinnedBy = i
		}
	}
	return d
}
