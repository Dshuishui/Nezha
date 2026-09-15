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

// estimatedEntryBytes 把条数换算成 MB，是一个**下界**：条目本身（protobuf 三层
// []*LogEntry + LogEntry + DetailCod）约 216 B 且与 value 无关，再加 value 本身，
// 所以 64B value 约 280 B、256B value 约 472 B。取 280 意味着大 value 下会低估——
// 小规模实测 150000 条 × 256B，RSS 涨 139MB 而此式只给出 38MB。
// 不按实际条目求和：那是 O(n)，而 n 可以到十亿级，压缩循环持着 rf.mu 走不起。
const estimatedEntryBytes = 280

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

		// 压缩上界：只能压缩已应用的条目
		safeIndex := rf.lastApplied - catchUpEntries

		// 且不能压缩掉任何 follower 尚未复制的条目，否则它再也追不上——doAppendEntries
		// 在 nextIndex 落进已压缩区间时只能跳过本轮，而且每一轮都会同样地跳过，那个
		// follower 就**永久卡住**（补它需要 InstallSnapshot，本实现没有）。
		// matchIndex 仅在成为 leader 时分配；follower 上为 nil，此时无需该约束。
		// 单节点时该循环为空，压缩仅受 lastApplied 约束。
		pinnedBy, pinnedAt := -1, safeIndex
		if rf.role == ROLE_LEADER && rf.matchIndex != nil {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] < safeIndex {
					safeIndex = rf.matchIndex[i]
					pinnedBy = i
				}
			}
		}
		// 有 follower 把压缩点按住时要说出来。
		//
		// 这不是修复，是让一个原本静默的失效模式变得可诊断：压缩上界被 matchIndex 钉住
		// 意味着 rf.log 会一直涨，而每条 LogEntry 在 64B value 下约 280 字节。
		// 100GB / 64B ≈ 11.4 亿条，follower 落后 10% 就是约 32GB 常驻内存——进程会被
		// OOM 杀掉，而在此之前没有任何一行日志提到过原因。
		//
		// 真正的修法是 InstallSnapshot：压缩点可以越过落后的 follower，再用状态快照把它
		// 补齐。那是一个未实现的特性，不是这里能顺手补的。
		if pinnedBy >= 0 && pinnedAt-safeIndex >= logPinWarnEntries {
			// 报**实际驻留的条数** `len(rf.log)`，而不只是"压缩点被推回了多少"：
			// 被按住期间根本不会发生压缩（下面的 safeIndex <= lastIncludedIndex 会 continue），
			// 所以真正占着内存的是整个 rf.log，而"推回了多少"只是它的一个下界。
			//
			// MB 是**下界估算**，按 64B value 的 280 B/条算。value 越大越低估：
			// 条目本身（protobuf 三层）约 216 B 与 value 无关，再加上 value 本身，
			// 256B value 时一条就是约 472 B。小规模实测（150000 条 × 256B）RSS 涨了
			// 139MB，而按 280 B/条估出来只有 38MB——所以这个数只能当"至少这么多"读。
			fmt.Printf("[LOG-PINNED] peer[%d] 只复制到 %d，压缩点被从 %d 按到 %d："+
				"内存日志驻留 %d 条（下界估算 ≥%dMB，value 越大越低估）"+
				"——随它的落后程度线性增长，补齐它需要 InstallSnapshot（未实现）\n",
				pinnedBy, rf.matchIndex[pinnedBy], pinnedAt, safeIndex,
				len(rf.log), int64(len(rf.log))*estimatedEntryBytes>>20)
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

		// 关键：必须 make + copy 重新分配。
		// rf.log = rf.log[pos+1:] 只是移动切片头指针，底层数组仍被引用、内存不会释放。
		newLog := make([]*raftrpc.LogEntry, len(rf.log)-pos-1)
		copy(newLog, rf.log[pos+1:])
		rf.log = newLog

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
