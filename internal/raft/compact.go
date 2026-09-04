package raft

import (
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

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

		// 且不能压缩掉任何 follower 尚未复制的条目，否则它再也追不上。
		// matchIndex 仅在成为 leader 时分配；follower 上为 nil，此时无需该约束。
		// 单节点时该循环为空，压缩仅受 lastApplied 约束。
		if rf.role == ROLE_LEADER && rf.matchIndex != nil {
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] < safeIndex {
					safeIndex = rf.matchIndex[i]
				}
			}
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
