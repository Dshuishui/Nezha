package raft

import (
	"fmt"
	"sync/atomic"
	"time"
)

// 写入路径的耗时分解。
//
// 起因：把每条日志的 OpenFile+Seek+Close 去掉之后，小 value 的写入吞吐纹丝不动
// （0.1491 → 0.1388 MB/S，落在 ±7% 的测量噪声里）。"每条五次系统调用"这个假设
// 因此被证伪，但真正的瓶颈在哪并不知道。
//
// 与其继续拿改动去试，不如直接量：一条 Put 在 Raft 侧的时间分成等锁和写文件两段，
// 谁占大头一眼可见。50 个并发客户端全部串行在 rf.mu 上，锁等待是首要嫌疑。
var writeStats struct {
	calls       atomic.Uint64
	lockWaitNs  atomic.Uint64 // 等 rf.mu 的时间
	writeFileNs atomic.Uint64 // writeEntries（追加/覆盖日志文件）的时间
	inLockNs    atomic.Uint64 // 持锁总时间（含写文件）

	// 日志文件被写了多少次。每次都是一次 Flush（syncOnWrite 下还有一次 fsync），
	// 所以它与"写了多少条"的比值就是攒批的实际效果。follower 侧曾经是 1:1
	// ——代码写着攒批，判据却恒为真，每条一次 fsync。
	logWrites  atomic.Uint64
	logEntries atomic.Uint64
}

// recordLogWrite 记一次日志文件写入及其携带的条数。
func recordLogWrite(entries int) {
	writeStats.logWrites.Add(1)
	writeStats.logEntries.Add(uint64(entries))
}

// LogWriteBatching 返回日志文件的写入次数、写入条数，以及平均每次带多少条。
func LogWriteBatching() (writes, entries uint64, perWrite float64) {
	writes = writeStats.logWrites.Load()
	entries = writeStats.logEntries.Load()
	if writes > 0 {
		perWrite = float64(entries) / float64(writes)
	}
	return
}

func recordWrite(lockWait, writeFile, inLock time.Duration) {
	writeStats.calls.Add(1)
	writeStats.lockWaitNs.Add(uint64(lockWait))
	writeStats.writeFileNs.Add(uint64(writeFile))
	writeStats.inLockNs.Add(uint64(inLock))
}

// RaftWriteStatsLine 给出每条 Put 在 Raft 侧的平均耗时分解。
// 锁等待远大于写文件，说明瓶颈是并发争用而非 I/O——那样再优化文件操作也是徒劳。
func RaftWriteStatsLine() string {
	n := writeStats.calls.Load()
	if n == 0 {
		return "[RAFT-WRITE] 无数据"
	}
	avg := func(total uint64) float64 {
		return float64(total) / float64(n) / 1e6 // ns -> ms
	}
	lock := avg(writeStats.lockWaitNs.Load())
	file := avg(writeStats.writeFileNs.Load())
	held := avg(writeStats.inLockNs.Load())
	writes, entries, perWrite := LogWriteBatching()
	return fmt.Sprintf(
		"[RAFT-WRITE] calls=%d avg_lock_wait=%.4fms avg_write_file=%.4fms avg_in_lock=%.4fms lock_wait_share=%.1f%% "+
			"log_writes=%d log_entries=%d entries_per_write=%.2f",
		n, lock, file, held, lock/(lock+held)*100, writes, entries, perWrite)
}
