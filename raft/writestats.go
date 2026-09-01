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
	writeFileNs atomic.Uint64 // WriteEntryToFile 的时间
	inLockNs    atomic.Uint64 // 持锁总时间（含写文件）
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
	return fmt.Sprintf(
		"[RAFT-WRITE] calls=%d avg_lock_wait=%.4fms avg_write_file=%.4fms avg_in_lock=%.4fms lock_wait_share=%.1f%%",
		n, lock, file, held, lock/(lock+held)*100)
}
