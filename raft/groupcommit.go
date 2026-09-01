package raft

import (
	"fmt"
	"sync"
	"time"
)

// Group commit：多条日志攒成一批，共用一次写入与一次 fsync。
//
// 为什么现在才值得做：开启 fsync 前，一次日志写入只要 6μs，攒批能省的那点开销
// 淹没在噪声里。开启 fsync 后单次写入变成 0.0711ms，而且它是**在持有 rf.mu 的
// 状态下**做的——实测锁等待随之从 0.223ms 涨到 1.606ms（×7.2）：一个客户端等
// 磁盘时，其余并发请求全堵在门外。攒批同时压掉两笔成本，一次 fsync 覆盖整批，
// 临界区也从 N 次磁盘等待缩成 1 次。
//
// 这正是"减少持久化写入次数"那个思路的延伸：从每条一次，变成每批一次。
//
// 能攒的只有写文件这一步。index 分配依赖 rf.log 的长度（lastIndex()+1），
// rf.log 的 append 必须留在 rf.mu 内，否则并发请求会拿到相同的 index。
//
// 语义不变：Start 仍然在日志落盘之后才返回。rf.log 会短暂领先于磁盘，
// 但那段窗口里客户端尚未收到成功响应，未确认的写入允许丢失，不违反 Raft。
type flushBatch struct {
	entries []*Entry
	done    chan struct{}
}

// enqueueForFlush 把 entry 挂进当前批次，返回该批次与"是否需要唤醒 flusher"。
// 调用方需持有 rf.mu。
func (rf *Raft) enqueueForFlush(e *Entry) (*flushBatch, bool) {
	rf.batchMu.Lock()
	defer rf.batchMu.Unlock()
	first := rf.curBatch == nil
	if first {
		rf.curBatch = &flushBatch{done: make(chan struct{})}
	}
	rf.curBatch.entries = append(rf.curBatch.entries, e)
	return rf.curBatch, first
}

// runFlusher 是唯一执行写入的 goroutine，保证批内顺序即入队顺序，
// 也就是 index 的升序——Offsets 依赖这个顺序。
func (rf *Raft) runFlusher() {
	for range rf.flushSignal {
		// 收集窗口：让同一时刻涌入的请求并进同一批。窗口越长批越大、
		// 单条延迟越高，是吞吐与延迟的直接权衡。
		if rf.batchWindow > 0 {
			time.Sleep(rf.batchWindow)
		}
		rf.batchMu.Lock()
		b := rf.curBatch
		rf.curBatch = nil
		rf.batchMu.Unlock()
		if b == nil || len(b.entries) == 0 {
			continue
		}
		rf.mu.Lock()
		rf.WriteEntryToFile(b.entries, 0) // 一次写入 + 一次 fsync 覆盖整批
		rf.mu.Unlock()
		recordBatch(len(b.entries))
		close(b.done)
	}
}

// EnableGroupCommit 启用攒批并启动 flusher。需在节点开始服务前调用。
func (rf *Raft) EnableGroupCommit(window time.Duration) {
	rf.batchMu.Lock()
	rf.groupCommit = true
	rf.batchWindow = window
	rf.batchMu.Unlock()
	rf.flushSignal = make(chan struct{}, 1024)
	go rf.runFlusher()
}

// ---- 批次统计 ----

var batchStats struct {
	mu      sync.Mutex
	batches uint64
	entries uint64
	maxSize int
}

func recordBatch(n int) {
	batchStats.mu.Lock()
	defer batchStats.mu.Unlock()
	batchStats.batches++
	batchStats.entries += uint64(n)
	if n > batchStats.maxSize {
		batchStats.maxSize = n
	}
}

// GroupCommitStatsLine 报告平均批大小——它直接决定省掉了多少次 fsync。
// 平均 1.0 表示攒批没起作用（窗口太短或并发不足），收益也就无从谈起。
func GroupCommitStatsLine() string {
	batchStats.mu.Lock()
	defer batchStats.mu.Unlock()
	if batchStats.batches == 0 {
		return "[GROUP-COMMIT] 未启用或无数据"
	}
	avg := float64(batchStats.entries) / float64(batchStats.batches)
	return fmt.Sprintf("[GROUP-COMMIT] batches=%d entries=%d avg_batch=%.2f max_batch=%d fsync_saved=%d",
		batchStats.batches, batchStats.entries, avg, batchStats.maxSize,
		batchStats.entries-batchStats.batches)
}
