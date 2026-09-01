package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/raft"
)

// 一条 Put 的端到端耗时分解。
//
// 客户端量到的 PUT 延迟约 16.7ms，但这段时间花在哪一直是黑盒。去掉每条日志的
// OpenFile/Seek/Close 之后吞吐没有变化，说明文件操作不是瓶颈——那就必须把
// 剩下的路径拆开看，而不是继续猜。
//
// StartPut 的结构是两段：
//
//	raft.Start()          写日志文件，全程持有 rf.mu
//	<-opCtx.committed     等 apply 回调，其中含 RocksDB 写入与 goroutine 调度
//
// 后者是不是大头，直接决定后续该优化 Raft 侧还是存储侧。
var putStats struct {
	calls        atomic.Uint64
	raftStartNs  atomic.Uint64 // raft.Start 的耗时（写日志 + 等锁）
	commitWaitNs atomic.Uint64 // 等 apply 回调的耗时
	rocksPutNs   atomic.Uint64 // apply 中 RocksDB 写入的耗时
	rocksCalls   atomic.Uint64
}

func recordPut(raftStart, commitWait time.Duration) {
	putStats.calls.Add(1)
	putStats.raftStartNs.Add(uint64(raftStart))
	putStats.commitWaitNs.Add(uint64(commitWait))
}

func recordRocksPut(d time.Duration) {
	putStats.rocksCalls.Add(1)
	putStats.rocksPutNs.Add(uint64(d))
}

// PutStatsLine 给出平均分解。三段之和应接近客户端量到的 PUT 延迟；
// 差额即为 gRPC 与其余框架开销。
func PutStatsLine() string {
	n := putStats.calls.Load()
	if n == 0 {
		return "[PUT-STATS] 无数据"
	}
	ms := func(total, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count) / 1e6
	}
	rs := ms(putStats.raftStartNs.Load(), n)
	cw := ms(putStats.commitWaitNs.Load(), n)
	rp := ms(putStats.rocksPutNs.Load(), putStats.rocksCalls.Load())
	return fmt.Sprintf(
		"[PUT-STATS] puts=%d avg_raft_start=%.4fms avg_commit_wait=%.4fms avg_rocksdb_put=%.4fms total=%.4fms",
		n, rs, cw, rp, rs+cw)
}

// StartWriteStatsReporter 周期性把写入路径的分解打进节点日志。
// 与 AVP 指标同样放在后台：在热路径上做格式化会污染要测的延迟本身。
func StartWriteStatsReporter(interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	go func() {
		for range time.Tick(interval) {
			if putStats.calls.Load() > 0 {
				fmt.Println(PutStatsLine())
				fmt.Println(raft.RaftWriteStatsLine())
			}
		}
	}()
}
