package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/raft"
)

// 写入路径的耗时分解。
//
// 阶段是照着实际代码路径切的，不是照搬源码里那三处被注释掉的 T1/T2/T4 探针——
// 那三个点只标了孤立的位置，既不覆盖整条路径，也无从判断有没有漏测。
//
// PutInRaft 收到请求后的实际流程：
//
//	StartPut
//	  ├─ raft.Start(op)                     ← S1+S2+S3，全程持 rf.mu
//	  │    ├─ 等 rf.mu                        S1  并发争用
//	  │    ├─ WriteEntryToFile                S2  编码 + write + flush
//	  │    └─ 分配 index、append 内存日志      S3  持锁内的其余部分
//	  ├─ 注册 opCtx 到 reqMap
//	  └─ <-opCtx.committed                  ← S4 等 apply 完成
//	                                             ApplyLoop 在另一个 goroutine 里
//	                                             写 RocksDB 后 close 这个通道
//
// 单节点下没有向 follower 分发这一步，所以 S4 里不含网络共识，只有调度延迟
// 加 RocksDB 写入。
//
// 关键是 residual：handler 总时长减去各阶段之和。分解如果不完整，残差就会顶起来，
// 而不是被悄悄摊进某个阶段里——没有这一项，任何"XX 占了 NN%"的结论都不可信。
var putStats struct {
	calls        atomic.Uint64
	handlerNs    atomic.Uint64 // StartPut 全程（不含 gRPC 收发）
	raftStartNs  atomic.Uint64 // S1+S2+S3：raft.Start 全程
	commitWaitNs atomic.Uint64 // S4：等 apply 回调
	applyStoreNs atomic.Uint64 // RocksDB 写入，嵌套在 S4 内部，不参与求和
	applyCalls   atomic.Uint64
}

func recordPut(handler, raftStart, commitWait time.Duration) {
	putStats.calls.Add(1)
	putStats.handlerNs.Add(uint64(handler))
	putStats.raftStartNs.Add(uint64(raftStart))
	putStats.commitWaitNs.Add(uint64(commitWait))
}

func recordApplyStore(d time.Duration) {
	putStats.applyCalls.Add(1)
	putStats.applyStoreNs.Add(uint64(d))
}

// PutStatsLine 输出平均分解。
// 第一行是 handler 级的划分，第二行是 raft.Start 内部的细分，
// 第三行是嵌套在 S4 里的 RocksDB 写入——它与 S4 重叠，不能与其他项相加。
func PutStatsLine() string {
	n := putStats.calls.Load()
	if n == 0 {
		return "[PUT-BREAKDOWN] 无数据"
	}
	ms := func(total, count uint64) float64 {
		if count == 0 {
			return 0
		}
		return float64(total) / float64(count) / 1e6
	}
	handler := ms(putStats.handlerNs.Load(), n)
	raftStart := ms(putStats.raftStartNs.Load(), n)
	commitWait := ms(putStats.commitWaitNs.Load(), n)
	applyStore := ms(putStats.applyStoreNs.Load(), putStats.applyCalls.Load())
	residual := handler - raftStart - commitWait

	pct := func(v float64) float64 {
		if handler <= 0 {
			return 0
		}
		return v / handler * 100
	}
	return fmt.Sprintf(
		"[PUT-BREAKDOWN] puts=%d handler=%.4fms | S1S2S3_raft_start=%.4fms(%.1f%%) S4_commit_wait=%.4fms(%.1f%%) residual=%.4fms(%.1f%%) | nested_apply_rocksdb=%.4fms",
		n, handler,
		raftStart, pct(raftStart),
		commitWait, pct(commitWait),
		residual, pct(residual),
		applyStore)
}

// StartWriteStatsReporter 周期性把分解打进节点日志。
// 与 AVP 指标一样放在后台：在热路径上做格式化会污染要测的延迟本身。
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
