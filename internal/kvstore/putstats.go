package kvstore

import (
	"fmt"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
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
	applyCalls   atomic.Uint64 // 落库**调用**次数（批量后一次带多行）
	applyRows    atomic.Uint64 // 落库的总行数
}

// applyLoop 自身的耗时。**S4 占了写延迟的 89.6%，而此前只量了嵌套在里面的
// RocksDB 写（8.8µs），剩下 96% 没有任何度量**——三个候选（apply 循环串行、
// 复制链路、rf.mu 争用）谁是天花板全靠猜。
//
// 判据是 idle 与 busy 的**对比**，不是任何一个的绝对值：
//
//	idle 约等于 0、busy × 速率 ≈ 1   → 循环一直在忙，它**就是**天花板，
//	                                   所有请求排在它后面（它是单 goroutine）
//	idle 占大头                      → 循环大部分时间在等消息，**它被饿着**，
//	                                   瓶颈在上游的复制/提交，优化 apply 没用
//
// 没有 idle 这一项就分不出这两种，而它们的优化方向完全相反。
//
// 代价：每条多两次 time.Now()（约 50ns），相对每条 17.8µs 是 0.3%，可以接受；
// 这条路径上本来就有 recordPut / recordApplyStore 在做同样的事。
var applyLoopStats struct {
	iters      atomic.Uint64 // 循环转了多少圈（= 批数）
	entries    atomic.Uint64 // 这些圈里一共应用了多少条
	idleNs     atomic.Uint64 // 阻塞在 <-applyCh 上的时间
	lockWaitNs atomic.Uint64 // 等 kvs.mu
	busyNs     atomic.Uint64 // 取锁 + 应用一批 + 放锁，即"循环在干活"的时间
}

func recordApplyLoop(idle, lockWait, busy time.Duration, entries int) {
	applyLoopStats.iters.Add(1)
	applyLoopStats.entries.Add(uint64(entries))
	applyLoopStats.idleNs.Add(uint64(idle))
	applyLoopStats.lockWaitNs.Add(uint64(lockWait))
	applyLoopStats.busyNs.Add(uint64(busy))
}

// ApplyLoopStatsLine 输出 apply 循环的占空比。
//
// busy_share 是这一行的重点：它就是**单 goroutine 的 apply 循环的利用率**。
// 接近 100% 意味着它已经跑满，写吞吐不可能超过 1/avg_busy，再加并发只会排队。
func ApplyLoopStatsLine() string {
	n := applyLoopStats.iters.Load()
	if n == 0 {
		return "[APPLY-LOOP] 无数据"
	}
	e := applyLoopStats.entries.Load()
	ms := func(total uint64) float64 { return float64(total) / float64(n) / 1e6 }
	idle, lockWait, busy := ms(applyLoopStats.idleNs.Load()),
		ms(applyLoopStats.lockWaitNs.Load()), ms(applyLoopStats.busyNs.Load())
	share := 0.0
	if idle+busy > 0 {
		share = busy / (idle + busy) * 100
	}
	perBatch := 0.0
	if n > 0 {
		perBatch = float64(e) / float64(n)
	}
	// **串行上限必须按「条」算，不是按「批」算。** 批量之后一圈带多条，
	// 拿 1/avg_busy 当上限会把它低估 perBatch 倍——那个数字会看起来像
	// 优化把上限变低了，而实际正好相反。
	cap_ := 0.0
	if busy > 0 {
		cap_ = perBatch * 1000.0 / busy
	}
	return fmt.Sprintf(
		"[APPLY-LOOP] batches=%d entries=%d per_batch=%.1f avg_idle=%.4fms avg_lock_wait=%.4fms "+
			"avg_busy=%.4fms busy_share=%.1f%% serial_cap=%.0f entries/s",
		n, e, perBatch, idle, lockWait, busy, share, cap_)
}

func recordPut(handler, raftStart, commitWait time.Duration) {
	putStats.calls.Add(1)
	putStats.handlerNs.Add(uint64(handler))
	putStats.raftStartNs.Add(uint64(raftStart))
	putStats.commitWaitNs.Add(uint64(commitWait))
}

// storeWriteCalls 是落库调用次数，给测试用：批量化的收益全在"一批只落一次库"上，
// 而攒行写错（比如每条都 flush）不会有任何报错，只是收益消失。
func storeWriteCalls() uint64 { return putStats.applyCalls.Load() }

// recordApplyStore 记一次**落库调用**（批量之后一次可能带 n 行），
// 所以既记调用次数也记行数——只记调用次数的话，批量一上来这个数字会凭空变大
// 而看起来像变慢了。
func recordApplyStore(d time.Duration, rows int) {
	putStats.applyCalls.Add(1)
	putStats.applyRows.Add(uint64(rows))
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
	storeRows := 0.0
	if c := putStats.applyCalls.Load(); c > 0 {
		storeRows = float64(putStats.applyRows.Load()) / float64(c)
	}
	residual := handler - raftStart - commitWait

	pct := func(v float64) float64 {
		if handler <= 0 {
			return 0
		}
		return v / handler * 100
	}
	return fmt.Sprintf(
		"[PUT-BREAKDOWN] puts=%d handler=%.4fms | S1S2S3_raft_start=%.4fms(%.1f%%) S4_commit_wait=%.4fms(%.1f%%) residual=%.4fms(%.1f%%) | nested_store_write=%.4fms/次 rows_per_write=%.1f",
		n, handler,
		raftStart, pct(raftStart),
		commitWait, pct(commitWait),
		residual, pct(residual),
		applyStore, storeRows)
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
				fmt.Println(ApplyLoopStatsLine())
				fmt.Println(ApplyRaceStatsLine())
				fmt.Println(raft.RaftWriteStatsLine())
				fmt.Println(raft.GroupCommitStatsLine())
			}
		}
	}()
}

// applyRaceStats 记录一个竞态窗口的命中次数。
//
// StartPut 里 raft.Start 先分配 index 并把日志写下去，之后才把 opCtx 注册进
// reqMap。这两步之间 applyLoop 完全可能已经把这条 index 处理掉了——它查 reqMap
// 查不到，于是不会 close(opCtx.committed)。Put 本身照常写进存储引擎，只有唤醒
// 客户端的那一步丢了，请求于是一直挂到 commitTimeout。
//
// earlyApply 统计注册时发现 index 已被 apply 的次数，也就是这条路径救回来的请求数。
var applyRaceStats struct {
	earlyApply atomic.Uint64
}

func recordEarlyApply() { applyRaceStats.earlyApply.Add(1) }

func ApplyRaceStatsLine() string {
	return fmt.Sprintf("[APPLY-RACE] early_apply_rescued=%d", applyRaceStats.earlyApply.Load())
}
