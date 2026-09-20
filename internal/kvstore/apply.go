// The apply loop: committed Raft entries become store rows and wake the waiting client.

package kvstore

import (
	"fmt"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        *raftrpc.DetailCod
	committed chan byte

	wrongLeader bool // 因为index位置log的term不一致, 说明leader换过了
	ignored     bool // 因为req id过期, 表示已经执行过，该日志需要被跳过

	// Get操作的结果
	keyExist bool
	value    string
}

func newOpContext(op *raftrpc.DetailCod) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

// maxApplyBatch 是一批最多带多少条。
//
// 上限的作用不是提高吞吐（批到几十条时每条的固定开销就已经摊薄了），而是**封住
// 两个会被批长度拉长的窗口**：一是 kvs.mu 被连续持有的时长（读路径的 captureState
// 要取它），二是 stateMu 读锁的持有时长（装快照要取它的写锁，见 applyBatch 的注释）。
// 256 条 × 每条约 10µs 的实际写入 ≈ 2.5ms，与单次扫描动辄几秒相比可以忽略。
const maxApplyBatch = 256

// applyLoop 把**已经排在 applyCh 里**的条目一次取走一批，一批只写一次存储引擎。
//
// 为什么要批：2026-09-20 的并发扫描实测，写吞吐在并发 100→400 之间完全压平
// （55,672 / 55,488 / 55,437 ops/s，差 0.4%），而这个循环是单 goroutine，
// 它的 1/avg_busy = 57~58K entries/s 正好就是那条天花板（实测占其 95~96%）。
// 每条 17.1µs 里，9.1µs 是一次 RocksDB db.Write，6.5µs 是等 kvs.mu。
//
// **这个批不等待。** 只把通道里已经到的取走，取空立刻走，绝不为凑满而 sleep。
// 于是低负载下批大小就是 1、延迟一点不变；高负载下批自然长起来，正是需要它的时候。
// 这与 group commit 是两回事——后者开一个定时窗口来摊薄 fsync，会凭空加延迟，
// 所以它只在 -syncWAL 下才启用（server.go:370）。这里没有窗口，也就没有这个代价。
func (kvs *KVServer) applyLoop() {
	batch := make([]raft.ApplyMsg, 0, maxApplyBatch)
	for !kvs.killed() {
		// **量 idle 与 busy 两头才能分清这个循环是天花板还是被饿着。**
		// 判据与代价见 putstats.go 里 applyLoopStats 那段。
		tIdle := time.Now()
		msg := <-kvs.applyCh
		idle := time.Since(tIdle)
		batch = batch[:0]
		if msg.CommandValid {
			batch = append(batch, msg)
		}
		// 机会性抽干：只取已经到的。default 一命中就停，不等。
		for drained := false; !drained && len(batch) < maxApplyBatch; {
			select {
			case m := <-kvs.applyCh:
				if m.CommandValid {
					batch = append(batch, m)
				}
			default:
				drained = true
			}
		}
		if len(batch) == 0 {
			continue
		}
		tLock := time.Now()
		kvs.mu.Lock()
		lockWait := time.Since(tLock)
		// In lsm-raft mode a follower holds committed entries and ingests the leader's
		// SSTables instead of replaying them (lsmraft.go).
		//
		// **lsm 模式下不能批。** lsmHoldOrApply 要逐条决定"扣住还是应用"，
		// 而它的判定依赖前一条的处理结果（held 队列与 lastAppliedIndex）。
		// 那个模式不在被测的四个系统里，保持逐条是最省事也最安全的。
		if kvs.lsm != nil {
			for i := range batch {
				if !kvs.lsmHoldOrApply(batch[i]) {
					kvs.applyCommand(batch[i])
				}
			}
		} else {
			kvs.applyBatch(batch)
		}
		kvs.mu.Unlock()
		recordApplyLoop(idle, lockWait, time.Since(tLock), len(batch))
	}
}

// applyCommand applies one committed entry to the store and wakes the client waiting on
// it. Caller holds kvs.mu.
//
// 它就是 applyBatch 的单条特例，**两者共用一份实现**。lsm 模式的 lsmReplayHeld
// 逐条调它，行为与批量路径完全一致。
// （同一段逻辑写两遍这件事这个仓库已经栽过：读路径上同一个七行块抄了七处，
// 修一处漏六处。）
func (kvs *KVServer) applyCommand(msg raft.ApplyMsg) {
	kvs.applyBatch([]raft.ApplyMsg{msg})
}

// applyBatch 应用一批已提交的条目：所有写进**当前库**的行攒成一个 WriteBatch，
// 连同**一个** applied 标记落库，然后才唤醒等在这些条目上的客户端。
// 调用方持有 kvs.mu。批大小为 1 时与逐条应用完全等价。
func (kvs *KVServer) applyBatch(msgs []raft.ApplyMsg) {
	// 与装快照互斥。**理由不是这条注释原来写的那个**：原先写的是"一条 apply 中途被换掉
	// 状态机就会把这一行写进新库、偏移却指着旧日志"，而那件事已经由 kvs.mu 挡住了——
	// applyBatch 的调用点都在 kvs.mu 之下（applyLoop 与 lsmReplayHeld），
	// 而装快照换指针的那一步也取 kvs.mu，所以换指针不可能插进一批中间。
	//
	// 真正需要这把锁的是**整个安装窗口**：装快照很早就查过
	// `lastAppliedIndex >= sm.LastIndex`，然后要花几秒做落位 / ingest / 装分区组。
	// 这段窗口里若 apply 还能推进，安装最后把 lastAppliedIndex 重设成 sm.AppliedIndex
	// 就是**往回退**，而那几条已应用的数据在旧库里、随后会被删掉。
	// 所以这把读锁要保留。批量之后它的持有时长从"一条"变成"一批"，
	// 由 maxApplyBatch 封住（理由见那里）。
	kvs.stateMu.RLock()
	defer kvs.stateMu.RUnlock()

	rows := make([]raft.StoreRow, 0, len(msgs))
	// 等着被唤醒的客户端。**必须等这一批落库之后再唤醒**：客户端拿到 OK 之后的读
	// 走 leader 本地的库（租约读），先唤醒再落库会让它读不到自己刚写的值。
	wake := make([]*OpContext, 0, len(msgs))
	applied := 0
	pending := false // 自上次落库以来有没有需要写的东西

	flushRows := func() {
		if !pending {
			return
		}
		tRocks := time.Now()
		if err := kvs.persister.WriteRowsApplied(rows, applied); err != nil {
			util.EPrintf("applyBatch: %d 行连同 applied=%d 落库失败: %v", len(rows), applied, err)
		}
		recordApplyStore(time.Since(tRocks), len(rows))
		rows = rows[:0]
		pending = false
	}

	for i := range msgs {
		msg := msgs[i]
		index := msg.CommandIndex
		// 更新已经应用到的日志
		kvs.lastAppliedIndex = index
		// 操作日志
		op := msg.Command.(*raftrpc.DetailCod)

		if op.OpType == "TermLog" { // leader 开始一个 Term 时的空指令，没有数据要写
			// no data for a no-op, but the applied index must advance or a restart replays it
			applied = index
			pending = true
			if kvs.lsm != nil {
				// lsmAfterApply 记录的是"已经落库的状态"，所以先把批写掉。
				flushRows()
				kvs.lsmAfterApply(index, "", nil)
			}
			continue
		}

		opCtx, existOp := kvs.reqMap[index] // 检查当前index对应的等待put的请求是否超时，即是否还在等待被apply
		kvs.seqMap[op.ClientId] = op.SeqId  // 更新服务器端，客户端请求的序列号
		if existOp {
			// 虽然没超时，但如果已经和刚开始写入的请求不一致了，那也不行：可能之前接受过
			// 该日志，但身份不是 leader 了，该 index 对应的请求日志被别的 leader 覆盖了。
			// 这里要用 msg 的 CommandTerm 而不是 cmd 里的 Term——空指令的 cmd.Term 是 0。
			if opCtx.op.Term != int32(msg.CommandTerm) {
				opCtx.wrongLeader = true
			}
		}

		// 只处理ID单调递增的客户端写请求
		if op.OpType == OP_TYPE_PUT {
			if op.SeqId%10000 == 0 {
				fmt.Println("底层执行了Put请求，以及重置put操作时间")
			}
			kvs.lastPutTime = time.Now() // 更新put操作时间
			switch {
			case kvs.inlinePlacement && kvs.shouldInline(len(op.Value)):
				// 小值直接落在存储引擎里，不进 valuelog：读路径因此缩短为一次点查，
				// 且 GC 无需再为它们做一次搬运。
				recordPlacement(len(op.Value), true)
				rows = append(rows, raft.EncodeInlineRow(op.Key, op.Value))
			case !kvs.kvSeparation:
				// 基线：value 本身写进 RocksDB。于是同一份 value 被持久化两次
				// （Raft 日志 + LSM），而后还要被 compaction 反复搬运。
				rows = append(rows, raft.EncodeValueRow(op.Key, op.Value))
				if kvs.lsm != nil {
					flushRows() // 同 TermLog 那处：lsmAfterApply 要看到已落库的状态
					kvs.lsmAfterApply(index, op.Key, []byte(op.Value))
				}
			case int(msg.FileVersion) == kvs.numGC:
				// 对于写入日志时又进行了 GC 的条目，需将偏移量存新文件。
				// 用 msg 带上来的版本，而不是命令自带的 op.FileVersion：
				// 后者在"决定写入"时记下，而 offset 在"实际写入"时才产生，
				// 两个时刻之间 GC 可能已经换过文件（切换走 logMu，拦不住持
				// rf.mu 的写入路径）。msg.FileVersion 与 offset 同源同锁，
				// 是唯一能保证配套的那个。
				recordPlacement(len(op.Value), false)
				rows = append(rows, raft.EncodeOffsetRow(op.Key, msg.Offset))
			default:
				// 否则存旧文件。**旧库是另一个存储引擎，没法与当前批同批**，
				// 所以先把当前批写掉，再单独写它。
				// Row in the old index, marker in the current one: two writes, not atomic.
				// Data first, marker second, so a crash in between only replays this entry
				// once on restart, and the replay is idempotent (same key, same offset).
				flushRows()
				kvs.oldPersister.Put_opt(op.Key, msg.Offset)
			}
			applied = index
			pending = true
		} else {
			// 走到这里说明日志里有一条本节点不认识的 op。
			//
			// **不能 panic。** 这条条目已经被复制给了所有副本，于是每个节点都会在同一条上崩——
			// 一个请求打掉整个集群。入口现在会拒绝这种 op（见 PutInRaft），这里是第二道：
			// 日志里万一已经有了，要响亮地跳过而不是把节点带走。
			util.EPrintf("applyBatch: 日志 index=%d 的 op 类型是 %q，本节点不认识，跳过（不写状态机）", index, op.OpType)
			if existOp {
				opCtx.keyExist = false
				opCtx.value = raft.NoKey
			}
		}

		if existOp {
			wake = append(wake, opCtx)
		}
	}

	flushRows()
	// 唤醒挂起的RPC——**在落库之后**，理由见 wake 的声明处。
	for _, c := range wake {
		close(c.committed)
	}
}
