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

func (kvs *KVServer) applyLoop() {
	for !kvs.killed() {
		msg := <-kvs.applyCh
		if !msg.CommandValid {
			continue
		}
		kvs.mu.Lock()
		// In lsm-raft mode a follower holds committed entries and ingests the leader's
		// SSTables instead of replaying them (lsmraft.go).
		if kvs.lsm == nil || !kvs.lsmHoldOrApply(msg) {
			kvs.applyCommand(msg)
		}
		kvs.mu.Unlock()
	}
}

// applyCommand applies one committed entry to the store and wakes the client waiting on
// it. Caller holds kvs.mu.
func (kvs *KVServer) applyCommand(msg raft.ApplyMsg) {
	// 与装快照互斥。**理由不是这条注释原来写的那个**：原先写的是"一条 apply 中途被换掉
	// 状态机就会把这一行写进新库、偏移却指着旧日志"，而那件事已经由 kvs.mu 挡住了——
	// applyCommand 的两个调用点都在 kvs.mu 之下（applyLoop 与 lsmReplayHeld），
	// 而装快照换指针的那一步也取 kvs.mu，所以换指针不可能插进一次 applyCommand 中间。
	//
	// 真正需要这把锁的是**整个安装窗口**：装快照很早就查过
	// `lastAppliedIndex >= sm.LastIndex`，然后要花几秒做落位 / ingest / 装分区组。
	// 这段窗口里若 apply 还能推进，安装最后把 lastAppliedIndex 重设成 sm.AppliedIndex
	// 就是**往回退**，而那几条已应用的数据在旧库里、随后会被删掉。
	// 所以这把读锁要保留；它很便宜（每条一次，不长期持有），装快照的写锁因此只等
	// "正在进行的一次 apply"。读路径已经不再整段持 stateMu 了，见 read.go 的 stateSnapshot。
	kvs.stateMu.RLock()
	defer kvs.stateMu.RUnlock()
	cmd := msg.Command
	index := msg.CommandIndex
	cmdTerm := msg.CommandTerm
	offset := msg.Offset
	// 更新已经应用到的日志
	kvs.lastAppliedIndex = index
	// fmt.Println("进入到applyLoop")
	// 操作日志
	op := cmd.(*raftrpc.DetailCod) // 操作在server端的PutAppend函数中已经调用Raft的Start函数，将请求以Op的形式存入日志。

	if op.OpType == "TermLog" { // 需要进行类型断言才能访问结构体的字段，如果是leader开始第一个Term时发起的空指令，则不用执行。
		kvs.persister.SetApplied(index) // no data for a no-op, but the applied index must advance or a restart replays it
		if kvs.lsm != nil {
			kvs.lsmAfterApply(index, "", nil)
		}
		return
	}

	opCtx, existOp := kvs.reqMap[index] // 检查当前index对应的等待put的请求是否超时，即是否还在等待被apply
	// prevSeq, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
	// _, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
	kvs.seqMap[op.ClientId] = op.SeqId // 更新服务器端，客户端请求的序列号
	// fmt.Printf("op:%v---index%v\n",existOp,index)
	if existOp { // 存在等待结果的apply日志的RPC, 那么判断状态是否与写入时一致，可能之前接受过该日志，但是身份不是leader了，该index对应的请求日志被别的leader同步日志时覆盖了。
		// 虽然没超时，但是如果已经和刚开始写入的请求不一致了，那也不行。
		if opCtx.op.Term != int32(cmdTerm) { //这里要用msg里面的CommandTerm而不是cmd里面的Term，因为当拿去到的是空指令时，其cmd里面的Term是0，会重复发生错误
			// fmt.Printf("这里有问题吗,opCtx.op.Term:%v,op.Term:%v\n",opCtx.op.Term,op.Term)
			opCtx.wrongLeader = true
		}
	}

	// 只处理ID单调递增的客户端写请求
	if op.OpType == OP_TYPE_PUT {
		// fmt.Printf("kaishiput")
		// if !existSeq || op.SeqId > prevSeq { // 如果是客户端第一次发请求，或者发生递增的请求ID，即比上次发来请求的序号大，那么接受它的变更
		// if !existSeq {	//	如果要改就是改这个了，就不管序号，直接先执行。
		// kvs.kvStore[op.Key] = op.Value		// ----------------------------------------------
		if op.SeqId%10000 == 0 {
			fmt.Println("底层执行了Put请求，以及重置put操作时间")
		}
		kvs.lastPutTime = time.Now() // 更新put操作时间

		// 将整数编码为字节流并存入 LevelDB
		// indexKey := make([]byte, 4)                            // 假设整数是 int32 类型
		// kvs.persister.Put(op.Key,indexKey)
		// binary.BigEndian.PutUint32(indexKey, uint32(op.Index)) // 这里注意是把op.Index放进去还是对应日志的entry.Command.Index，两者应该都一样
		// kvs.persister.Put(op.Key, indexKey)                    // <key,idnex>,其中index是string类型
		// addrs := kvs.raft.GetOffsets()		// 拿到raft层的offsets，这个可以优化用通道传输
		// addr := addrs[op.Index]
		// positionBytes := make([]byte, binary.MaxVarintLen64) // 相当于把地址（指向keysize开始处）压缩一下
		// n := binary.PutVarint(positionBytes, offset)
		// 只保留实际使用的字节
		// positionBytes = positionBytes[:n]
		// fmt.Printf("此时put进去的offsetL%v\n", offset)
		// fmt.Printf("转换后的offset：%v\n", positionBytes)

		tRocks := time.Now()
		if kvs.inlinePlacement && kvs.shouldInline(len(op.Value)) {
			// 小值直接落在存储引擎里，不进 valuelog：读路径因此缩短为一次点查，
			// 且 GC 无需再为它们做一次搬运。
			recordPlacement(len(op.Value), true)
			kvs.persister.PutInlineApplied(op.Key, op.Value, index)
		} else if !kvs.kvSeparation {
			// 基线：value 本身写进 RocksDB。于是同一份 value 被持久化两次
			// （Raft 日志 + LSM），而后还要被 compaction 反复搬运。
			kvs.persister.PutValueApplied(op.Key, op.Value, index)
			if kvs.lsm != nil {
				kvs.lsmAfterApply(index, op.Key, []byte(op.Value))
			}
		} else if int(msg.FileVersion) == kvs.numGC { // 对于写入日志时，又进行了 GC ，需将偏移量存新文件
			// 用 msg 带上来的版本，而不是命令自带的 op.FileVersion：
			// 后者在"决定写入"时记下，而 offset 在"实际写入"时才产生，
			// 两个时刻之间 GC 可能已经换过文件（切换走 logMu，拦不住持
			// rf.mu 的写入路径）。msg.FileVersion 与 offset 同源同锁，
			// 是唯一能保证配套的那个。
			recordPlacement(len(op.Value), false)
			kvs.persister.PutOffsetApplied(op.Key, offset, index) // row and applied index in one batch
		} else { // 否则存旧文件
			kvs.oldPersister.Put_opt(op.Key, offset) //  Nezha
			// Row in the old index, marker in the current one: two writes, not atomic.
			// Data first, marker second, so a crash in between only replays this entry
			// once on restart, and the replay is idempotent (same key, same offset).
			kvs.persister.SetApplied(index)
			// kvs.oldPersister.Put(op.Key, op.Value)		//  original
		}
		recordApplyStore(time.Since(tRocks))
	} else {
		// 走到这里说明日志里有一条 applyOne 不认识的 op。
		//
		// **不能 panic。** 这条条目已经被复制给了所有副本，于是每个节点都会在同一条上崩——
		// 一个请求打掉整个集群。原先这里是 OP_TYPE_GET 的处理分支（读经由 Raft 日志的老
		// 写法），里面两处 `panic(err)`；而读早就不走日志了（service.go 的 StartGet 直接
		// 应答），那段代码是死的，却因为写成 else 而接住了**一切**非 Put / 非 TermLog 的 op。
		// 入口现在会拒绝这种 op（见 PutInRaft），这里是第二道：日志里万一已经有了，
		// 要响亮地跳过而不是把节点带走。
		util.EPrintf("applyOne: 日志 index=%d 的 op 类型是 %q，本节点不认识，跳过（不写状态机）", index, op.OpType)
		if existOp {
			opCtx.keyExist = false
			opCtx.value = raft.NoKey
		}
	}

	// 唤醒挂起的RPC
	if existOp { // 如果等待apply的请求还没超时
		// fmt.Printf("666")
		close(opCtx.committed)
	}
}
