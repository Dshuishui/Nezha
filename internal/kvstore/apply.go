// The apply loop: committed Raft entries become store rows and wake the waiting client.

package kvstore

import (
	"fmt"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
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
		if kvs.inlinePlacement && len(op.Value) < kvs.inlineThreshold {
			// 小值直接落在存储引擎里，不进 valuelog：读路径因此缩短为一次点查，
			// 且 GC 无需再为它们做一次搬运。
			recordPlacement(len(op.Value), true)
			kvs.persister.PutInlineApplied(op.Key, op.Value, index)
		} else if !kvs.kvSeparation {
			// 基线：value 本身写进 RocksDB。于是同一份 value 被持久化两次
			// （Raft 日志 + LSM），而后还要被 compaction 反复搬运。
			kvs.persister.PutValueApplied(op.Key, op.Value, index)
			if kvs.lsm != nil {
				kvs.lsmAfterApply(index, kvs.persister.PadKey(op.Key), []byte(op.Value))
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
	} else { // OP_TYPE_GET
		if existOp { // 如果是GET请求，只要没超时，都可以进行幂等处理
			// opCtx.value, opCtx.keyExist = kvs.kvStore[op.Key]	// --------------------------------------------
			// value := kvs.persister.Get(op.Key)		leveldb拿取value

			// 从 LevelDB 中获取键对应的值，并解码为整数
			positionBytes, err := kvs.persister.Get_opt(op.Key)
			if err != nil {
				fmt.Println("拿取value有问题")
				panic(err)
			}
			// positionBytes := kvs.persister.Get(op.Key)
			// position, _ := binary.Varint(positionBytes) // 将字节流解码为整数，拿到key对应的index
			if positionBytes == -1 { //  说明leveldb中没有该key
				opCtx.keyExist = false
				opCtx.value = raft.NoKey
			} else {
				_, value, err := kvs.raft.ReadValueFromFile(kvs.currentLog, positionBytes)
				if err != nil {
					fmt.Println("拿取value有问题")
					panic(err)
				}
				opCtx.value = value
			}
		}
	}

	// 唤醒挂起的RPC
	if existOp { // 如果等待apply的请求还没超时
		// fmt.Printf("666")
		close(opCtx.committed)
	}
}
