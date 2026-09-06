// The client-facing gRPC service (kvrpc): Put goes through Raft, Get and Scan are served
// from the local store and value files.

package kvstore

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

func (kvs *KVServer) ScanRangeInRaft(ctx context.Context, in *kvrpc.ScanRangeRequest) (*kvrpc.ScanRangeResponse, error) {
	reply := &kvrpc.ScanRangeResponse{Err: raft.OK}

	// commitIndex, isLeader := kvs.raft.GetReadIndex()
	// if !isLeader {
	// 	reply.Err = raft.ErrWrongLeader
	// 	reply.LeaderId = kvs.raft.GetLeaderId()
	// 	return reply, nil
	// }

	// for {
	// 	if kvs.raft.GetApplyIndex() >= commitIndex {
	if kvs.FirstGC {
		result, err := kvs.firstGCScan(in.StartKey, in.EndKey)
		if err != nil {
			reply.Err = "error in scan"
			return reply, nil
		}
		reply.KeyValuePairs = result
		return reply, nil
	}
	result, err := kvs.anotherGCScan(in.StartKey, in.EndKey)
	if err != nil {
		reply.Err = "error in scan"
		return reply, nil
	}
	reply.KeyValuePairs = result
	return reply, nil

	// }
	// 	time.Sleep(6 * time.Millisecond) // 等待applyindex赶上commitindex
	// }
	// ————以下是之前的scan查询————
	// reply := kvs.StartScan(in)
	// 检查是否已经垃圾回收完毕
	// 垃圾回收完毕再调用在已排序文件的scan方法，范围查询结果，最好用goroutine，两者同时进行scan查询
	// 如果垃圾回收没完，需要调用在旧未排序的文件，进行范围查询
	// 还有一个比较复杂的情况，针对已排序文件，继已排序文件后的新文件，以及前两者即将合并时又生成的新文件。
	// 这三个文件就比较复杂，需要在最新文件、新文件、已排序的文件同时查询。
	// 后面再合并两者的结果，或者合并三者的结果
	// 返回即可
	// if reply.Err == raft.ErrWrongLeader {
	// reply.LeaderId = kvs.raft.GetLeaderId()
	// } else if reply.Err == raft.ErrNoKey {
	// 返回客户端没有该key即可，这里先不做操作
	// fmt.Println("server端没有client查询的key")
	// } else if reply.Err == "error in scan" {
	// reply.Err = "error in scan"
	// }
	// return reply, nil
}

// scanResult carries one branch's partial scan output together with its error.
type scanResult struct {
	data map[string]string
	err  error
}

// scanResultOf converts a StartScan_opt reply into a scanResult.
//
// Every caller used to hard-code `err: nil` and drop reply.Err on the floor, so a
// failed scan was indistinguishable from a scan that legitimately matched nothing:
// the caller merged an empty map and reported success. That is how the broken
// scanNewFile decoding stayed hidden for so long — it returned no rows and no error.
func scanResultOf(reply *kvrpc.ScanRangeResponse) scanResult {
	if reply == nil {
		return scanResult{err: errors.New("scan returned no reply")}
	}
	if reply.Err != raft.OK {
		return scanResult{err: fmt.Errorf("scan failed: %s", reply.Err)}
	}
	return scanResult{data: reply.KeyValuePairs}
}

func (kvs *KVServer) StartScan_opt(args *kvrpc.ScanRangeRequest, persister *raft.Persister, logLocation string) *kvrpc.ScanRangeResponse {
	startKey := args.GetStartKey()
	endKey := args.GetEndKey()
	reply := &kvrpc.ScanRangeResponse{Err: raft.OK}

	// 执行范围查询
	result, err := kvs.scanNewFile(startKey, endKey, persister, logLocation)
	if err != nil {
		log.Printf("Scan error: %v", err)
		reply.Err = "error in scan"
		return reply
	}

	// 构造响应并返回
	reply.KeyValuePairs = result
	return reply
}

func (kvs *KVServer) StartGet(args *kvrpc.GetInRaftRequest) *kvrpc.GetInRaftResponse {
	reply := &kvrpc.GetInRaftResponse{Err: raft.OK}
	key := args.GetKey()
	if !kvs.kvSeparation {
		// 基线：value 就在 RocksDB 里，一次点查即可，既不查偏移也不读日志文件。
		// GC 那套多路查找在这条路径上没有意义——基线没有 valuelog 需要回收。
		value, err := kvs.persister.Get(key)
		if err != nil || value == raft.ErrNoKey {
			reply.Err = raft.ErrNoKey
			reply.Value = raft.NoKey
			return reply
		}
		reply.Value = value
		return reply
	}
	if kvs.inlinePlacement {
		// 小值内联时一次点查就拿到 value，省去"查偏移 + 读日志文件"的第二次 I/O。
		// 不是内联的 key 会落回下面的多路查找，大 value 的路径完全不变。
		if v, ok := kvs.persister.GetInline(key); ok {
			reply.Value = v
			return reply
		}
	}
	if kvs.FirstGC { // 未开始第二轮GC
		reply = kvs.firstGCGet(key, reply)
		return reply
	}
	reply = kvs.anotherGCGet(key, reply)
	return reply
	// }
	// time.Sleep(6 * time.Millisecond) // 等待applyindex赶上commitindex
	// }
}

func (kvs *KVServer) GetInRaft(ctx context.Context, in *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	reply := kvs.StartGet(in)
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	} else if reply.Err == raft.ErrNoKey {
		// 全部查找路径都没找到，这才是真正的"键不存在"。
		// 读的键空间大于实际写入量时，这类请求注定 miss，必须从缓存命中率里剔除，
		// 否则命中率反映的是负载怎么配的，而不是 AVP 好不好。
		avpRecordNotFound()
		// 返回客户端没有该key即可，这里先不做操作
	}
	return reply, nil
}

func (kvs *KVServer) PutInRaft(ctx context.Context, in *kvrpc.PutInRaftRequest) (*kvrpc.PutInRaftResponse, error) {
	// fmt.Println("走到了server端的put函数"
	// startTime := time.Now() // 总开始时间
	reply := kvs.StartPut(in)
	// endTime := time.Now() // 总结束时间
	// fmt.Printf("执行总时间：%v", endTime.Sub(startTime))
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	}
	return reply, nil

	// 创建一个用于接收处理结果的通道
	// resultCh := make(chan *kvrpc.PutInRaftResponse)
	// // 在 goroutine 中处理请求
	// go func() {
	// // 处理请求的逻辑...
	// // 这里可以根据具体的业务逻辑来处理客户端请求并将其发送到 Raft 集群中

	// // 处理完成后，将结果发送到通道
	// reply := kvs.StartPut(in)
	// if reply.Err == raft.ErrWrongLeader {
	// 	reply.LeaderId = kvs.raft.GetLeaderId()
	// }
	// resultCh <- reply
	// }()

	// // 返回结果通道，让客户端可以等待结果
	// return <-resultCh, nil
}

func (kvs *KVServer) StartPut(args *kvrpc.PutInRaftRequest) *kvrpc.PutInRaftResponse {
	tHandler := time.Now() // handler 全程，用于校验各阶段之和有没有漏测
	// The target file is no longer decided here: a GC switch can happen between this point
	// and the actual write, which once misplaced three records. The Raft layer now records
	// the file version together with the offset at write time (ApplyMsg.FileVersion), and
	// reading kvs.numGC here without a lock raced with GC's increment anyway.
	reply := &kvrpc.PutInRaftResponse{Err: raft.OK, LeaderId: 0}
	op := raftrpc.DetailCod{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	// T1开始 - Raft日志持久化阶段
	// t1Start := time.Now()
	tStart := time.Now()
	// Start fills op.Index/op.Term under its lock before publishing the entry; writing them
	// back here would race with the replication goroutine that may already be encoding it.
	_, _, isLeader = kvs.raft.Start(&op)
	raftStartDur := time.Since(tStart)
	// t1End := time.Now()
	// t1Duration := t1End.Sub(t1Start)
	// fmt.Printf("T1 (Raft日志持久化) duration: %v\n", t1Duration)
	if !isLeader {
		// fmt.Println("不是leader，返回")
		reply.Err = raft.ErrWrongLeader
		return reply // 如果收到客户端put请求的不是leader，需要将leader的id返回给客户端的reply中
	}
	opCtx := newOpContext(&op)
	// alreadyApplied：raft.Start 已经把这条 index 写下去了，而注册 opCtx 是之后的事。
	// 这中间 applyLoop 完全可能已经处理完这条 index——它查 reqMap 查不到，就不会
	// close(opCtx.committed)，于是下面的 select 永远等不到通知，一直挂到超时。
	// 写入本身不受影响（applyLoop 的存储分支与 existOp 无关），丢的只是那次唤醒。
	//
	// lastAppliedIndex 和这里的注册都在 kvs.mu 下，天然串行：要么注册先到、apply 时
	// 查得到 opCtx，要么 apply 先到、此处就能看见 lastAppliedIndex 已经越过 op.Index。
	var alreadyApplied bool
	func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if int(op.Index) <= kvs.lastAppliedIndex {
			alreadyApplied = true
			recordEarlyApply()
			return
		}
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kvs.reqMap[int(op.Index)] = opCtx
	}()
	if alreadyApplied {
		// 已经落盘，没有需要等待的回调
		recordPut(time.Since(tHandler), raftStartDur, 0)
		return reply
	}
	// _,exist:=kvs.reqMap[int(op.Index)]
	// fmt.Println("大撒上的",exist)
	// fmt.Printf("index%v\n",op.Index)

	// fmt.Println("222")

	defer func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if one, ok := kvs.reqMap[int(op.Index)]; ok {
			if one == opCtx {
				delete(kvs.reqMap, int(op.Index))
			}
		}
	}()
	timer := time.NewTimer(kvs.commitTimeout)
	defer timer.Stop()
	tWait := time.Now()
	select {
	// 通道关闭或者有数据传入都会执行以下的分支
	case <-opCtx.committed: // ApplyLoop函数执行完后，会关闭committed通道，再根据相关的值设置请求reply的结果
		recordPut(time.Since(tHandler), raftStartDur, time.Since(tWait))
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = raft.ErrWrongLeader
			// fmt.Println("走了哪个操作1")
			// fmt.Println("设置reply为WrongLeader")
		} else if opCtx.ignored {
			// fmt.Println("走了哪个操作2")
			// 说明req id过期了，该请求被忽略，对MIT这个lab来说只需要告知客户端OK跳过即可
			reply.Err = raft.OK
		}
		// fmt.Println("444")
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		// fmt.Println("Put请求执行超时了，超过了2s，重新让client发送执行")
		// reply.Err = raft.ErrWrongLeader
		reply.Err = "defeat"
		// fmt.Println("555")
	}
	return reply
}
