package kvstore

import "sync/atomic"

// storeLease 表示"这一份状态快照上还有读者没读完"。
//
// 要防的失效是这个：GC 的读路径把三路查找**并行**发出去，然后按优先级逐路收结果，
// **哪一路答出了 value 就直接 return**。于是剩下那几个 goroutine 在调用方返回之后
// 还在跑，而调用方返回时 `defer kvs.storeRetireMu.RUnlock()` 已经执行了——
// 读锁放掉了，读者却还在里面。接着 removeSupersededStore 毫无阻碍地拿到写锁，
// `kvs.oldPersister.Close()` 把 RocksDB 关掉，那个孤儿 goroutine 再去 Get_opt：
//
//	grocksdb.(*DB).Get(0x0, ...)                     ← DB 句柄是 nil
//	  raft.(*Persister).Get_opt(0xc029fadf20, ...)    ← Persister 本身还在
//	    kvstore.(*KVServer).anotherGCGet.func3()      read.go:677
//
// 2026-09-18 在 nezha-avp 256B 那一格实测到，node0 直接 panic 退出，随后触发了一次
// 重新选举。**这不是持锁太久，是跑到锁外面去了**——与 storeRetireMu / stateMu 当初
// 修的那两个"长读者把关键锁钉住"正好相反的一面，所以那两把锁都挡不住它。
//
// 修法：读锁的生命周期不再等于调用栈的生命周期，而等于**这份快照上所有读者**的生命周期。
// 调用方本身算一个参与者，每个派生 goroutine 算一个，谁最后结束谁放锁。
// 于是调用方仍然可以在第一路答出来时立刻返回（延迟不变，这一点对交付的延迟对比很重要），
// 而库在最后一个读者离开之前不会被回收。
//
// 为什么用计数器而不是 WaitGroup：WaitGroup 需要有人去 Wait，那就得再开一个
// goroutine 专门等着放锁——每次 GET 都多一个 goroutine，而 GET 是 6 万/秒那一档。
// 计数器让"放锁"发生在最后那个本来就要结束的 goroutine 上，不多开任何东西。
//
// 从别的 goroutine 调 RUnlock 是允许的：sync.RWMutex 的读锁不绑定 goroutine，
// 只要加减配平即可。
type storeLease struct {
	kvs *KVServer
	n   atomic.Int32
}

// done 交还一个参与者。减到 0 的那一个负责放掉读锁。
func (l *storeLease) done() {
	if l.n.Add(-1) == 0 {
		l.kvs.storeRetireMu.RUnlock()
	}
}

// beginRead 取读锁、捕获状态快照，并把两者绑成一份租约。
// 调用方必须 `defer st.endRead()`，并且**一切会摸到 st 里的库、日志、分区的
// goroutine 都要用 st.spawn 派生**，否则它就会跑到锁外面去。
func (kvs *KVServer) beginRead() stateSnapshot {
	kvs.storeRetireMu.RLock()
	st := kvs.captureState()
	l := &storeLease{kvs: kvs}
	l.n.Store(1) // 调用方自己算一个参与者
	st.lease = l
	return st
}

// endRead 交还调用方那一份。此时若还有派生的 goroutine 在跑，读锁不会被放掉。
func (st stateSnapshot) endRead() { st.lease.done() }

// spawn 派生一个计入租约的 goroutine。
//
// n.Add(1) 必须在 `go` **之前**：放到 goroutine 里面做的话，调用方可能先返回、
// 把计数减到 0 并放掉读锁，然后这个 goroutine 才开始计数——那就等于没计。
// 这也是为什么所有 spawn 都必须发生在调用方返回之前（现有代码都是这样）。
func (st stateSnapshot) spawn(f func()) {
	st.lease.n.Add(1)
	go func() {
		defer st.lease.done()
		f()
	}()
}
