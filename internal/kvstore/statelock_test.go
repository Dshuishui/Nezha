package kvstore

import (
	"sync"
	"testing"
	"time"
)

// 这一组用例钉的是**锁的拓扑**，不是某条读写路径的结果。
//
// 要防的失效是这个：读路径原先整段读都持着 stateMu 的读锁，而 applyCommand 也按读锁
// 持有它，装快照按写锁。Go 的 RWMutex 在有写者等待时不再放新读者进来，于是
//     一次长扫描持读锁 -> 装快照等写锁 -> 之后每一条 apply 的读锁都排在写者后面
// apply 停一次扫描的时长。4GB 规模下一次扫描是 33 秒，而它表现成"这个副本莫名其妙
// 追不上"，日志里一行解释都没有。
//
// 修法是把"一致地看到同一份状态"与"这份状态在读完之前不被回收"拆给两把锁：
// 前者 stateMu 只在 captureState 那一小段持有，后者整段读持 storeRetireMu 的读锁。
// 这两条性质都不体现在任何返回值里，所以只能这样直接测：
//
//   TestLongReaderDoesNotBlockStateWriter   长读者不得延迟 stateMu.Lock()
//   TestLongReaderBlocksStoreRetire         长读者**必须**挡住回收
//   TestApplyExcludedDuringInstall          安装窗口内 apply 仍被挡住（这条要保留）
//
// 第二条与第一条方向相反，是有意的：只测"不再挡住"会让人把锁全删掉也过。

// grabTime 返回取到锁花了多久。
func grabTime(lock func()) time.Duration {
	start := time.Now()
	lock()
	return time.Since(start)
}

func TestLongReaderDoesNotBlockStateWriter(t *testing.T) {
	kvs := &KVServer{}

	// 模拟一次长范围扫描：整段读持 storeRetireMu 的读锁，
	// 并且只在开头极短地摸一下 stateMu（captureState 的行为）。
	kvs.storeRetireMu.RLock()
	kvs.stateMu.RLock()
	kvs.stateMu.RUnlock()

	// 装快照要取 stateMu 的写锁。它不该被上面那个读者挡住。
	got := grabTime(kvs.stateMu.Lock)
	kvs.stateMu.Unlock()
	kvs.storeRetireMu.RUnlock()

	// 阈值取得很松：要抓的是"被挡住整段读的时长"（秒级），不是调度抖动（微秒级）。
	if got > 200*time.Millisecond {
		t.Fatalf("有长读者在时，取 stateMu 写锁花了 %v——读路径又开始整段持 stateMu 了", got)
	}
}

func TestLongReaderBlocksStoreRetire(t *testing.T) {
	kvs := &KVServer{}

	kvs.storeRetireMu.RLock()
	// 回收路径（removeSupersededStore / removeSupersededLog / 装快照的第 7 步）取写锁。
	// 它**必须**等这个读者——否则迭代器会用在一个已经关掉的 RocksDB 上，
	// 那是 C++ 层的 use-after-free，不是一个 Go panic。
	locked := make(chan struct{})
	go func() {
		kvs.storeRetireMu.Lock()
		close(locked)
		kvs.storeRetireMu.Unlock()
	}()

	select {
	case <-locked:
		kvs.storeRetireMu.RUnlock()
		t.Fatal("读者还持着锁，回收就拿到了写锁——被取代的库会在读到一半时被关掉")
	case <-time.After(100 * time.Millisecond):
	}

	kvs.storeRetireMu.RUnlock()
	select {
	case <-locked:
	case <-time.After(2 * time.Second):
		t.Fatal("读者放手之后回收仍拿不到写锁")
	}
}

func TestApplyExcludedDuringInstall(t *testing.T) {
	kvs := &KVServer{}

	// 装快照持 stateMu 的写锁覆盖整个安装窗口。apply 每条取一次读锁，
	// 期间必须被挡住——安装很早就查过 lastAppliedIndex，若 apply 还能推进，
	// 最后把它重设成快照里的值就是往回退，而那几条的数据在即将被删的旧库里。
	kvs.stateMu.Lock()

	applied := make(chan struct{})
	go func() {
		kvs.stateMu.RLock()
		close(applied)
		kvs.stateMu.RUnlock()
	}()

	select {
	case <-applied:
		kvs.stateMu.Unlock()
		t.Fatal("安装窗口内 apply 仍能推进——lastAppliedIndex 会被安装往回退")
	case <-time.After(100 * time.Millisecond):
	}

	kvs.stateMu.Unlock()
	select {
	case <-applied:
	case <-time.After(2 * time.Second):
		t.Fatal("安装结束之后 apply 仍拿不到读锁")
	}
}

// TestCaptureStateTakesEverythingAtOnce 钉的是"一次取完"。
//
// stateMu 当初被引入正是因为：只用 kvs.mu 分别护住各自的赋值，读路径会取到**新的
// persister 配旧的 currentLog**——那读出来是别的记录的 value，而且不报错。
// captureState 把它们放在同一个临界区里取，所以只要写入方也在同一个临界区里成对更新，
// 读到的组合就必然是同一份。这里用一个持续在"两个一致状态"之间翻转的写入方来检查：
// 任何一次捕获都不该看到 A 的库配 B 的日志。
func TestCaptureStateTakesEverythingAtOnce(t *testing.T) {
	kvs := &KVServer{}
	const (
		logA = "logA"
		logB = "logB"
	)
	// 用 currentLog 与 oldLog 这一对代表"必须一起变"的两个字段：写入方永远把它们
	// 设成 (A,A) 或 (B,B)，于是读到 (A,B) 就说明捕获跨越了一次更新。
	kvs.currentLog, kvs.oldLog = logA, logA

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		to := logB
		for {
			select {
			case <-stop:
				return
			default:
			}
			// 写入方按写锁成对更新，与装快照的做法一致。
			kvs.stateMu.Lock()
			kvs.currentLog, kvs.oldLog = to, to
			kvs.stateMu.Unlock()
			if to == logB {
				to = logA
			} else {
				to = logB
			}
		}
	}()

	for i := 0; i < 20000; i++ {
		st := kvs.captureState()
		if st.currentLog != st.oldLog {
			close(stop)
			wg.Wait()
			t.Fatalf("第 %d 次捕获取到了 currentLog=%q 配 oldLog=%q——捕获跨越了一次状态更新",
				i, st.currentLog, st.oldLog)
		}
	}
	close(stop)
	wg.Wait()
}

// TestSpawnedReaderKeepsStoreAlive 钉的是"派生的读者也算读者"。
//
// 要防的失效见 storeLease 的注释：GC 的多路查找把三路并行发出去，第一路答出 value
// 就 return，而 `defer RUnlock()` 跟着这次返回执行——剩下那几个 goroutine 就跑到锁
// 外面去了。接着回收路径拿到写锁、Close() 掉 RocksDB，孤儿 goroutine 再去 Get_opt，
// 拿到的是一个 nil 句柄：
//
//	grocksdb.(*DB).Get(0x0, ...)
//
// 2026-09-18 在 nezha-avp 256B 那一格实测到，node0 panic 退出并触发重新选举。
//
// 这条与 TestLongReaderBlocksStoreRetire 是一对：那条测"调用方还在读时挡住回收"，
// 这条测"调用方已经返回、但派生 goroutine 还在读时**也**挡住回收"。
// 只有前者的话，把 spawn 换回裸 go func 仍然会过。
func TestSpawnedReaderKeepsStoreAlive(t *testing.T) {
	kvs := &KVServer{}

	st := kvs.beginRead()

	// 一个还没结束的派生读者。
	running := make(chan struct{})
	finish := make(chan struct{})
	st.spawn(func() {
		close(running)
		<-finish
	})
	<-running

	// 调用方在这里就返回了——这正是 GET 的行为：第一路答出来就走。
	st.endRead()

	retired := make(chan struct{})
	go func() {
		kvs.storeRetireMu.Lock()
		close(retired)
		kvs.storeRetireMu.Unlock()
	}()

	select {
	case <-retired:
		close(finish)
		t.Fatal("调用方返回后回收就拿到了写锁——派生的读者还在库里，Close() 会让它解引用 nil")
	case <-time.After(100 * time.Millisecond):
	}

	// 派生读者结束，锁这才该放开。
	close(finish)
	select {
	case <-retired:
	case <-time.After(2 * time.Second):
		t.Fatal("派生读者结束之后回收仍拿不到写锁——读锁没有被最后那个读者放掉")
	}
}

// TestLeaseReleasesExactlyOnce 防的是配平：多放一次会让下一次 Lock() 直接穿过去
// （读锁计数变负），少放一次会让回收永远等下去。这里连着做两轮，第二轮能取到写锁
// 才说明第一轮恰好放了一次。
func TestLeaseReleasesExactlyOnce(t *testing.T) {
	kvs := &KVServer{}
	for round := 0; round < 2; round++ {
		st := kvs.beginRead()
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			st.spawn(func() { wg.Done() })
		}
		st.endRead()
		wg.Wait()

		got := grabTime(kvs.storeRetireMu.Lock)
		kvs.storeRetireMu.Unlock()
		if got > 200*time.Millisecond {
			t.Fatalf("第 %d 轮：所有读者都结束了，取写锁却花了 %v——租约少放了一次", round, got)
		}
	}
}
