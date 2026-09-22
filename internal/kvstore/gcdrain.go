// GC 的收尾（排空）开关：让驱动在测读之前把 valuelog 尾部吸收干净。
//
// 为什么需要它。GC 的触发条件是「尾部有多长」（gcloop.go 里的 tailBytes >= need），
// 不是「还有没有活干」。写入一停，尾部就不再增长，于是**低于阈值的那一截永远留在
// valuelog 里**。而读路径是先查 valuelog、再查有序分区，所以这一截残留直接决定
// 读走哪条路——两条路的长度差了一个数量级（一次库查找加一次 pread，对
// 分区定位加稀疏索引二分加块内顺序扫）。
//
// 于是「读得多快」里混进了「写完的那一刻恰好剩多少尾部」，而那一项**取决于写入
// 有多快**，与被测的变量无关。2026-09-22 实测，同样写 10GiB：
//
//	多数派提交   230 秒写完   GC 跑了 11 轮
//	异步提交      52 秒写完   GC 只跑了 4 轮
//
// 由 valuelog 答复的读因此在 1KB / 4KB / 16KB 三档分别是 5.7% / 13.0% / 0%。
// 拿这样两份数据比读性能，比出来的是布局差异。
//
// 这不是异步档才有的问题：**此前每一轮读数据都打在一个随机的半成品布局上**，
// 主表那 72 格也一样。LSM 的读测试在读之前先 compact 是标准做法，这就是那一步。
package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"
)

// gcDrainFloorBytes 是「尾部见底」的判据。
//
// 不取 0：valuelog 永远有一条 NoOp 打头（20 字节的头，keySize=0、valueSize=0），
// 而且吸收完一轮之后新日志文件里马上会有新的元数据行。取 0 会让排空永远不结束。
// 取 1 MiB：比任何元数据都大得多，又远小于任何一档的单轮吸收量（最小的 GC 阈值
// 是 0.3 GiB），所以「剩下不到 1 MiB」等价于「没有用户数据可搬了」。
const gcDrainFloorBytes = 1 << 20

// gcDrainFile 是驱动与节点之间的握手文件，放在数据目录下。
//
// 为什么用文件而不是 RPC 或信号：驱动是一串 ssh 命令，touch 一个文件是它最自然的
// 动作；而**由节点删掉这个文件**给出了一个不含歧义的完成信号——驱动不必去猜
// 「轮数不变是不是代表做完了」，那个猜法已经错过一次（轮数在一轮**开始**时就自增，
// 于是完成数可以连续 40 秒不变而下一轮正在写盘）。
const gcDrainFile = "gc_drain"

// gcDrainState 记录这一次排空做了几轮、尾部从多大降到多大，供驱动写进结果。
type gcDrainState struct {
	rounds    atomic.Int64
	startTail atomic.Int64
	requested atomic.Bool
}

func (kvs *KVServer) gcDrainPath() string {
	return filepath.Join(kvs.dataDir, "data", gcDrainFile)
}

// gcDrainRequested 看驱动有没有要求排空。
//
// 每轮 tick 都 stat 一次文件（5 秒一次，代价可以忽略）。第一次看见时打一行日志并
// 记下起始尾部——**只打一次**，否则排空期间每 5 秒刷一行。
func (kvs *KVServer) gcDrainRequested() bool {
	if !kvs.gcEnabled || !kvs.kvSeparation {
		// 不跑 GC 的系统没有尾部要排。**这里必须早退**：否则 baseline 会带着
		// 一个永远等不到删除的握手文件，驱动就在那里等到超时，而日志上什么都没有。
		return false
	}
	_, err := os.Stat(kvs.gcDrainPath())
	if err != nil {
		if kvs.gcDrain.requested.Swap(false) {
			// 文件被外面删掉了（驱动放弃或人工干预），把状态收干净。
			fmt.Printf("[GC-DRAIN] 握手文件消失，排空中止（已做 %d 轮）\n", kvs.gcDrain.rounds.Load())
		}
		return false
	}
	if !kvs.gcDrain.requested.Swap(true) {
		kvs.gcDrain.rounds.Store(0)
		tail := int64(-1)
		kvs.mu.Lock()
		cur := kvs.currentLog
		kvs.mu.Unlock()
		if fi, e := os.Stat(cur); e == nil {
			tail = fi.Size()
		}
		kvs.gcDrain.startTail.Store(tail)
		fmt.Printf("[GC-DRAIN] 收到排空请求，忽略阈值继续吸收；起始尾部 %.1fMB，见底线 %.1fMB\n",
			float64(tail)/1048576, float64(gcDrainFloorBytes)/1048576)
	}
	return true
}

// noteGCDrainRound 在排空模式下每完成一轮记一次。由 gcLoop 在一轮结束后调用。
func (kvs *KVServer) noteGCDrainRound() {
	if kvs.gcDrain.requested.Load() {
		kvs.gcDrain.rounds.Add(1)
	}
}

// finishGCDrain 宣布排空完成：删掉握手文件。
//
// **删除是唯一的完成信号**，所以删不掉就必须喊出来——静默失败会让驱动一直等，
// 而它等的那个条件永远不会成立。
func (kvs *KVServer) finishGCDrain(tailBytes int64) {
	rounds := kvs.gcDrain.rounds.Load()
	start := kvs.gcDrain.startTail.Load()
	if err := os.Remove(kvs.gcDrainPath()); err != nil && !os.IsNotExist(err) {
		fmt.Printf("[GC-DRAIN] **删不掉握手文件 %s: %v** —— 驱动会一直等下去\n", kvs.gcDrainPath(), err)
		return
	}
	kvs.gcDrain.requested.Store(false)
	fmt.Printf("[GC-DRAIN] 排空完成：%d 轮，尾部 %.1fMB -> %.1fMB，用时截至 %s\n",
		rounds, float64(start)/1048576, float64(tailBytes)/1048576,
		time.Now().Format("15:04:05"))
}
