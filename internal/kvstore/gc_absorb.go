package kvstore

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"github.com/linxGnu/grocksdb"
)

// 吸收：把尾部日志并进它**实际覆盖到**的那些分区，其余分区原样复用。
//
// 改造前每一轮都是"上一版全部分区 + 新日志 → 全量重写成新的一组"，代价 O(数据集)。
// 更要命的是触发条件是一个**绝对值**（valuelog 超过 gcThresholdGB），于是第 k 轮重写
// k×阈值 的活数据却只吸收了一个阈值的新数据，总代价 Σk·T = O(n²/T)。
// 重写多少不是问题，**"重写多少"与"因此吸收了多少新数据"不成比例才是问题**。
//
// 两处修正缺一不可：
//
//  1. 这里：只重写尾部覆盖到的分区。**它省多少完全取决于写入流的 key 区间局部性**，
//     与频次倾斜无关——决定一个分区要不要重写的是**覆盖**，碰到一个 key 就得整块重写。
//     两端都实测过：
//       顺序写（scripts/test/gc-rounds.sh，键按 0,1,2… 写）复用 3~4/8 个分区，
//       约 40~50% 的字节原样留下；
//       zipf 或 uniform 铺满键空间时复用**恒为零**（results/amplification/2026-09-09-writeamp）：
//       尾部攒到 GC 阈值已含数万个不同 key，分区只有十来个，每个必然被碰到，
//       某个分区一个 key 都没碰到的概率约 (1-1/12)^64000。
//     "Zipf 倾斜 ⇒ 尾部只碰少数分区"这个原始推理是错的，别再照它推。
//  2. gcloop 里：触发条件改成"尾部大小 / 分区总量"的比例。这样每重写一次 O(n) 之前
//     必然已吸收 O(n) 新数据，摊销后每字节 O(1)，最坏情况总代价也回到 O(n)。
//
// 为什么不需要"按垃圾率压实单个分区"：吸收重写一个分区时，被新版本取代的旧记录当场就
// 丢掉了，该分区垃圾归零；而没被碰到的分区，正是那些 key 没被覆盖、本来就没有垃圾的。
// 垃圾因此不会在分区里累积。代价是一个分区里哪怕只有一个 key 被覆盖也要整块重写——
// 这就是 LSM tiering 的权衡，由上面第 2 条按比例触发来摊销。

// absorbTail 执行一次吸收。调用前 AnotherSwitchToNewFiles 已把新日志与新库挂上，
// 客户端的写入落在新的一对上，这里处理的是被冻结的旧库 + 上一组分区。
func (kvs *KVServer) absorbTail(startTime time.Time) error {
	old := kvs.lastPartitions
	if old == nil {
		return fmt.Errorf("吸收缺少上一组分区")
	}

	// 基名固定、只加轮号，不以上一轮的基名为前缀——否则名字逐轮叠加，
	// 8 轮之后就是 RaftState_sorted_1_absorb_2_absorb_3_..._absorb_8。
	base := fmt.Sprintf("%s_%d", sortedFileBase, kvs.numGC)
	// 本轮的分区文件如果已经在盘上，那是上一次尝试留下的、**未提交**的产物：一组分区只有
	// 在 finishAnotherGC 把清单写进 kv_state.json 之后才算数，而那之后 numGC 就推进了，
	// 下一轮用的是另一个基名。所以这里看到的必然是半成品，先清掉再重写。
	//
	// 原先这里是"存在就跳过"。文件存在分不清"写完了"和"写了一半"，于是搬运途中崩溃会让
	// 重做把半个分区当成完整产物：吸收直接返回，anotherPartitions 仍是 nil，恢复随即
	// log.Fatalf，而且每次重启都走同一条路——一次崩溃就让节点再也起不来。
	if err := removePartitionFiles(base); err != nil {
		return fmt.Errorf("清理上一次未完成的吸收产物失败: %v", err)
	}
	kvs.anotherSortedFilePath = base

	oldFile, err := os.Open(kvs.oldLog)
	if err != nil {
		return fmt.Errorf("打开旧日志失败: %v", err)
	}
	defer oldFile.Close()

	// 尾部按 key 序读出。RocksDB 迭代器天然按 key 升序，正是归并要的顺序。
	tail := make(chan *raft.Entry, 1000)
	var tailErr atomic.Value
	go func() {
		defer close(tail)
		it := kvs.oldPersister.GetDb().NewIterator(grocksdb.NewDefaultReadOptions())
		defer it.Close()
		for it.SeekToFirst(); it.Valid(); it.Next() {
			k, v := it.Key(), it.Value()
			if raft.IsMetaKey(k.Data()) {
				k.Free()
				v.Free()
				continue
			}
			e, err := kvs.entryFromRecord(string(k.Data()), v.Data(), oldFile)
			k.Free()
			v.Free()
			if err != nil {
				// 搬不动就必须让整轮失败、保住源文件等下一轮重试。少搬一条也不能算成功：
				// 读路径把"这一处没有"当作常态，丢掉的记录只会在某次 GET 上变成一个 NOKEY。
				tailErr.Store(fmt.Errorf("读取旧库记录失败: %v", err))
				return
			}
			tail <- e
		}
	}()

	pw := kvs.newPartitionWriter(base)
	var newParts []*SortedFileIndex
	var reclaimed int64 // 本次吸收丢掉的旧版本字节数，纯观测
	reusedParts, rewrittenParts, tailCount := 0, 0, 0
	// 复用与重写各自的源字节数。吸收省下的写入量就是 reusedBytes——全量重写会把这些
	// 字节原样再写一遍。两者相加即全量重写的代价，比值可直接算出本轮省了多少，
	// 无需再造一个"全量重写"的二进制来对照。
	var reusedBytes, rewrittenBytes int64

	pending, more := <-tail
	// flushWriter 把写入器刚产出的分区接到清单末尾，保持 key 序
	taken := 0
	flushWriter := func() {
		newParts = append(newParts, pw.done[taken:]...)
		taken = len(pw.done)
	}

	for _, part := range old.parts {
		// 收集落在本分区区间内的尾部记录。分区按 Lo 升序、区间互不重叠，
		// 所以"key <= 本分区 Hi"的尾部记录必然属于本分区或它之前的空隙。
		var slice []*raft.Entry
		for more && pending.Key <= part.Hi {
			slice = append(slice, pending)
			pending, more = <-tail
		}
		if len(slice) == 0 {
			// 尾部没碰到它，原样复用：不读、不写、不改名。这正是省下来的代价。
			newParts = append(newParts, part)
			reusedParts++
			reusedBytes += part.FileSize
			continue
		}
		tailCount += len(slice)
		n, err := kvs.mergePartition(pw, part, slice)
		if err != nil {
			pw.Abort()
			return err
		}
		reclaimed += n
		// 必须在这里封口：下一个被重写的区间与本段 key 不相邻，中间隔着原样复用的分区。
		// 不封口就会把两段写进同一个文件，其 key 区间横跨复用分区，区间重叠、路由失效。
		if err := pw.Seal(); err != nil {
			pw.Abort()
			return err
		}
		flushWriter()
		rewrittenParts++
		rewrittenBytes += part.FileSize
	}

	// 比最后一个分区还大的尾部记录：单独成新分区接在末尾
	for more {
		if err := pw.Add(pending); err != nil {
			pw.Abort()
			return err
		}
		tailCount++
		pending, more = <-tail
	}

	if e := tailErr.Load(); e != nil {
		pw.Abort()
		return fmt.Errorf("吸收中止，源文件保持不动: %v", e.(error))
	}

	parts, err := pw.Finish()
	if err != nil {
		pw.Abort()
		return err
	}
	newParts = append(newParts, parts.parts[taken:]...)

	// 复用过来的分区仍指向**上一组**的内联缓存，其中可能缓存着刚被本次吸收覆盖掉的旧值。
	// 读路径命中它就会返回陈旧数据，所以整组统一换成新缓存。代价是丢掉缓存热度，
	// 换来的是正确性——缓存是纯加速层，冷启动只是慢一点。
	for _, p := range newParts {
		p.InlineValues = parts.inline
	}
	merged := &PartitionSet{parts: newParts, inline: parts.inline, base: base}

	kvs.mu.Lock()
	kvs.anotherPartitions = merged
	kvs.mu.Unlock()

	fmt.Printf("[GC-ABSORB] round=%d 尾部=%d条 复用分区=%d(%dB) 重写分区=%d(%dB) 产出=%d个 回收=%dB 耗时=%v\n",
		kvs.numGC, tailCount, reusedParts, reusedBytes, rewrittenParts, rewrittenBytes,
		merged.Len(), reclaimed, time.Since(startTime).Round(time.Millisecond))

	kvs.anotherEndGC = true
	kvs.switchedPersister = nil
	return nil
}

// mergePartition 把 slice（已按 key 升序）并进 part，产物交给 pw。
// 返回被新版本取代、因此丢掉的旧记录字节数。
func (kvs *KVServer) mergePartition(pw *partitionWriter, part *SortedFileIndex, slice []*raft.Entry) (int64, error) {
	f, err := os.Open(part.FilePath)
	if err != nil {
		return 0, fmt.Errorf("打开分区 %s 失败: %v", part.FilePath, err)
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 1<<20)

	var reclaimed int64
	i := 0
	cur, curErr := nextEntry(reader)
	for cur != nil || i < len(slice) {
		if curErr != nil {
			return 0, fmt.Errorf("读取分区 %s 失败: %v", part.FilePath, curErr)
		}
		switch {
		case cur == nil: // 分区读完，只剩尾部
			if err := pw.Add(slice[i]); err != nil {
				return 0, err
			}
			i++
		case i >= len(slice): // 尾部用完，只剩分区
			if err := pw.Add(cur); err != nil {
				return 0, err
			}
			cur, curErr = nextEntry(reader)
		case cur.Key < slice[i].Key:
			if err := pw.Add(cur); err != nil {
				return 0, err
			}
			cur, curErr = nextEntry(reader)
		case cur.Key > slice[i].Key:
			if err := pw.Add(slice[i]); err != nil {
				return 0, err
			}
			i++
		default: // 同一个 key：尾部的是新版本，分区里的那份就是垃圾，就地丢掉
			reclaimed += int64(20 + len(cur.Key) + len(cur.Value))
			if err := pw.Add(slice[i]); err != nil {
				return 0, err
			}
			i++
			cur, curErr = nextEntry(reader)
		}
	}
	return reclaimed, nil
}

// nextEntry 读下一条 entry；读完返回 (nil, nil)，好让归并循环用 cur == nil 表示这一路耗尽。
// ReadEntry 用 io.EOF 表示正常读完，直接透传会与真正的读取错误混在一起。
func nextEntry(reader *bufio.Reader) (*raft.Entry, error) {
	e, _, err := ReadEntry(reader, 0)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return e, nil
}
