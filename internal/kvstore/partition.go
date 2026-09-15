package kvstore

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 分区化的有序运行单元
//
// 改造前 GC 的产物是**一个**排序文件：第一轮把整个数据集重写成它，第二轮把它与其后的新
// 日志归并成一个更大的。文件是一个整体，回收也就只能是整体——垃圾占 5% 也要重写 100%，
// 代价正比于活数据而非垃圾量；第二轮起输出还逐轮变大，放开轮数上限后全生命周期总代价是
// O(n²)。`numGC >= 2` 这个硬上限某种意义上正是在掩盖这一点。
//
// 分区把那一个文件切成若干覆盖互不重叠 key 区间的文件，每个自带稀疏索引与文件描述符池。
// 单个分区即一个独立的回收单元，压实一个分区的代价是 O(该分区)而非 O(数据集)。
//
// 为什么按 **key 区间**切，而不是按写入顺序或哈希分段（Titan 的 blob file、HashKV 的哈希段）：
// 本系统的 SCAN 优势完全来自"有序布局 + 稀疏索引"，范围查询因此是一段连续读。不按 key 有序
// 的分段会把范围查询打回随机读——对这个系统是灾难。DiffKV 的 sorted group 是唯一同时满足
// "回收可增量"与"读不退化"的骨架，这里借的是它。
//
// 本文件只做布局与路由。回收逻辑在 gc_absorb.go 与 gcloop.go：吸收把尾部并进它覆盖到的
// 分区，触发条件按尾部占分区总量的比例。分区级的垃圾计量**不需要**——吸收重写一个分区时
// 旧版本当场丢掉，没碰到的分区本来就没垃圾，所以任何 DeadBytes 都会恒为零。

// defaultPartitionTargetBytes 是单个分区的目标大小。
//
// 它是"读代价"与"回收粒度"之间的旋钮：调小则压实单个分区更便宜、GC 暂停更短，但一次全范围
// SCAN 要跨更多文件；调大则反之。128MB 下 10GB 数据约 80 个分区，一次全扫多出约 80 次打开与
// mmap（每次约 0.1ms，合计约 8ms），相对本就数秒的全扫可以忽略；而窄范围扫描只碰 1~2 个分区。
const defaultPartitionTargetBytes = 128 << 20

// partitionPoolSize 是每个分区缓存的文件描述符数。
//
// 改造前单文件时是 50。分区化之后这个数要乘以分区数，80 个分区就是 4000 个描述符，直接撞上
// 默认 1024 的 ulimit。配合惰性池（见 NewFileDescriptorPool），稳态描述符数由实际读并发决定，
// 这里只是每个分区的归还上限。
const partitionPoolSize = 8

// PartitionSet 是分区清单：按 Lo 升序排列、key 区间互不重叠的一组分区。
// 读路径靠它把一次查找路由到唯一（GET）或若干连续（SCAN）分区上。
type PartitionSet struct {
	parts []*SortedFileIndex
	// inline 是全集共享的一份内联缓存。不按分区切分是刻意的：分区数要写到最后才确定，没法预先
	// 分摊预算；共享一份则总内存与改造前完全一致，读性能对照不会被"缓存容量变了"污染——而这正是
	// P1 验收门槛所测的东西。每个分区的 InlineValues 都指向这同一个实例，getFromSortedFile
	// 因此不必改动。
	inline *InlineCache
	base   string // 这组分区的基名；分区文件是 <base>.p0、<base>.p1 …
	// refs 是长期读者（今天只有快照传输）持有的引用数。见本文件末尾"生命周期"一节：
	// 引用未归零的分区组，它的文件一个都不能删。
	refs atomic.Int32
}

func (ps *PartitionSet) Len() int {
	if ps == nil {
		return 0
	}
	return len(ps.parts)
}

// TotalSize 是全部分区的字节数之和。
func (ps *PartitionSet) TotalSize() int64 {
	if ps == nil {
		return 0
	}
	var n int64
	for _, p := range ps.parts {
		n += p.FileSize
	}
	return n
}

// Paths 按 key 序返回全部分区文件路径。
func (ps *PartitionSet) Paths() []string {
	if ps == nil {
		return nil
	}
	out := make([]string, 0, len(ps.parts))
	for _, p := range ps.parts {
		out = append(out, p.FilePath)
	}
	return out
}

// find 返回可能含有 paddedKey 的那个分区；没有则返回 nil。
//
// 区间互不重叠且按 Lo 升序，所以"最后一个 Lo <= key 的分区"是唯一候选。若 key 还在它的 Hi
// 之后，说明 key 落进了两个分区之间的空隙，这组分区里必然没有它，无需再读文件。
func (ps *PartitionSet) find(paddedKey string) *SortedFileIndex {
	if ps == nil || len(ps.parts) == 0 {
		return nil
	}
	i := sort.Search(len(ps.parts), func(j int) bool { return ps.parts[j].Lo > paddedKey }) - 1
	if i < 0 {
		return nil // 比所有分区的下界都小
	}
	if paddedKey > ps.parts[i].Hi {
		return nil // 落在两个分区之间的空隙
	}
	return ps.parts[i]
}

// overlapping 返回与 [lo, hi] 有交集的全部分区，按 key 升序；两端都是 padded key。
func (ps *PartitionSet) overlapping(lo, hi string) []*SortedFileIndex {
	if ps == nil || len(ps.parts) == 0 || lo > hi {
		return nil
	}
	// 第一个 Hi >= lo 的分区就是起点；其后凡 Lo <= hi 的都与区间有交集
	i := sort.Search(len(ps.parts), func(j int) bool { return ps.parts[j].Hi >= lo })
	j := i
	for j < len(ps.parts) && ps.parts[j].Lo <= hi {
		j++
	}
	return ps.parts[i:j]
}

// Close 释放全部分区持有的文件描述符。
func (ps *PartitionSet) Close() {
	if ps == nil {
		return
	}
	for _, p := range ps.parts {
		p.closePool()
	}
}

// ---- 清单的持久化 ----

// partitionMeta 是清单里的一项，随 KV 状态一起写盘。重启时按它直接重建分区边界，
// 不必先扫描全部数据文件才知道被切成了几段、各覆盖哪一段 key。
//
// 稀疏索引不放在这里，而是每个分区一个旁挂文件（见 sparseindex.go 的持久化一节）：
// 100GB 按 4KB 块是约 2500 万项，塞进这个每次保存 KV 状态都要重写的 JSON 里不合适。
type partitionMeta struct {
	// Path 存的是**文件名**，不是绝对路径。装载时按当前 -data 下的 valuelog 目录解析。
	//
	// 曾经存绝对路径，后果是数据目录不可搬动，而且失败方式极其阴险：把一个数据目录
	// 复制到别处再启动，节点会打开**原目录**里的分区文件（实测 /proc/<pid>/fd 全部指向
	// 原路径），副本自己那几个文件一个都不读。原目录还在且内容变了，读到的就是变了的
	// 数据；原目录被删了，才会报错。归档、复现、搬机器这些动作都会踩上。
	Path string `json:"path"`
	Lo   string `json:"lo"`
	Hi   string `json:"hi"`
	Size int64  `json:"size"`
	// Entries 是记录条数，纯观测：清单是回看一组分区长什么样的唯一入口。
	Entries int `json:"entries"`
}

func (ps *PartitionSet) manifest() []partitionMeta {
	if ps == nil {
		return nil
	}
	out := make([]partitionMeta, 0, len(ps.parts))
	for _, p := range ps.parts {
		out = append(out, partitionMeta{
			Path: filepath.Base(p.FilePath), Lo: p.Lo, Hi: p.Hi, Size: p.FileSize,
			Entries: p.Entries,
		})
	}
	return out
}

// loadPartitionSet 按清单重建一组分区：边界取自清单，稀疏索引优先读旁挂文件。
//
// 这一步的耗时要打出来。它正比于数据量（走扫描重建时实测约 110MB/s，100GB 约 15 分钟），
// 期间节点不能服务、在三节点里会被判失联——跑大规模实验时这是启动慢的第一嫌疑，
// 而"到底是读了旁挂索引还是扫了一遍"只有日志能说清。
func (kvs *KVServer) loadPartitionSet(base string, metas []partitionMeta) (*PartitionSet, error) {
	t0 := time.Now()
	var loaded, scanned int
	var bytesTotal int64
	defer func() {
		if len(metas) == 0 {
			return
		}
		fmt.Printf("[RECOVER] 装载 %d 个分区（%dB）耗时 %v：旁挂索引 %d 个，扫描重建 %d 个\n",
			len(metas), bytesTotal, time.Since(t0), loaded, scanned)
	}()
	ps := &PartitionSet{base: base, inline: NewInlineCache(kvs.inlineCacheBytes)}
	// 清单里只有文件名，按当前数据目录解析——旧清单存的是绝对路径，取 Base 一样能用，
	// 而且正好把"指向别处"这件事一并修掉。
	dir := filepath.Dir(base)
	for _, m := range metas {
		bytesTotal += m.Size
		path := filepath.Join(dir, filepath.Base(m.Path))
		// 先比字节数再重建索引。反过来的话，被截断的分区会先在扫描时撞上
		// "unexpected EOF"——那个错误既指不出是哪里不对，也不说明期望多长。
		// 清单说的字节数与文件实际长度对不上，意味着这个分区没有完整落盘或被改动过；
		// 继续用它会让读路径按错误的边界判定"这里没有这个 key"，那是静默丢数据。
		st, err := os.Stat(path)
		if err != nil {
			ps.Close()
			return nil, fmt.Errorf("partition %s: %v", path, err)
		}
		if st.Size() != m.Size {
			ps.Close()
			return nil, fmt.Errorf("partition %s: manifest says %d bytes, file has %d", path, m.Size, st.Size())
		}
		// 优先读旁挂索引：扫描重建的速率实测约 110MB/s，100GB 要 15 分钟才起得来。
		// 旁挂文件缺失或校验不过就退回扫描，所以最坏只是慢，不会用到错的索引。
		sparse, size, fromDisk, err := kvs.loadOrRebuildSparseIndex(path, m.Size)
		if err != nil {
			ps.Close()
			return nil, err
		}
		if fromDisk {
			loaded++
		} else {
			scanned++
		}
		pool, err := NewFileDescriptorPool(path, partitionPoolSize)
		if err != nil {
			ps.Close()
			return nil, fmt.Errorf("file descriptor pool for %s: %v", path, err)
		}
		ps.parts = append(ps.parts, &SortedFileIndex{
			Sparse:       sparse,
			FileSize:     size,
			InlineValues: ps.inline,
			FilePath:     path,
			Lo:           m.Lo,
			Hi:           m.Hi,
			pool:         pool,
			Entries:      m.Entries,
		})
	}
	return ps, nil
}

// ---- 写入 ----

func partitionPath(base string, i int) string { return fmt.Sprintf("%s.p%d", base, i) }

// unreferencedFiles 返回 reaped 这些分区组里、已经不再被 keep 里任何一组引用的文件。
//
// 吸收会把没被尾部碰到的分区**原样复用**进新一组，它们的文件仍在被引用，删掉就是丢数据。
// 所以不能按"上一组的基名"整体清理，必须按路径逐个比对。
//
// keep 是一组而不是一个，因为"仍被引用"不只指当前对外可读的那一组：还有引用没归零的
// 退役组（正在被快照传输读着）。见"生命周期"一节。
func unreferencedFiles(reaped, keep []*PartitionSet) []string {
	live := map[string]bool{}
	for _, ps := range keep {
		for _, p := range ps.Paths() {
			live[p] = true
		}
	}
	seen := map[string]bool{}
	var out []string
	for _, ps := range reaped {
		for _, p := range ps.Paths() {
			if live[p] || seen[p] {
				continue
			}
			seen[p] = true
			// 旁挂索引跟着它的数据文件一起走：留下一个描述已删除文件的 .idx 没有害处
			// （下次装载读不到对应的数据文件），但会一直占着盘。
			out = append(out, p, sparseIndexPath(p))
		}
	}
	return out
}

// removePartitionFiles 删除一组分区留下的全部文件。崩溃重做前要先清掉上次写了一半的产物，
// 否则残留的 .pN 会被下一次写入跳过或覆盖到一半。
func removePartitionFiles(base string) error {
	// 旁挂索引在 index/ 子目录里（见 sparseIndexPath），不在 base.p* 的通配范围内，
	// 所以要单独清一遍——留下一个描述已删分区的索引虽然读不错，但会一直占着盘，
	// 而且下一轮写到同名分区时那份旧索引的 dataSize 校验会失败、白扫一遍。
	patterns := []string{
		base + ".p*",
		filepath.Join(filepath.Dir(base), sparseIndexDir, filepath.Base(base)+".p*.idx"),
	}
	for _, pat := range patterns {
		matches, err := filepath.Glob(pat)
		if err != nil {
			return err
		}
		for _, m := range matches {
			if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// partitionWriter 按 key 序接收 entry，写满 targetBytes 就滚动到下一个分区文件，
// 边写边为每个分区建稀疏索引。
//
// 两轮 GC 的写入循环此前是两份几乎一样的代码：各自维护 sparse 构建器、内联缓存、bufio.Writer
// 与 currentOffset，收尾各自造一个 SortedFileIndex 和一个文件描述符池。分区化只在这里实现一次，
// 两轮共用。
type partitionWriter struct {
	kvs         *KVServer
	base        string
	targetBytes int64
	inline      *InlineCache

	// 当前正在写的分区
	file   *os.File
	w      *bufio.Writer
	sparse *SparseIndexBuilder
	offset int64
	lo, hi string
	n      int

	done []*SortedFileIndex

	// 分相位计时，供 [GC-PHASE] 报告。改造前 Flush 与 fsync 各只发生一次，现在分散在每次封口，
	// 累加起来才与改造前那两个数字可比。
	flushTime time.Duration
	syncTime  time.Duration
	total     int64
}

func (kvs *KVServer) newPartitionWriter(base string) *partitionWriter {
	target := kvs.partitionTargetBytes
	if target <= 0 {
		target = defaultPartitionTargetBytes
	}
	return &partitionWriter{
		kvs:         kvs,
		base:        base,
		targetBytes: target,
		inline:      NewInlineCache(kvs.inlineCacheBytes),
	}
}

// open 开始一个新分区。
func (pw *partitionWriter) open() error {
	path := partitionPath(pw.base, len(pw.done))
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create partition %s: %v", path, err)
	}
	pw.file = f
	pw.w = bufio.NewWriter(f)
	pw.sparse = NewSparseIndexBuilder(pw.kvs.indexBlockBytes)
	pw.offset, pw.n = 0, 0
	pw.lo, pw.hi = "", ""
	return nil
}

// Add 写入一条 entry。
//
// **调用方必须按 key 升序调用**——"分区区间互不重叠"这个前提全靠它，读路径的二分路由又完全
// 建立在那个前提上。两个调用点（第一轮的 RocksDB 迭代器、第二轮的归并循环）本来就是按 key
// 序产出的。
func (pw *partitionWriter) Add(entry *raft.Entry) error {
	if pw.file == nil {
		if err := pw.open(); err != nil {
			return err
		}
	}
	before := pw.offset
	if err := pw.kvs.WriteEntryToSortedFile(pw.w, entry); err != nil {
		return fmt.Errorf("write entry to partition %s: %v", pw.file.Name(), err)
	}
	size := int64(20 + len(entry.Key) + len(entry.Value)) // 与 WriteEntryToSortedFile 的格式一致

	// 每约 indexBlockBytes 记录一个块起点；索引项用文件里的 padded key，与查找时的比较一致
	pw.sparse.Observe(entry.Key, before, size)
	if pw.n == 0 {
		pw.lo = entry.Key
	}
	pw.hi = entry.Key
	pw.offset += size
	pw.n++

	// AVP：小值在预算内预热进内联缓存，读命中即可免去一次文件 seek
	if len(entry.Value) < pw.kvs.inlineThreshold {
		pw.inline.Add(entry.Key, entry.Value)
	}

	// 只在 entry 边界滚动，分区因此永远不会从中间截断一条记录
	if pw.offset >= pw.targetBytes {
		return pw.seal()
	}
	return nil
}

// seal 封口当前分区：落盘、建索引、开描述符池，然后加进清单。
//
// fsync 放在每个分区封口时而不是全部写完之后，有两个理由。一是调用方在本轮 GC 返回 nil 之后
// 会删掉源日志文件，产物必须先真正落盘，否则断电就是"源已删、目标还在 page cache"，本轮搬运的
// 数据全部丢失。二是顺带把一次巨大的 fsync 拆成若干次小的——改造前实测单次 fsync 曾达 6.8 秒，
// 长到足以让 follower 发起选举。
func (pw *partitionWriter) seal() error {
	if pw.file == nil {
		return nil
	}
	if pw.n == 0 { // 空分区不进清单，文件直接丢掉
		path := pw.file.Name()
		pw.file.Close()
		pw.file, pw.w = nil, nil
		return os.Remove(path)
	}

	t := time.Now()
	if err := pw.w.Flush(); err != nil {
		return fmt.Errorf("flush partition %s: %v", pw.file.Name(), err)
	}
	pw.flushTime += time.Since(t)

	t = time.Now()
	if err := pw.file.Sync(); err != nil {
		return fmt.Errorf("fsync partition %s: %v", pw.file.Name(), err)
	}
	pw.syncTime += time.Since(t)

	path := pw.file.Name()
	if err := pw.file.Close(); err != nil {
		return fmt.Errorf("close partition %s: %v", path, err)
	}
	pw.file, pw.w = nil, nil

	sparse := pw.sparse.Build()
	// 旁挂索引写在数据 fsync **之后**，所以它描述的一定是已经落盘的内容。
	// 写失败不算这一轮失败：索引缺失只会让下次启动退回扫描重建，不影响正确性。
	if err := writeSparseIndex(path, sparse, pw.kvs.indexBlockBytes, pw.offset); err != nil {
		fmt.Printf("[GC] 写分区 %s 的旁挂索引失败（下次启动会扫描重建）: %v\n", path, err)
	}

	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		return fmt.Errorf("file descriptor pool for %s: %v", path, err)
	}
	pw.done = append(pw.done, &SortedFileIndex{
		Sparse:       sparse,
		FileSize:     pw.offset,
		InlineValues: pw.inline,
		FilePath:     path,
		Lo:           pw.lo,
		Hi:           pw.hi,
		pool:         pool,
		Entries:      pw.n,
	})
	pw.total += pw.offset
	// 一个分区刚落盘、后面还有的那一刻——本轮产物在盘上但不完整、也还没提交。
	// 崩溃恢复的场景 D 要的就是这个窗口，它与"搬运尚未开始"的失败方式不同。
	gcWritePauseWindow()
	return nil
}

// Seal 强制封口当前分区，即使它还没写满。
//
// 吸收时必须在每一段被重写的区间结束时调用：中间那些没被尾部碰到的分区是**原样复用**的，
// 不经过写入器。若不封口，下一段（key 区间不相邻）会继续写进同一个文件，该文件的 [Lo,Hi]
// 就会横跨那个复用分区的区间——区间重叠，读路径的二分路由随即失效。
func (pw *partitionWriter) Seal() error { return pw.seal() }

// Finish 封口最后一个分区并交出清单。
func (pw *partitionWriter) Finish() (*PartitionSet, error) {
	if err := pw.seal(); err != nil {
		return nil, err
	}
	return &PartitionSet{parts: pw.done, inline: pw.inline, base: pw.base}, nil
}

// Abort 丢弃已经写下的一切。搬运中途失败时调用：本轮不推进状态，半成品不能留在盘上被下一轮
// 当成完整清单——那会让读路径以为某段 key 已经搬完而不再去源文件找。
func (pw *partitionWriter) Abort() {
	if pw.file != nil {
		pw.file.Close()
		pw.file, pw.w = nil, nil
	}
	for _, p := range pw.done {
		p.closePool()
	}
	pw.done = nil
	if err := removePartitionFiles(pw.base); err != nil {
		fmt.Printf("[GC] 清理未完成的分区文件失败（%s.p*）: %v\n", pw.base, err)
	}
}

// ---- 生命周期：钉住与回收 ----
//
// 一组分区被下一组取代之后，其中没有被新一组复用的文件就没人引用了，必须删掉。不删是空间
// 无界的直接原因：实测 8 轮之后盘上留着 35 个分区文件而活跃的只有 8 个，空间放大从 13 涨到 52。
//
// "没人引用"这个判断在快照出现之后不再只看分区组。一次快照传输会持续几分钟，期间发送端
// 按文件名逐个打开源文件流出去——它故意不做本地副本（多 GB 的状态复制一份比传输本身还贵，
// etcd 与 dragonboat 同样是直接流式发送）。于是 GC 在中途删掉一个还没发到的分区，
// 发送端的下一次 os.Open 就失败，而快照已经传了一半。
//
// 所以用引用计数：传输开始前钉住当前这一组，结束后放开；被取代的分区组进入等待队列，
// 引用归零之后才真正删文件。
//
// **只推迟 unlink，不关描述符池。** 已打开的 fd 在文件被 unlink 之后照样读到正确内容
// （POSIX 语义，inode 活到最后一个 fd 关闭），今天的代码正是靠这一点，才能在读者可能仍
// 持着上一组的时候立刻删文件。反过来，如果回收时顺手把描述符池 Close 掉，一个刚取到指针、
// 正在读的读者就会撞上已关闭的池：那等于在修一个并不是泄漏的问题（实测跑满 8 轮 GC，
// 进程 fd 数在 36~68 之间震荡、不随轮数增长，Go 给 os.File 挂了 finalizer）的同时，
// 引入一个真的 use-after-close。描述符池仍由 finalizer 回收，与改造前一致。

// pinPartitions 钉住当前对外可读的那一组分区，返回它和释放函数。
//
// 返回的释放函数**不能**在持有 kvs.mu 时调用（它会去拿锁），可以重复调用，只有第一次生效。
// 没有分区组时返回 (nil, 非 nil 的空操作)，调用方不必判空。
func (kvs *KVServer) pinPartitions() (*PartitionSet, func()) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	return kvs.pinPartitionsLocked()
}

// pinPartitionsLocked 同 pinPartitions，但由调用方持有 kvs.mu。快照制作要在**同一个**
// 临界区里既钉住分区又读下 numGC / 当前日志名，否则钉住的那一组可能属于另一轮。
func (kvs *KVServer) pinPartitionsLocked() (*PartitionSet, func()) {
	ps := kvs.lastPartitions
	if ps != nil {
		ps.refs.Add(1)
	}
	var once sync.Once
	return ps, func() {
		once.Do(func() {
			if ps != nil && ps.refs.Add(-1) == 0 {
				kvs.reapPartitions()
			}
		})
	}
}

// retirePartitions 把 next 换成当前对外可读的那一组，被它取代的那一组进入等待队列。
//
// 调用方必须持有 kvs.mu，并且必须在 kv 状态**落盘之后**才调用 reapPartitions：
// 反过来（先删文件后落盘）崩在中间就是清单指向已被删除的文件，重启直接起不来；
// 而按这个顺序崩在中间只会留下几个没人引用的文件，下一轮回收会带走它们。
func (kvs *KVServer) retirePartitions(next *PartitionSet) {
	prev := kvs.lastPartitions
	kvs.lastPartitions = next
	if prev != nil && prev != next {
		kvs.retiredPartitions = append(kvs.retiredPartitions, prev)
	}
}

// reapPartitions 删除已经没人引用的分区文件，返回删掉的文件数。
// 调用时**不能**持有 kvs.mu。
func (kvs *KVServer) reapPartitions() int {
	kvs.mu.Lock()
	var reaped, stillHeld []*PartitionSet
	for _, ps := range kvs.retiredPartitions {
		if ps.refs.Load() > 0 {
			stillHeld = append(stillHeld, ps)
		} else {
			reaped = append(reaped, ps)
		}
	}
	kvs.retiredPartitions = stillHeld
	// 仍被引用的是：当前对外可读的那一组，加上引用还没归零的退役组。
	keep := append([]*PartitionSet{kvs.lastPartitions}, stillHeld...)
	stale := unreferencedFiles(reaped, keep)
	kvs.mu.Unlock()

	var removed int
	for _, f := range stale {
		if err := os.Remove(f); err != nil {
			if !os.IsNotExist(err) {
				fmt.Printf("删除已废弃的分区 %s 失败: %v\n", f, err)
			}
			continue
		}
		removed++
	}
	if len(stillHeld) > 0 {
		fmt.Printf("[GC] %d 个退役分区组仍被引用（快照传输中），它们的文件暂不删除\n", len(stillHeld))
	}
	return removed
}
