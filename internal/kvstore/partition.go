package kvstore

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
// **本文件只做布局与路由（P1）。回收逻辑一行未动**：两轮上限、全量重写、无垃圾计量都还在，
// 改变的仅是产物从 1 个文件变成 N 个分区。先证明布局改造不伤读，再谈回收效率。

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
}

func (ps *PartitionSet) Len() int {
	if ps == nil {
		return 0
	}
	return len(ps.parts)
}

// Base 是这组分区的基名，第二轮 GC 用它派生归并产物的名字。
func (ps *PartitionSet) Base() string {
	if ps == nil {
		return ""
	}
	return ps.base
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

// DeadBytes 是全部分区可回收字节之和；TotalSize 是它们的总字节数。两者之比就是整组的垃圾率。
func (ps *PartitionSet) DeadBytes() int64 {
	if ps == nil {
		return 0
	}
	var n int64
	for _, p := range ps.parts {
		n += p.DeadBytes
	}
	return n
}

// dirtiest 返回垃圾率最高且超过 ratio 的那个分区；没有则返回 nil。
// 压实一次只做一个分区，取最脏的那个——同样的搬运字节数，回收的垃圾最多。
func (ps *PartitionSet) dirtiest(ratio float64) *SortedFileIndex {
	if ps == nil {
		return nil
	}
	var best *SortedFileIndex
	for _, p := range ps.parts {
		if p.deadRatio() <= ratio {
			continue
		}
		if best == nil || p.deadRatio() > best.deadRatio() {
			best = p
		}
	}
	return best
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

// openStream 按 key 序读出整组分区的全部 entry。
//
// 分区各自内部有序、区间又互不重叠，顺次拼接即得全局有序流——第二轮归并因此完全不必关心
// 上一轮的产物被切成了几个文件，它读到的仍是一条有序流。
func (ps *PartitionSet) openStream() (*bufio.Reader, func(), error) {
	if ps == nil || len(ps.parts) == 0 {
		return bufio.NewReader(bytes.NewReader(nil)), func() {}, nil
	}
	files := make([]*os.File, 0, len(ps.parts))
	readers := make([]io.Reader, 0, len(ps.parts))
	for _, p := range ps.parts {
		f, err := os.Open(p.FilePath)
		if err != nil {
			for _, o := range files {
				o.Close()
			}
			return nil, nil, fmt.Errorf("open partition %s: %v", p.FilePath, err)
		}
		files = append(files, f)
		readers = append(readers, f)
	}
	closeAll := func() {
		for _, f := range files {
			f.Close()
		}
	}
	return bufio.NewReaderSize(io.MultiReader(readers...), 1<<20), closeAll, nil
}

// ---- 清单的持久化 ----

// partitionMeta 是清单里的一项，随 KV 状态一起写盘。重启时按它直接重建分区边界，
// 不必先扫描全部数据文件才知道被切成了几段、各覆盖哪一段 key。
//
// 稀疏索引本身仍要扫文件重建（与改造前的 CreateSortedFileIndex 一样）——把索引也持久化
// 是另一件事，不在 P1 范围内。
type partitionMeta struct {
	Path string `json:"path"`
	Lo   string `json:"lo"`
	Hi   string `json:"hi"`
	Size int64  `json:"size"`
	// Entries 与 DeadBytes 必须一起持久化：重启后若把 DeadBytes 清零，已经攒下的垃圾就
	// 再也不会触发压实，空间放大只增不减——这正是改造要消除的那个问题。
	Entries   int   `json:"entries"`
	DeadBytes int64 `json:"dead_bytes"`
}

func (ps *PartitionSet) manifest() []partitionMeta {
	if ps == nil {
		return nil
	}
	out := make([]partitionMeta, 0, len(ps.parts))
	for _, p := range ps.parts {
		out = append(out, partitionMeta{
			Path: p.FilePath, Lo: p.Lo, Hi: p.Hi, Size: p.FileSize,
			Entries: p.Entries, DeadBytes: p.DeadBytes,
		})
	}
	return out
}

// loadPartitionSet 按清单重建一组分区：边界取自清单，稀疏索引扫文件重建。
func (kvs *KVServer) loadPartitionSet(base string, metas []partitionMeta) (*PartitionSet, error) {
	ps := &PartitionSet{base: base, inline: NewInlineCache(kvs.inlineCacheBytes)}
	for _, m := range metas {
		// 先比字节数再重建索引。反过来的话，被截断的分区会先在扫描时撞上
		// "unexpected EOF"——那个错误既指不出是哪里不对，也不说明期望多长。
		// 清单说的字节数与文件实际长度对不上，意味着这个分区没有完整落盘或被改动过；
		// 继续用它会让读路径按错误的边界判定"这里没有这个 key"，那是静默丢数据。
		st, err := os.Stat(m.Path)
		if err != nil {
			ps.Close()
			return nil, fmt.Errorf("partition %s: %v", m.Path, err)
		}
		if st.Size() != m.Size {
			ps.Close()
			return nil, fmt.Errorf("partition %s: manifest says %d bytes, file has %d", m.Path, m.Size, st.Size())
		}
		sparse, size, err := kvs.BuildSparseIndex(m.Path, kvs.indexBlockBytes)
		if err != nil {
			ps.Close()
			return nil, fmt.Errorf("rebuild sparse index for %s: %v", m.Path, err)
		}
		if size != m.Size {
			// 长度对得上但解析出来的字节数不对：文件被改过内容而非长度
			ps.Close()
			return nil, fmt.Errorf("partition %s: manifest says %d bytes, parsed %d", m.Path, m.Size, size)
		}
		pool, err := NewFileDescriptorPool(m.Path, partitionPoolSize)
		if err != nil {
			ps.Close()
			return nil, fmt.Errorf("file descriptor pool for %s: %v", m.Path, err)
		}
		ps.parts = append(ps.parts, &SortedFileIndex{
			Sparse:       sparse,
			FileSize:     size,
			InlineValues: ps.inline,
			FilePath:     m.Path,
			Lo:           m.Lo,
			Hi:           m.Hi,
			pool:         pool,
			Entries:      m.Entries,
			DeadBytes:    m.DeadBytes,
		})
	}
	return ps, nil
}

// ---- 写入 ----

func partitionPath(base string, i int) string { return fmt.Sprintf("%s.p%d", base, i) }

// obsoleteFiles 返回 prev 里已经不再被 next 引用的那些分区文件。
//
// 吸收会把没被尾部碰到的分区**原样复用**进新一组，它们的文件仍在被引用，删掉就是丢数据。
// 所以不能按"上一组的基名"整体清理，必须按路径逐个比对。
func obsoleteFiles(prev, next *PartitionSet) []string {
	if prev == nil {
		return nil
	}
	keep := make(map[string]bool, next.Len())
	for _, p := range next.Paths() {
		keep[p] = true
	}
	var out []string
	for _, p := range prev.Paths() {
		if !keep[p] {
			out = append(out, p)
		}
	}
	return out
}

// removePartitionFiles 删除一组分区留下的全部文件。崩溃重做前要先清掉上次写了一半的产物，
// 否则残留的 .pN 会被下一次写入跳过或覆盖到一半。
func removePartitionFiles(base string) error {
	matches, err := filepath.Glob(base + ".p*")
	if err != nil {
		return err
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			return err
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
		pw.inline.Add(pw.kvs.persister.UnpadKey(entry.Key), entry.Value)
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

	pool, err := NewFileDescriptorPool(path, partitionPoolSize)
	if err != nil {
		return fmt.Errorf("file descriptor pool for %s: %v", path, err)
	}
	pw.done = append(pw.done, &SortedFileIndex{
		Sparse:       pw.sparse.Build(),
		FileSize:     pw.offset,
		InlineValues: pw.inline,
		FilePath:     path,
		Lo:           pw.lo,
		Hi:           pw.hi,
		pool:         pool,
		Entries:      pw.n,
		// 刚写出来的分区里每一条都是最新版本，垃圾为零。它由后续的吸收累加。
		DeadBytes: 0,
	})
	pw.total += pw.offset
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
