package kvstore

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 稀疏块索引
//
// GC 产出的 sortedFile 是按 key 升序写入的（RocksDB 迭代器 SeekToFirst/Next 顺序），
// 且 key 定长 padding，因此字典序即为键序。早期实现为每个 key 在内存里保留一条
// key→offset 记录，内存随 key 数线性增长：100GB 的 64B 小值有约 10 亿个 key，
// 光索引就要 58GB。
//
// 这里改为每约 blockBytes 记录一个块起点：查找时二分定位到块，再在块内顺序扫描。
// 内存从 O(key 数) 降为 O(块数)——默认 4KB 块时同样的数据集约 1GB，降约两个数量级。
// 代价是每次点查要顺序解析一个块内的 entry，见 defaultIndexBlockBytes 处的实测数据。

// SparseEntry 是稀疏索引的一项：一个数据块的起始偏移及块内首条 entry 的 key。
type SparseEntry struct {
	PaddedKey string // 块内首条 entry 的 key（padding 后，定长）
	Offset    int64  // 块在 sortedFile 中的起始偏移
}

// SparseIndexBuilder 在顺序写 sortedFile 的同时构建稀疏索引。
// 调用方每写完一条 entry 就调用一次 Observe，最后用 Build 收尾。
type SparseIndexBuilder struct {
	entries    []SparseEntry
	blockBytes int64
	sinceLast  int64
	started    bool
}

func NewSparseIndexBuilder(blockBytes int64) *SparseIndexBuilder {
	if blockBytes <= 0 {
		blockBytes = defaultIndexBlockBytes
	}
	return &SparseIndexBuilder{blockBytes: blockBytes}
}

// 4KB。块尺寸直接决定点查要顺序解析多少条 entry，实测（50 万条 64B value，
// 每条 94B）GET 延迟相对稠密 map 的增幅随块内条数近似线性：
//
//	4KB(~44 条) +15% | 16KB(~174 条) +21% | 64KB(~697 条) +117%
//
// 4KB 下 SCAN 与稠密 map 持平，是内存与读代价的合理折中。
const defaultIndexBlockBytes = 4 * 1024

// Observe 记录一条刚写入的 entry。key 必须是写进文件的那个 key（已 padding），
// offset 是它的起始偏移，size 是它占用的字节数。
func (b *SparseIndexBuilder) Observe(key string, offset int64, size int64) {
	// 第一条永远建立索引点，保证 Sparse[0] 是文件中最小的 key
	if !b.started || b.sinceLast >= b.blockBytes {
		b.entries = append(b.entries, SparseEntry{PaddedKey: key, Offset: offset})
		b.sinceLast = 0
		b.started = true
	}
	b.sinceLast += size
}

func (b *SparseIndexBuilder) Build() []SparseEntry {
	return b.entries
}

// blockRange 返回可能包含 key 的块区间 [start, end)。
// key 小于文件中所有 key 时返回 ok=false。
func (sfi *SortedFileIndex) blockRange(key string) (start, end int64, ok bool) {
	n := len(sfi.Sparse)
	if n == 0 {
		return 0, 0, false
	}
	// 最后一个 PaddedKey <= key 的块
	i := sort.Search(n, func(j int) bool { return sfi.Sparse[j].PaddedKey > key }) - 1
	if i < 0 {
		return 0, 0, false
	}
	start = sfi.Sparse[i].Offset
	end = sfi.FileSize
	if i+1 < n {
		end = sfi.Sparse[i+1].Offset
	}
	return start, end, true
}

// firstBlockAtOrAfter 返回第一个可能含有 >= key 的块的起始偏移。
// 供范围查询定位扫描起点：即使 key 本身不存在也能给出正确的起点。
func (sfi *SortedFileIndex) firstBlockAtOrAfter(key string) (int64, bool) {
	if len(sfi.Sparse) == 0 {
		return 0, false
	}
	if start, _, ok := sfi.blockRange(key); ok {
		return start, true
	}
	// key 比文件中所有 key 都小，从头开始扫
	return sfi.Sparse[0].Offset, true
}

// scanBlock 在 [start, end) 内顺序查找 key。
// 找到返回其 entry；块内 key 已超过目标说明不存在，返回 raft.ErrNoKey。
func (kvs *KVServer) scanBlock(index *SortedFileIndex, key string, start, end int64) (*raft.Entry, error) {
	file, err := os.Open(index.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReaderSize(io.LimitReader(file, end-start), 64*1024)
	// 解析条数就是 AVP 每次命中所省下的工作量，逐条计数后统一上报
	scanned := 0
	for {
		entry, size, err := ReadEntry(reader, 0)
		if err != nil {
			if err == io.EOF {
				break
			}
			avpRecordScan(scanned, end-start)
			return nil, err
		}
		scanned++
		_ = size
		if entry.Key == key {
			avpRecordScan(scanned, end-start)
			return entry, nil
		}
		if entry.Key > key { // 已越过目标，块内有序，后面不可能再有
			break
		}
	}
	avpRecordScan(scanned, end-start)
	return nil, ErrKeyAbsent
}

// lookupInSortedFile 通过稀疏索引查找 key 对应的 entry。
func (kvs *KVServer) lookupInSortedFile(index *SortedFileIndex, key string) (*raft.Entry, error) {
	if index == nil {
		return nil, errors.New("invalid index: index is nil")
	}
	start, end, ok := index.blockRange(key)
	if !ok {
		return nil, ErrKeyAbsent
	}
	return kvs.scanBlock(index, key, start, end)
}

// BuildSparseIndex 扫描整个 sortedFile 重建稀疏索引（进程重启或索引重建时使用）。
func (kvs *KVServer) BuildSparseIndex(filePath string, blockBytes int64) ([]SparseEntry, int64, error) {
	if blockBytes <= 0 {
		blockBytes = defaultIndexBlockBytes
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open sorted file: %v", err)
	}
	defer file.Close()

	builder := NewSparseIndexBuilder(blockBytes)
	reader := bufio.NewReaderSize(file, 1<<20)
	var offset int64
	for {
		entry, size, err := ReadEntry(reader, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}
		builder.Observe(entry.Key, offset, size)
		offset += size
	}
	return builder.Build(), offset, nil
}

// SortedFileIndex 描述一个有序文件——GC 改造后即一个分区（见 partition.go）。
type SortedFileIndex struct {
	// Sparse 是按 key 升序的稀疏块索引，每约 indexBlockBytes 一项。
	// 查找时二分定位到块，再在块内顺序扫描，内存从 O(key 数) 降为 O(块数)。
	Sparse   []SparseEntry
	FileSize int64 // sortedFile 总长度，用于界定最后一块的右边界
	// InlineValues 是小值的有界缓存，纯读加速层，命中则免去一次文件 seek。
	// 它可以为 nil、可以随时淘汰任何条目，都不影响正确性——value 始终在 sortedFile 里。
	// 同一组分区里的每个分区都指向同一个实例，见 PartitionSet.inline。
	InlineValues *InlineCache
	FilePath     string

	// Lo、Hi 是本文件内最小与最大的 padded key，即它覆盖的 key 区间（闭区间）。
	// 同一组分区里各区间互不重叠且按 Lo 升序，读路径的二分路由完全建立在这个前提上。
	Lo string
	Hi string

	// pool 缓存本文件的只读描述符。改造前它是 KVServer 上的一个全局单例，而
	// scanFromSortedFile 又不看传进来的 index.FilePath——两者恰好总指向同一个文件才没出事。
	// 分区化之后一个进程同时持有多个有序文件，描述符必须跟着文件走。
	pool *FileDescriptorPool

	// Entries 是本分区里的记录条数，纯观测量，随清单持久化。
	Entries int
}

func (sfi *SortedFileIndex) closePool() {
	if sfi != nil && sfi.pool != nil {
		sfi.pool.Close()
		sfi.pool = nil
	}
}

// ---- 稀疏索引的持久化 ----
//
// 索引原先每次启动都靠扫描全部分区文件重建（BuildSparseIndex）。扫描速率实测约
// 110MB/s，于是启动时长正比于数据量：10GB 约 1.5 分钟，**100GB 约 15.5 分钟**——
// 那段时间节点不能服务，在三节点里会被判定为失联。而分区文件一旦封口就不可变，
// 索引完全可以跟着它一起落盘。
//
// 存成每个分区一个旁挂文件（`<分区路径>.idx`）而不是写进清单：100GB 按 4KB 块是约
// 2500 万项，塞进那个每次保存 KV 状态都要重写一遍的 JSON 里不合适。
//
// **安全退化**：旁挂文件缺失、版本不符、块粒度不同、长度或校验和对不上，一律退回扫描
// 重建。所以它只可能让启动变慢，不可能让启动读到错的索引。崩在"数据文件已 fsync、
// 旁挂文件还没写"的窗口里正是缺失这一种。

const (
	sparseIndexMagic   = "NZSI"
	sparseIndexVersion = uint32(1)
)

// sparseIndexDir 是旁挂索引所在的子目录名（与分区文件同级）。
//
// 放进子目录而不是与分区文件并排，是因为一堆脚本用 `valuelog/*.p*` 数分区个数
// （crash-recovery.sh、gc-rounds.sh），而 `xxx.p0.idx` 会被这个通配式匹配到——
// 分区计数会凭空翻倍，断言和报告全部失真。子目录一次性挡掉全部现有和将来的这类通配，
// 而 du 仍然把它算进目录大小（索引是我们真实占用的空间，本就该计入空间放大）。
const sparseIndexDir = "index"

// sparseIndexPath 是一个分区文件对应的旁挂索引路径。
func sparseIndexPath(dataPath string) string {
	return filepath.Join(filepath.Dir(dataPath), sparseIndexDir, filepath.Base(dataPath)+".idx")
}

// writeSparseIndex 把索引落到旁挂文件。必须在数据文件 fsync **之后**调用：
// 反过来的话，旁挂文件可能描述一份还没落盘的数据。
func writeSparseIndex(dataPath string, sparse []SparseEntry, blockBytes, dataSize int64) error {
	var buf bytes.Buffer
	buf.WriteString(sparseIndexMagic)
	var hdr [4]byte
	binary.LittleEndian.PutUint32(hdr[:], sparseIndexVersion)
	buf.Write(hdr[:])
	var num [8]byte
	binary.LittleEndian.PutUint64(num[:], uint64(blockBytes))
	buf.Write(num[:])
	binary.LittleEndian.PutUint64(num[:], uint64(dataSize))
	buf.Write(num[:])
	binary.LittleEndian.PutUint32(hdr[:], uint32(len(sparse)))
	buf.Write(hdr[:])
	for _, e := range sparse {
		binary.LittleEndian.PutUint32(hdr[:], uint32(len(e.PaddedKey)))
		buf.Write(hdr[:])
		buf.WriteString(e.PaddedKey)
		binary.LittleEndian.PutUint64(num[:], uint64(e.Offset))
		buf.Write(num[:])
	}
	binary.LittleEndian.PutUint32(hdr[:], crc32.ChecksumIEEE(buf.Bytes()))
	buf.Write(hdr[:])

	path := sparseIndexPath(dataPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(path)
		return err
	}
	return f.Close()
}

// readSparseIndex 读回旁挂索引，并校验它描述的正是这份数据、且是同一个块粒度。
// 任何一项不符都返回错误，调用方应退回扫描重建。
func readSparseIndex(dataPath string, blockBytes, dataSize int64) ([]SparseEntry, error) {
	raw, err := os.ReadFile(sparseIndexPath(dataPath))
	if err != nil {
		return nil, err
	}
	if len(raw) < 32 { // 4 magic + 4 ver + 8 block + 8 size + 4 count + 4 crc
		return nil, fmt.Errorf("sparse index too short: %d bytes", len(raw))
	}
	body, want := raw[:len(raw)-4], binary.LittleEndian.Uint32(raw[len(raw)-4:])
	if got := crc32.ChecksumIEEE(body); got != want {
		return nil, fmt.Errorf("sparse index checksum %08x, want %08x", got, want)
	}
	if string(body[:4]) != sparseIndexMagic {
		return nil, errors.New("sparse index: bad magic")
	}
	if v := binary.LittleEndian.Uint32(body[4:8]); v != sparseIndexVersion {
		return nil, fmt.Errorf("sparse index version %d, want %d", v, sparseIndexVersion)
	}
	if b := int64(binary.LittleEndian.Uint64(body[8:16])); b != blockBytes {
		// 粒度变了（-indexBlockKB 改过）。旧索引仍然是正确的，但块大小决定点查要顺序
		// 解析多少条 entry，沿用旧粒度会让实测口径与配置不符，所以重建。
		return nil, fmt.Errorf("sparse index block size %d, configured %d", b, blockBytes)
	}
	if sz := int64(binary.LittleEndian.Uint64(body[16:24])); sz != dataSize {
		return nil, fmt.Errorf("sparse index describes %d data bytes, file has %d", sz, dataSize)
	}
	count := int(binary.LittleEndian.Uint32(body[24:28]))
	out := make([]SparseEntry, 0, count)
	pos := 28
	for i := 0; i < count; i++ {
		if pos+4 > len(body) {
			return nil, fmt.Errorf("sparse index truncated at entry %d", i)
		}
		klen := int(binary.LittleEndian.Uint32(body[pos : pos+4]))
		pos += 4
		if pos+klen+8 > len(body) {
			return nil, fmt.Errorf("sparse index truncated at entry %d", i)
		}
		key := string(body[pos : pos+klen])
		pos += klen
		off := int64(binary.LittleEndian.Uint64(body[pos : pos+8]))
		pos += 8
		out = append(out, SparseEntry{PaddedKey: key, Offset: off})
	}
	if pos != len(body) {
		return nil, fmt.Errorf("sparse index has %d trailing bytes", len(body)-pos)
	}
	return out, nil
}

// loadOrRebuildSparseIndex 取一个分区的稀疏索引：先试旁挂文件，不可用就扫描重建。
//
// 返回的第二个值是按记录逐条解析出来的字节数，装载路径用它与清单核对。走旁挂文件时
// 没有逐条解析，只能以清单长度为准——**这里少掉了一层校验**：扫描重建会顺带发现
// "长度对得上但内容被改过"（记录框架解析不下去）。截断一个字节这类改动仍然被
// loadPartitionSet 的长度检查拦住（崩溃恢复场景 C），但同长度的内容篡改在快路径上
// 不再被发现。要拿回那层校验就用 -verifyPartitions 启动，它强制扫描并与旁挂索引比对。
//
// 第三个返回值表示索引是从旁挂文件读来的（true）还是扫描重建的（false），供装载日志统计。
func (kvs *KVServer) loadOrRebuildSparseIndex(path string, manifestSize int64) ([]SparseEntry, int64, bool, error) {
	if kvs.verifyPartitions {
		sparse, size, err := kvs.BuildSparseIndex(path, kvs.indexBlockBytes)
		if err != nil {
			return nil, 0, false, fmt.Errorf("rebuild sparse index for %s: %v", path, err)
		}
		if size != manifestSize {
			return nil, 0, false, fmt.Errorf("partition %s: manifest says %d bytes, parsed %d", path, manifestSize, size)
		}
		// 旁挂索引在这条路径上不是被信任的，而是被检查的：它和扫描结果不一致说明
		// 落盘的索引有 bug，此刻宁可直接失败，也不要带着一个错的索引去服务读。
		if persisted, err := readSparseIndex(path, kvs.indexBlockBytes, manifestSize); err == nil {
			if err := sameSparseIndex(persisted, sparse); err != nil {
				return nil, 0, false, fmt.Errorf("partition %s: 旁挂索引与扫描结果不一致: %v", path, err)
			}
		}
		return sparse, size, false, nil
	}

	if sparse, err := readSparseIndex(path, kvs.indexBlockBytes, manifestSize); err == nil {
		return sparse, manifestSize, true, nil
	} else if !os.IsNotExist(err) {
		// 缺失是正常的（旧数据目录、或崩在数据已落盘而索引未写的窗口里），不值得报。
		// 其它原因要说出来：它意味着落盘的索引有问题，而不只是没有。
		fmt.Printf("[RECOVER] 分区 %s 的旁挂索引不可用，改为扫描重建: %v\n", path, err)
	}
	sparse, size, err := kvs.BuildSparseIndex(path, kvs.indexBlockBytes)
	if err != nil {
		return nil, 0, false, fmt.Errorf("rebuild sparse index for %s: %v", path, err)
	}
	if size != manifestSize {
		// 长度对得上但解析出来的字节数不对：文件被改过内容而非长度
		return nil, 0, false, fmt.Errorf("partition %s: manifest says %d bytes, parsed %d", path, manifestSize, size)
	}
	// 顺手把重建结果补写成旁挂文件，下次启动就不必再扫一遍。
	if err := writeSparseIndex(path, sparse, kvs.indexBlockBytes, size); err != nil {
		fmt.Printf("[RECOVER] 补写分区 %s 的旁挂索引失败（下次仍会扫描）: %v\n", path, err)
	}
	return sparse, size, false, nil
}

// sameSparseIndex 比较两份索引是否逐项相同。
func sameSparseIndex(a, b []SparseEntry) error {
	if len(a) != len(b) {
		return fmt.Errorf("条目数 %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Errorf("第 %d 项 %v vs %v", i, a[i], b[i])
		}
	}
	return nil
}
