package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"gitee.com/dong-shuishui/FlexSync/raft"
)

// 稀疏块索引
//
// GC 产出的 sortedFile 是按 key 升序写入的（RocksDB 迭代器 SeekToFirst/Next 顺序），
// 且 key 定长 padding，因此字典序即为键序。早期实现为每个 key 在内存里保留一条
// key→offset 记录，内存随 key 数线性增长：100GB 的 64B 小值有约 10 亿个 key，
// 光索引就要 58GB。
//
// 这里改为每约 blockBytes 记录一个块起点：查找时二分定位到块，再在块内顺序扫描。
// 内存从 O(key 数) 降为 O(块数)——64KB 块时同样的数据集约 64MB，降约三个数量级。
// 代价是每次点查多读一个块（一次 seek + 一次顺序读），范围查询反而更快，因为
// 定位起点从线性试探变成了二分。

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

const defaultIndexBlockBytes = 64 * 1024

// Observe 记录一条刚写入的 entry。paddedKey 必须是写进文件的那个 key（已 padding），
// offset 是它的起始偏移，size 是它占用的字节数。
func (b *SparseIndexBuilder) Observe(paddedKey string, offset int64, size int64) {
	// 第一条永远建立索引点，保证 Sparse[0] 是文件中最小的 key
	if !b.started || b.sinceLast >= b.blockBytes {
		b.entries = append(b.entries, SparseEntry{PaddedKey: paddedKey, Offset: offset})
		b.sinceLast = 0
		b.started = true
	}
	b.sinceLast += size
}

func (b *SparseIndexBuilder) Build() []SparseEntry {
	return b.entries
}

// blockRange 返回可能包含 paddedKey 的块区间 [start, end)。
// paddedKey 小于文件中所有 key 时返回 ok=false。
func (sfi *SortedFileIndex) blockRange(paddedKey string) (start, end int64, ok bool) {
	n := len(sfi.Sparse)
	if n == 0 {
		return 0, 0, false
	}
	// 最后一个 PaddedKey <= paddedKey 的块
	i := sort.Search(n, func(j int) bool { return sfi.Sparse[j].PaddedKey > paddedKey }) - 1
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

// firstBlockAtOrAfter 返回第一个可能含有 >= paddedKey 的块的起始偏移。
// 供范围查询定位扫描起点：即使 paddedKey 本身不存在也能给出正确的起点。
func (sfi *SortedFileIndex) firstBlockAtOrAfter(paddedKey string) (int64, bool) {
	if len(sfi.Sparse) == 0 {
		return 0, false
	}
	if start, _, ok := sfi.blockRange(paddedKey); ok {
		return start, true
	}
	// paddedKey 比文件中所有 key 都小，从头开始扫
	return sfi.Sparse[0].Offset, true
}

// scanBlock 在 [start, end) 内顺序查找 paddedKey。
// 找到返回其 entry；块内 key 已超过目标说明不存在，返回 raft.ErrNoKey。
func (kvs *KVServer) scanBlock(index *SortedFileIndex, paddedKey string, start, end int64) (*raft.Entry, error) {
	file, err := os.Open(index.FilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, err
	}

	reader := bufio.NewReaderSize(io.LimitReader(file, end-start), 64*1024)
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if entry.Key == paddedKey {
			return entry, nil
		}
		if entry.Key > paddedKey { // 已越过目标，块内有序，后面不可能再有
			break
		}
	}
	return nil, errors.New(raft.ErrNoKey)
}

// lookupInSortedFile 通过稀疏索引查找 key 对应的 entry。
func (kvs *KVServer) lookupInSortedFile(index *SortedFileIndex, key string) (*raft.Entry, error) {
	if index == nil {
		return nil, errors.New("invalid index: index is nil")
	}
	paddedKey := kvs.persister.PadKey(key)
	start, end, ok := index.blockRange(paddedKey)
	if !ok {
		return nil, errors.New(raft.ErrNoKey)
	}
	return kvs.scanBlock(index, paddedKey, start, end)
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
