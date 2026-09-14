package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func mkIndex(entries []SparseEntry, fileSize int64) *SortedFileIndex {
	return &SortedFileIndex{Sparse: entries, FileSize: fileSize}
}

// 块边界必须正确：最后一块用 FileSize 收尾
func TestBlockRange(t *testing.T) {
	idx := mkIndex([]SparseEntry{
		{PaddedKey: "0000000100", Offset: 0},
		{PaddedKey: "0000000200", Offset: 1000},
		{PaddedKey: "0000000300", Offset: 2000},
	}, 3000)

	cases := []struct {
		key          string
		wantS, wantE int64
		wantOK       bool
		desc         string
	}{
		{"0000000100", 0, 1000, true, "命中首块首key"},
		{"0000000150", 0, 1000, true, "落在首块中间"},
		{"0000000200", 1000, 2000, true, "命中第二块首key"},
		{"0000000250", 1000, 2000, true, "落在第二块中间"},
		{"0000000300", 2000, 3000, true, "命中末块，右边界用 FileSize"},
		{"0000009999", 2000, 3000, true, "大于所有key，仍归入末块"},
		{"0000000050", 0, 0, false, "小于所有key，应 ok=false"},
	}
	for _, c := range cases {
		s, e, ok := idx.blockRange(c.key)
		if ok != c.wantOK || (ok && (s != c.wantS || e != c.wantE)) {
			t.Errorf("%s: blockRange(%s) = %d,%d,%v; want %d,%d,%v",
				c.desc, c.key, s, e, ok, c.wantS, c.wantE, c.wantOK)
		}
	}
}

// 范围查询起点：key 小于全部时应从头开始，而不是放弃
func TestFirstBlockAtOrAfter(t *testing.T) {
	idx := mkIndex([]SparseEntry{
		{PaddedKey: "0000000100", Offset: 0},
		{PaddedKey: "0000000200", Offset: 1000},
	}, 2000)

	if off, ok := idx.firstBlockAtOrAfter("0000000050"); !ok || off != 0 {
		t.Errorf("小于所有key时应从首块起扫: got %d,%v want 0,true", off, ok)
	}
	if off, ok := idx.firstBlockAtOrAfter("0000000250"); !ok || off != 1000 {
		t.Errorf("落在末块: got %d,%v want 1000,true", off, ok)
	}
	if _, ok := mkIndex(nil, 0).firstBlockAtOrAfter("x"); ok {
		t.Error("空索引应返回 ok=false")
	}
}

// 构建器：每块一项，且首条必建索引点
func TestSparseIndexBuilder(t *testing.T) {
	b := NewSparseIndexBuilder(100) // 100 字节一块
	var off int64
	for i := 0; i < 10; i++ { // 10 条 × 30 字节 = 300 字节
		b.Observe(fmt.Sprintf("%010d", i), off, 30)
		off += 30
	}
	got := b.Build()
	// 期望索引点落在累计字节跨过 100 的边界：第0条(0)、第4条(120)、第8条(240)
	want := []SparseEntry{
		{PaddedKey: "0000000000", Offset: 0},
		{PaddedKey: "0000000004", Offset: 120},
		{PaddedKey: "0000000008", Offset: 240},
	}
	if len(got) != len(want) {
		t.Fatalf("索引项数 = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("第%d项 = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// 索引必须严格按 key 升序，二分才成立
func TestSparseIndexIsSorted(t *testing.T) {
	b := NewSparseIndexBuilder(64)
	var off int64
	for i := 0; i < 1000; i++ {
		b.Observe(fmt.Sprintf("%010d", i), off, 20)
		off += 20
	}
	got := b.Build()
	if len(got) < 2 {
		t.Fatal("索引项太少")
	}
	for i := 1; i < len(got); i++ {
		if got[i].PaddedKey <= got[i-1].PaddedKey {
			t.Fatalf("第%d项 key 未递增: %q <= %q", i, got[i].PaddedKey, got[i-1].PaddedKey)
		}
		if got[i].Offset <= got[i-1].Offset {
			t.Fatalf("第%d项 offset 未递增: %d <= %d", i, got[i].Offset, got[i-1].Offset)
		}
	}
}

// 内存：稀疏索引项数应远小于 key 数
func TestSparseIndexMemoryReduction(t *testing.T) {
	const keys = 100000
	const entryBytes = 94 // 20B 头 + 10B key + 64B value
	b := NewSparseIndexBuilder(64 * 1024)
	var off int64
	for i := 0; i < keys; i++ {
		b.Observe(fmt.Sprintf("%010d", i), off, entryBytes)
		off += entryBytes
	}
	n := len(b.Build())
	ratio := float64(keys) / float64(n)
	if ratio < 100 {
		t.Fatalf("压缩比仅 %.0fx（%d key -> %d 索引项），期望 >100x", ratio, keys, n)
	}
	t.Logf("%d 个 key -> %d 个索引项，压缩 %.0fx", keys, n, ratio)
}

// 稀疏索引的持久化。索引原先每次启动都扫全部分区重建（约 110MB/s，100GB 要 15 分钟），
// 现在跟着分区文件一起落盘。这一组钉住"落盘的索引与扫描出来的一模一样"以及
// "任何一处对不上就退回扫描，而不是用一个错的索引"。
func TestSparseIndexPersistRoundTrip(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "part.p0")
	if err := os.WriteFile(data, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	want := []SparseEntry{
		{PaddedKey: "0000000000", Offset: 0},
		{PaddedKey: "0000000042", Offset: 4096},
		{PaddedKey: "user18446744073709551615", Offset: 8192}, // 变长 key 也要能存
	}
	if err := writeSparseIndex(data, want, 4096, 7); err != nil {
		t.Fatalf("writeSparseIndex: %v", err)
	}
	got, err := readSparseIndex(data, 4096, 7)
	if err != nil {
		t.Fatalf("readSparseIndex: %v", err)
	}
	if err := sameSparseIndex(got, want); err != nil {
		t.Fatalf("往返不一致: %v", err)
	}
}

func TestSparseIndexRejectsMismatch(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "part.p0")
	if err := os.WriteFile(data, []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	entries := []SparseEntry{{PaddedKey: "0000000000", Offset: 0}}
	if err := writeSparseIndex(data, entries, 4096, 7); err != nil {
		t.Fatal(err)
	}

	// 数据长度不符：这份索引描述的不是现在盘上这个文件
	if _, err := readSparseIndex(data, 4096, 8); err == nil {
		t.Error("数据长度不符时应当拒绝")
	}
	// 块粒度不符：索引仍然正确，但点查代价的口径会与配置不符
	if _, err := readSparseIndex(data, 8192, 7); err == nil {
		t.Error("块粒度不符时应当拒绝")
	}
	// 内容被改：校验和必须抓到
	raw, err := os.ReadFile(sparseIndexPath(data))
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(sparseIndexPath(data), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readSparseIndex(data, 4096, 7); err == nil {
		t.Error("内容被改动时应当拒绝")
	}
	// 缺失：崩在"数据已 fsync、索引未写"的窗口里就是这一种，必须是 IsNotExist
	os.Remove(sparseIndexPath(data))
	if _, err := readSparseIndex(data, 4096, 7); !os.IsNotExist(err) {
		t.Errorf("索引缺失应当报 IsNotExist，实际 %v", err)
	}
}

// 落盘的索引必须与扫描重建的逐项相同——这是 -verifyPartitions 那条路径的判据，
// 也是"信任落盘索引"这件事唯一的依据。
func TestPersistedSparseIndexMatchesRebuild(t *testing.T) {
	kvs := newTestServer(4096) // 小目标值，强制切出多个分区
	base := filepath.Join(t.TempDir(), "sorted")
	ps := writeEntries(t, kvs, base, 4000, 64)
	defer ps.Close()

	for _, p := range ps.parts {
		rebuilt, size, err := kvs.BuildSparseIndex(p.FilePath, kvs.indexBlockBytes)
		if err != nil {
			t.Fatalf("BuildSparseIndex(%s): %v", p.FilePath, err)
		}
		persisted, err := readSparseIndex(p.FilePath, kvs.indexBlockBytes, size)
		if err != nil {
			t.Fatalf("分区 %s 没有可用的旁挂索引: %v", p.FilePath, err)
		}
		if err := sameSparseIndex(persisted, rebuilt); err != nil {
			t.Errorf("分区 %s 的旁挂索引与扫描结果不一致: %v", p.FilePath, err)
		}
		if err := sameSparseIndex(persisted, p.Sparse); err != nil {
			t.Errorf("分区 %s 的旁挂索引与内存里的不一致: %v", p.FilePath, err)
		}
	}
}
