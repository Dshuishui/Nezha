package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 路由是 P1 唯一新增的读路径逻辑，也是整套分区设计正确性的支点：路由错了就是静默返回
// "这个 key 不存在"——不报错、不崩溃，只是丢数据。这里把每种边界都钉住。

// mkSet 造一组 key 区间互不重叠的分区。不涉及文件，只测路由。
func mkSet(ranges ...[2]string) *PartitionSet {
	ps := &PartitionSet{}
	for _, r := range ranges {
		ps.parts = append(ps.parts, &SortedFileIndex{Lo: r[0], Hi: r[1], FilePath: r[0] + "-" + r[1]})
	}
	return ps
}

func TestPartitionFind(t *testing.T) {
	// 三个分区，两两之间刻意留出空隙：[100,199] [300,399] [500,599]
	ps := mkSet([2]string{"100", "199"}, [2]string{"300", "399"}, [2]string{"500", "599"})

	cases := []struct {
		key  string
		want string // 期望命中的分区 Lo；"" 表示应当返回 nil
		desc string
	}{
		{"100", "100", "命中首分区下界"},
		{"150", "100", "落在首分区中间"},
		{"199", "100", "命中首分区上界"},
		{"300", "300", "命中中间分区下界"},
		{"399", "300", "命中中间分区上界"},
		{"599", "500", "命中末分区上界"},
		{"050", "", "比所有分区都小"},
		{"250", "", "落在第一、二分区之间的空隙"},
		{"450", "", "落在第二、三分区之间的空隙"},
		{"700", "", "比所有分区都大"},
	}
	for _, c := range cases {
		got := ps.find(c.key)
		switch {
		case c.want == "" && got != nil:
			t.Errorf("%s: find(%s) = 分区 %s; want nil", c.desc, c.key, got.Lo)
		case c.want != "" && got == nil:
			t.Errorf("%s: find(%s) = nil; want 分区 %s", c.desc, c.key, c.want)
		case c.want != "" && got.Lo != c.want:
			t.Errorf("%s: find(%s) = 分区 %s; want %s", c.desc, c.key, got.Lo, c.want)
		}
	}
}

func TestPartitionFindEdgeSets(t *testing.T) {
	if got := (*PartitionSet)(nil).find("100"); got != nil {
		t.Errorf("nil 集合应返回 nil，得到 %v", got)
	}
	if got := mkSet().find("100"); got != nil {
		t.Errorf("空集合应返回 nil，得到 %v", got)
	}
	// 单分区是最常见的退化情形（数据量小于一个分区），必须与改造前行为一致
	one := mkSet([2]string{"100", "199"})
	if got := one.find("150"); got == nil || got.Lo != "100" {
		t.Errorf("单分区命中失败: %v", got)
	}
	if got := one.find("250"); got != nil {
		t.Errorf("单分区越界应返回 nil，得到 %v", got.Lo)
	}
}

func TestPartitionOverlapping(t *testing.T) {
	ps := mkSet([2]string{"100", "199"}, [2]string{"300", "399"}, [2]string{"500", "599"})

	cases := []struct {
		lo, hi string
		want   []string // 期望命中的分区 Lo，按序
		desc   string
	}{
		{"100", "199", []string{"100"}, "恰好一个分区"},
		{"150", "160", []string{"100"}, "窄范围落在单个分区内"},
		{"150", "350", []string{"100", "300"}, "跨两个分区"},
		{"000", "999", []string{"100", "300", "500"}, "全范围触及全部分区"},
		{"199", "300", []string{"100", "300"}, "两端各贴一个分区的边界"},
		{"210", "290", nil, "整段落在空隙里"},
		{"000", "050", nil, "整段在所有分区之前"},
		{"700", "800", nil, "整段在所有分区之后"},
		{"400", "100", nil, "起点大于终点"},
	}
	for _, c := range cases {
		got := ps.overlapping(c.lo, c.hi)
		if len(got) != len(c.want) {
			t.Errorf("%s: overlapping(%s,%s) 命中 %d 个; want %d", c.desc, c.lo, c.hi, len(got), len(c.want))
			continue
		}
		for i, p := range got {
			if p.Lo != c.want[i] {
				t.Errorf("%s: overlapping(%s,%s)[%d] = %s; want %s", c.desc, c.lo, c.hi, i, p.Lo, c.want[i])
			}
		}
	}
}

// newTestServer 造一个够用的 KVServer：分区写入器只用到 persister 的 key padding
// 与几个尺寸配置，不碰 Raft、不碰 RocksDB 实例。
func newTestServer(targetBytes int64) *KVServer {
	return &KVServer{
		persister:            &raft.Persister{},
		indexBlockBytes:      defaultIndexBlockBytes,
		inlineCacheBytes:     1 << 20,
		inlineThreshold:      512,
		partitionTargetBytes: targetBytes,
	}
}

func writeEntries(t *testing.T, kvs *KVServer, base string, n, valueSize int) *PartitionSet {
	t.Helper()
	pw := kvs.newPartitionWriter(base)
	for i := 0; i < n; i++ {
		key := kvs.persister.PadKey(fmt.Sprintf("%d", i))
		if err := pw.Add(&raft.Entry{Key: key, Value: string(make([]byte, valueSize))}); err != nil {
			pw.Abort()
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	ps, err := pw.Finish()
	if err != nil {
		pw.Abort()
		t.Fatalf("Finish: %v", err)
	}
	return ps
}

// 写入器滚动之后，读路径必须还能找回每一个 key。这条覆盖的是"分区切开 → 路由 → 块内查找"
// 的完整链路，也是 P1 会不会静默丢数据的直接检验。
func TestPartitionWriterRollAndLookup(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096) // 小目标值，强制切出多个分区
	base := filepath.Join(dir, "sorted_1")

	const n, vsize = 200, 100
	ps := writeEntries(t, kvs, base, n, vsize)
	defer ps.Close()

	if ps.Len() < 2 {
		t.Fatalf("目标 4KB、总量约 %d 字节，应切出多个分区，实际 %d 个", n*(20+vsize), ps.Len())
	}

	// 区间必须严格递增且互不重叠——读路径的二分完全建立在这个前提上
	for i := 1; i < ps.Len(); i++ {
		if ps.parts[i-1].Hi >= ps.parts[i].Lo {
			t.Errorf("分区 %d 的 Hi=%q 未小于分区 %d 的 Lo=%q，区间重叠", i-1, ps.parts[i-1].Hi, i, ps.parts[i].Lo)
		}
	}

	// 每个分区都不应超出目标太多：滚动只发生在 entry 边界，最多超出一条 entry
	maxAllowed := int64(4096 + 20 + vsize + 64)
	for i, p := range ps.parts {
		if p.FileSize > maxAllowed {
			t.Errorf("分区 %d 大小 %d 超出目标上限 %d", i, p.FileSize, maxAllowed)
		}
	}

	// 每个 key 都要能通过路由找回来
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		got, err := kvs.getFromPartitions(key, ps)
		if err != nil {
			t.Fatalf("getFromPartitions(%s): %v", key, err)
		}
		if len(got) != vsize {
			t.Errorf("key %s: value 长度 %d; want %d", key, len(got), vsize)
		}
	}

	// 不存在的 key 必须报 NOKEY 而不是命中别的分区
	if _, err := kvs.getFromPartitions("999999", ps); err == nil {
		t.Error("查询不存在的 key 应当失败")
	}
}

// 空输入不能留下空文件，也不能让读路径炸掉。
func TestPartitionWriterEmpty(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096)
	base := filepath.Join(dir, "sorted_1")

	ps := writeEntries(t, kvs, base, 0, 0)
	defer ps.Close()

	if ps.Len() != 0 {
		t.Fatalf("空输入应产出 0 个分区，实际 %d", ps.Len())
	}
	if matches, _ := filepath.Glob(base + ".p*"); len(matches) != 0 {
		t.Errorf("空输入不应留下文件，实际 %v", matches)
	}
	if _, err := kvs.getFromPartitions("1", ps); err == nil {
		t.Error("空分区组的查询应当失败")
	}
	if m, err := kvs.scanFromPartitions("0", "999", ps); err != nil || len(m) != 0 {
		t.Errorf("空分区组的扫描应返回空结果，得到 %v, %v", m, err)
	}
}

// Abort 必须把半成品清干净：残留的 .pN 会被下一次重做当成本轮产物的一部分。
func TestPartitionWriterAbortRemovesFiles(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096)
	base := filepath.Join(dir, "sorted_1")

	pw := kvs.newPartitionWriter(base)
	for i := 0; i < 200; i++ {
		key := kvs.persister.PadKey(fmt.Sprintf("%d", i))
		if err := pw.Add(&raft.Entry{Key: key, Value: string(make([]byte, 100))}); err != nil {
			t.Fatalf("Add(%d): %v", i, err)
		}
	}
	if matches, _ := filepath.Glob(base + ".p*"); len(matches) == 0 {
		t.Fatal("前置条件不成立：此时应已封口若干分区")
	}

	pw.Abort()
	matches, err := filepath.Glob(base + ".p*")
	if err != nil {
		t.Fatalf("glob: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("Abort 之后仍残留 %v", matches)
	}
}

// 清单要能跨重启还原出同一组分区，且字节数对不上时必须拒绝——那意味着某个分区没有完整
// 落盘，继续用它就是按错误的边界判定"这里没有这个 key"。
func TestPartitionManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096)
	base := filepath.Join(dir, "sorted_1")

	const n, vsize = 200, 100
	ps := writeEntries(t, kvs, base, n, vsize)
	metas := ps.manifest()
	ps.Close()

	if len(metas) < 2 {
		t.Fatalf("前置条件不成立：应有多个分区，实际 %d", len(metas))
	}

	reloaded, err := kvs.loadPartitionSet(base, metas)
	if err != nil {
		t.Fatalf("loadPartitionSet: %v", err)
	}
	defer reloaded.Close()

	if reloaded.Len() != len(metas) {
		t.Errorf("重建后 %d 个分区; want %d", reloaded.Len(), len(metas))
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%d", i)
		if _, err := kvs.getFromPartitions(key, reloaded); err != nil {
			t.Fatalf("重建后查不到 key %s: %v", key, err)
		}
	}

	// 截断一个分区，重建必须失败而不是静默接受
	if err := os.Truncate(metas[0].Path, metas[0].Size-1); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if bad, err := kvs.loadPartitionSet(base, metas); err == nil {
		bad.Close()
		t.Error("分区被截断后 loadPartitionSet 应当报错")
	}
}

// SCAN 跨分区拼接的结果必须与"数据没被切开"时完全一致。
func TestScanAcrossPartitions(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096)
	base := filepath.Join(dir, "sorted_1")

	const n, vsize = 200, 100
	ps := writeEntries(t, kvs, base, n, vsize)
	defer ps.Close()

	// 全范围：所有 key 都要出现
	all, err := kvs.scanFromPartitions("0", "999999", ps)
	if err != nil {
		t.Fatalf("全范围 scan: %v", err)
	}
	if len(all) != n {
		t.Errorf("全范围 scan 得到 %d 条; want %d", len(all), n)
	}

	// 窄范围：只应返回区间内的 key。padding 后按字典序比较，所以直接用 padded 边界取样本
	sub, err := kvs.scanFromPartitions("10", "12", ps)
	if err != nil {
		t.Fatalf("窄范围 scan: %v", err)
	}
	if len(sub) == 0 {
		t.Error("窄范围 scan 不应为空")
	}
	loPad, hiPad := kvs.persister.PadKey("10"), kvs.persister.PadKey("12")
	for k := range sub {
		p := kvs.persister.PadKey(k)
		if p < loPad || p > hiPad {
			t.Errorf("窄范围 scan 返回了区间外的 key %q", k)
		}
	}
}

// 垃圾计量要能跨重启活下来。清零的话，已经攒下的垃圾再也不会触发压实，
// 空间放大只增不减——那正是改造要消除的问题。
func TestObsoleteFilesKeepsReusedPartitions(t *testing.T) {
	prev := mkSet([2]string{"100", "199"}, [2]string{"300", "399"}, [2]string{"500", "599"})
	for i, p := range prev.parts {
		p.FilePath = fmt.Sprintf("/d/old.p%d", i)
	}
	// 新一组：复用 prev 的第 1 个分区（同一个文件），另外两个被重写成新文件
	next := &PartitionSet{parts: []*SortedFileIndex{
		{FilePath: "/d/new.p0"},
		prev.parts[1], // 原样复用
		{FilePath: "/d/new.p1"},
	}}

	got := obsoleteFiles(prev, next)
	want := map[string]bool{"/d/old.p0": true, "/d/old.p2": true}
	if len(got) != len(want) {
		t.Fatalf("应删 %d 个，实际 %d 个: %v", len(want), len(got), got)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("不该删 %s", g)
		}
		if g == prev.parts[1].FilePath {
			t.Errorf("删掉了仍被复用的分区 %s——这是丢数据", g)
		}
	}

	if obsoleteFiles(nil, next) != nil {
		t.Error("prev 为 nil 时应返回 nil")
	}
	// 全部复用时一个都不该删
	if got := obsoleteFiles(prev, prev); len(got) != 0 {
		t.Errorf("全部复用时应删 0 个，实际 %v", got)
	}
}
