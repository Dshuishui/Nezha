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

// newTestServer 造一个够用的 KVServer：分区写入器只用到几个尺寸配置，
// 不碰 Raft、不碰 RocksDB 实例。
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
		key := fmt.Sprintf("%010d", i) // 分区按字节序排，用定长十进制保证字节序=数值序
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
		key := fmt.Sprintf("%010d", i) // 与写入侧同一种编码；存储层不再帮两边归一化
		got, _, err := kvs.getFromPartitions(key, ps)
		if err != nil {
			t.Fatalf("getFromPartitions(%s): %v", key, err)
		}
		if len(got) != vsize {
			t.Errorf("key %s: value 长度 %d; want %d", key, len(got), vsize)
		}
	}

	// 不存在的 key 必须报 NOKEY 而不是命中别的分区
	if _, _, err := kvs.getFromPartitions("999999", ps); err == nil {
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
	if _, _, err := kvs.getFromPartitions("1", ps); err == nil {
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
		key := fmt.Sprintf("%010d", i) // 分区按字节序排，用定长十进制保证字节序=数值序
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
		key := fmt.Sprintf("%010d", i) // 与写入侧同一种编码；存储层不再帮两边归一化
		if _, _, err := kvs.getFromPartitions(key, reloaded); err != nil {
			t.Fatalf("重建后查不到 key %s: %v", key, err)
		}
	}

	// 清单里存的是**文件名**，按 base 所在目录解析——这正是数据目录可搬动的依据。
	for _, m := range metas {
		if filepath.Base(m.Path) != m.Path {
			t.Errorf("清单项 %q 不是纯文件名：存绝对路径会让数据目录搬动之后去读原目录", m.Path)
		}
	}

	// 截断一个分区，重建必须失败而不是静默接受
	victim := filepath.Join(filepath.Dir(base), metas[0].Path)
	if err := os.Truncate(victim, metas[0].Size-1); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if bad, err := kvs.loadPartitionSet(base, metas); err == nil {
		bad.Close()
		t.Error("分区被截断后 loadPartitionSet 应当报错")
	}
}

// 数据目录整体搬到别处之后，必须读新位置的文件。
//
// 清单曾存绝对路径：把数据目录复制到 B 再以 -data B 启动，节点打开的是原目录 A 里的
// 分区文件（实测 /proc/<pid>/fd 全部指向 A），B 自己那几个一个都不读。原目录还在且内容
// 变了就读到变了的数据，原目录被删了才报错——最坏情况下它不报错。
func TestPartitionSetFollowsRelocatedDataDir(t *testing.T) {
	kvs := newTestServer(4096)
	dirA := t.TempDir()
	ps := writeEntries(t, kvs, filepath.Join(dirA, "sorted_1"), 200, 100)
	metas := ps.manifest()
	ps.Close()

	// 把分区文件搬到 B（含旁挂索引子目录），A 一个不留
	dirB := t.TempDir()
	entries, err := os.ReadDir(dirA)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		src, dst := filepath.Join(dirA, e.Name()), filepath.Join(dirB, e.Name())
		if e.IsDir() {
			if err := os.MkdirAll(dst, 0o755); err != nil {
				t.Fatal(err)
			}
			sub, err := os.ReadDir(src)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range sub {
				if err := os.Rename(filepath.Join(src, f.Name()), filepath.Join(dst, f.Name())); err != nil {
					t.Fatal(err)
				}
			}
			continue
		}
		if err := os.Rename(src, dst); err != nil {
			t.Fatal(err)
		}
	}

	reloaded, err := kvs.loadPartitionSet(filepath.Join(dirB, "sorted_1"), metas)
	if err != nil {
		t.Fatalf("搬到新目录后装载失败: %v", err)
	}
	defer reloaded.Close()
	for _, p := range reloaded.Paths() {
		if filepath.Dir(p) != dirB {
			t.Fatalf("装载后仍指向 %s，应当在 %s 下", p, dirB)
		}
	}
	if _, _, err := kvs.getFromPartitions(fmt.Sprintf("%010d", 7), reloaded); err != nil {
		t.Fatalf("搬到新目录后读不到 key: %v", err)
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

	// 窄范围：只应返回区间内的 key。比较是纯字节序，所以边界也要写成写入时的定长形式
	sub, err := kvs.scanFromPartitions("0000000010", "0000000012", ps)
	if err != nil {
		t.Fatalf("窄范围 scan: %v", err)
	}
	if len(sub) == 0 {
		t.Error("窄范围 scan 不应为空")
	}
	loPad, hiPad := "0000000010", "0000000012"
	for k := range sub {
		p := k
		if p < loPad || p > hiPad {
			t.Errorf("窄范围 scan 返回了区间外的 key %q", k)
		}
	}
}

// 吸收会把没被尾部碰到的分区**原样复用**进新一组。回收必须按路径逐个比对，
// 不能按基名整体清理——删掉一个被复用的文件就是丢数据。
func TestUnreferencedFilesKeepsReusedPartitions(t *testing.T) {
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

	got := unreferencedFiles([]*PartitionSet{prev}, []*PartitionSet{next})
	// 每个被淘汰的分区带一个旁挂索引（.idx），两者必须一起删——留下描述已删文件的索引
	// 不会读错，但会一直占着盘。
	// 索引路径从 sparseIndexPath 派生，别在用例里写死：它曾与分区文件并排（xxx.p0.idx），
	// 后来挪进 index/ 子目录以躲开脚本里的 `*.p*` 通配。
	want := map[string]bool{
		"/d/old.p0": true, sparseIndexPath("/d/old.p0"): true,
		"/d/old.p2": true, sparseIndexPath("/d/old.p2"): true,
	}
	if len(got) != len(want) {
		t.Fatalf("应删 %d 个，实际 %d 个: %v", len(want), len(got), got)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("不该删 %s", g)
		}
		if g == prev.parts[1].FilePath || g == sparseIndexPath(prev.parts[1].FilePath) {
			t.Errorf("删掉了仍被复用的分区（或它的索引）%s——这是丢数据", g)
		}
	}

	if unreferencedFiles(nil, []*PartitionSet{next}) != nil {
		t.Error("没有退役组时应返回 nil")
	}
	// 全部复用时一个都不该删
	if got := unreferencedFiles([]*PartitionSet{prev}, []*PartitionSet{prev}); len(got) != 0 {
		t.Errorf("全部复用时应删 0 个，实际 %v", got)
	}
	// keep 里有多组时，任何一组引用到就得留下——这正是"退役但仍被快照钉住"的那一档。
	held := &PartitionSet{parts: []*SortedFileIndex{prev.parts[0]}}
	got = unreferencedFiles([]*PartitionSet{prev}, []*PartitionSet{next, held})
	for _, g := range got {
		if g == prev.parts[0].FilePath || g == sparseIndexPath(prev.parts[0].FilePath) {
			t.Errorf("删掉了仍被另一组引用的 %s", g)
		}
	}
	if len(got) != 2 { // 只剩 old.p2 与它的索引
		t.Errorf("应只删 2 个，实际 %v", got)
	}
}

// 快照传输期间 GC 不能删掉它正在读的分区文件：发送端按文件名逐个打开源文件流出去，
// 中途被删就是下一次 os.Open 失败，而快照已经传了一半。
func TestPinnedPartitionsSurviveGC(t *testing.T) {
	dir := t.TempDir()
	touch := func(name string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(sparseIndexPath(p)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(sparseIndexPath(p), []byte("i"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	setOf := func(paths ...string) *PartitionSet {
		ps := &PartitionSet{}
		for _, p := range paths {
			ps.parts = append(ps.parts, &SortedFileIndex{FilePath: p})
		}
		return ps
	}
	exists := func(p string) bool { _, err := os.Stat(p); return err == nil }

	oldA, oldB := touch("old.p0"), touch("old.p1")
	newA := touch("new.p0")
	kvs := &KVServer{}
	kvs.lastPartitions = setOf(oldA, oldB)

	// 钉住当前这一组，然后让 GC 换上新的一组
	pinned, release := kvs.pinPartitions()
	if pinned != kvs.lastPartitions {
		t.Fatal("钉住的不是当前那一组")
	}
	kvs.mu.Lock()
	kvs.retirePartitions(setOf(newA))
	kvs.mu.Unlock()
	if removed := kvs.reapPartitions(); removed != 0 {
		t.Errorf("被钉住期间删了 %d 个文件，应当一个都不删", removed)
	}
	if !exists(oldA) || !exists(oldB) {
		t.Error("被钉住的分区文件被删了——这会让快照传到一半失败")
	}
	if len(kvs.retiredPartitions) != 1 {
		t.Errorf("退役组应仍在等待队列里，实际 %d 组", len(kvs.retiredPartitions))
	}

	// 放开引用之后才真正删
	release()
	if exists(oldA) || exists(oldB) {
		t.Error("引用归零后旧分区文件应被删除")
	}
	if exists(sparseIndexPath(oldA)) {
		t.Error("旁挂索引应与它的数据文件一起删")
	}
	if !exists(newA) {
		t.Error("当前这一组的文件不该被删")
	}
	if len(kvs.retiredPartitions) != 0 {
		t.Errorf("回收后等待队列应为空，实际 %d 组", len(kvs.retiredPartitions))
	}
	release() // 幂等：重复放开不该再删一遍、也不该 panic
}

// 没有分区组时钉住应当是安全的空操作：调用方不必判空。
func TestPinPartitionsWithNoSet(t *testing.T) {
	kvs := &KVServer{}
	ps, release := kvs.pinPartitions()
	if ps != nil {
		t.Errorf("没有分区组时应返回 nil，实际 %v", ps)
	}
	release()
}

// 清单里的 Lo/Hi 是**路由**用的：find 与 overlapping 全靠它们决定一个 key 该去哪个分区。
// 字节数早就校验了，Lo/Hi 却一直照抄清单。错了的后果是最坏的那种——Lo 偏高或 Hi 偏低
// 会让 find 对一个确实在文件里的 key 返回 nil，而读路径把"这一处没有"当作常态，
// 于是它变成一个静默的 NOKEY。
//
// Lo 能免费精确核对：稀疏索引的第一项按构造就是文件里最小的 key。
func TestLoadPartitionSetVerifiesManifestRanges(t *testing.T) {
	dir := t.TempDir()
	kvs := newTestServer(4096)
	base := filepath.Join(dir, "sorted_1")

	ps := writeEntries(t, kvs, base, 500, 64)
	good := ps.manifest()
	ps.Close()
	if len(good) < 2 {
		t.Fatalf("需要至少两个分区才能测重叠，实际 %d", len(good))
	}

	// 健康的清单必须装得上——只测"坏的被拒"是灵敏而不特异
	if reloaded, err := kvs.loadPartitionSet(base, good); err != nil {
		t.Fatalf("健康清单被拒：%v", err)
	} else {
		reloaded.Close()
	}

	clone := func() []partitionMeta { return append([]partitionMeta(nil), good...) }

	// Lo 偏高：第一个分区的最小 key 就路由不到了
	bad := clone()
	bad[0].Lo = "9999999999"
	if got, err := kvs.loadPartitionSet(base, bad); err == nil {
		got.Close()
		t.Error("Lo 与文件首 key 不符却装上了——那些 key 会静默读成不存在")
	}

	// Hi 落在分区**中间**：后半段 key 全部路由不到，而且完全静默。
	// 这一档是判据强弱的分水岭——只核对"最后一个索引点不超过 Hi"查不出它，
	// 因为块粒度等于分区目标值时每个分区只有一个索引点、它就是首 key。
	bad = clone()
	bad[0].Hi = bad[0].Lo // 声称只含一个 key，实际含四十多个
	if got, err := kvs.loadPartitionSet(base, bad); err == nil {
		got.Close()
		t.Error("Hi 落在分区中间却装上了——它后面的 key 会静默读成不存在")
	}

	// Hi 偏高也要查出来：它会把本该落进空隙的 key 路由进这个分区，
	// 于是读到的是"这个分区里没有"，而正确答案可能在别处。
	bad = clone()
	bad[0].Hi = "9999999999"
	if got, err := kvs.loadPartitionSet(base, bad); err == nil {
		got.Close()
		t.Error("Hi 高于文件末 key 却装上了")
	}

	// 区间重叠：find/overlapping 的二分要求升序且不重叠，重叠会让它返回任意结果
	bad = clone()
	bad[1].Lo = bad[0].Lo
	if got, err := kvs.loadPartitionSet(base, bad); err == nil {
		got.Close()
		t.Error("区间重叠的清单装上了——二分的前提不成立")
	}

	// 字节数不符这条原先就有，一起钉住，免得重构时丢掉
	bad = clone()
	bad[0].Size = good[0].Size + 1
	if got, err := kvs.loadPartitionSet(base, bad); err == nil {
		got.Close()
		t.Error("字节数与文件实际长度不符却装上了")
	}
}
