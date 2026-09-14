package raft

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// 存储层对 key 的契约：原样存、原样返回、按字节序排，唯一的限制是 NUL 开头留给元数据。
//
// 这一组用例取代了此前钉住 PadKey/UnpadKey 的五个。那五个记录的是**已知缺陷**
// （前导零撞成一个、超长静默截断、"是否已补齐"的启发式会猜错），注释里写着"真正修好
// 的那天这些用例会失败"。那天就是现在：编码搬到了 internal/client，存储层不再改写 key，
// 所以下面断言的是缺陷**不再存在**。

// 前导零曾经会丢：PadKey("007") 与 PadKey("7") 是同一个串，后写的静默覆盖先写的。
func TestStoreKeepsLeadingZerosDistinct(t *testing.T) {
	p := newTestPersister(t)
	p.PutInline("7", "seven")
	p.PutInline("007", "zero-zero-seven")
	p.PutInline("0000000007", "padded")

	for key, want := range map[string]string{
		"7":          "seven",
		"007":        "zero-zero-seven",
		"0000000007": "padded",
	} {
		got, ok := p.GetInline(key)
		if !ok {
			t.Errorf("key %q 读不回来", key)
			continue
		}
		if got != want {
			t.Errorf("key %q = %q，期望 %q —— 说明这几个 key 又被折叠到同一处了", key, got, want)
		}
	}
}

// 超长的 key 曾被截断到 KeyLength，前缀相同的 key 于是互相覆盖，且一个错都不报。
// 标准 YCSB 的 key（"user" + 64 位哈希，最长 24 字符）正是这种。
func TestStoreDoesNotTruncateLongKeys(t *testing.T) {
	p := newTestPersister(t)
	shared := "user" + strings.Repeat("9", 30)
	a, b := shared+"_profile", shared+"_settings"
	p.PutInline(a, "A")
	p.PutInline(b, "B")

	if v, ok := p.GetInline(a); !ok || v != "A" {
		t.Errorf("长 key a 读回 %q/%v，期望 \"A\"/true", v, ok)
	}
	if v, ok := p.GetInline(b); !ok || v != "B" {
		t.Errorf("长 key b 读回 %q/%v，期望 \"B\"/true —— 前 24 字符相同的 key 又撞上了", v, ok)
	}

	// 最长的一种 YCSB key
	ycsb := fmt.Sprintf("user%d", uint64(math.MaxUint64))
	if len(ycsb) != 24 {
		t.Fatalf("用例假设失效：最长的 YCSB key %q 是 %d 字符", ycsb, len(ycsb))
	}
	p.PutInline(ycsb, "ycsb")
	if v, ok := p.GetInline(ycsb); !ok || v != "ycsb" {
		t.Errorf("YCSB key 读回 %q/%v", v, ok)
	}
}

// 唯一的限制：NUL 开头留给元数据（applied index 与用户数据同库，靠首字节区分）。
// 这一条是**显式报错**，不是静默改写——静默改写正是被修掉的那个缺陷的形态。
func TestValidateKeyReservesMetaPrefix(t *testing.T) {
	for _, k := range []string{"\x00", "\x00applied_index", "\x00anything"} {
		if err := ValidateKey(k); err == nil {
			t.Errorf("key %q 以 NUL 开头，应当被拒绝", k)
		}
	}
	for _, k := range []string{"", "0", "007", "user1", strings.Repeat("x", 500)} {
		if err := ValidateKey(k); err != nil {
			t.Errorf("key %q 被拒绝了: %v", k, err)
		}
	}
}

// 范围扫描不能把元数据那一行当成用户数据返回。key 原样存之后，一个很小的 startKey
// 会让迭代器落到库首，元数据就在那里。
func TestScanSkipsMetaRow(t *testing.T) {
	p := newTestPersister(t)
	p.PutValueApplied("a", "1", 5)
	p.PutValueApplied("b", "2", 6)

	rows, err := p.ScanRange("", "z")
	if err != nil {
		t.Fatalf("ScanRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("扫到 %d 行 %v，期望只有 a/b 两行用户数据", len(rows), rows)
	}
	for k := range rows {
		if IsMetaKey([]byte(k)) {
			t.Errorf("元数据行 %q 被当成用户数据返回了", k)
		}
	}
}

// 存储引擎里现在存着两种东西：8 字节偏移，或内联的 value 本身。
// 早先靠长度区分（Get_opt 检查 len != 8），一旦开始内联就会撞车——
// 一个 7 字节的 value 加上标记正好也是 8 字节。这几个用例钉住标记字节的语义。

func newTestPersister(t *testing.T) *Persister {
	t.Helper()
	dir, err := os.MkdirTemp("", "placement")
	if err != nil {
		t.Fatal(err)
	}
	p := &Persister{}
	p, err = p.Init(dir, true)
	if err != nil {
		t.Skipf("RocksDB 不可用，跳过: %v", err)
	}
	t.Cleanup(func() { p.Close(); os.RemoveAll(dir) })
	return p
}

func TestOffsetAndInlineDoNotCollide(t *testing.T) {
	p := newTestPersister(t)

	p.Put_opt("offkey", 123456789)
	got, err := p.Get_opt("offkey")
	if err != nil {
		t.Fatalf("Get_opt: %v", err)
	}
	if got != 123456789 {
		t.Fatalf("偏移读回 %d, want 123456789", got)
	}
	if _, ok := p.GetInline("offkey"); ok {
		t.Fatal("偏移记录被误判为内联 value")
	}

	// 7 字节 value：加上标记后总长 8，正是旧的长度判别会撞车的那个尺寸
	p.PutInline("inkey", "1234567")
	v, ok := p.GetInline("inkey")
	if !ok {
		t.Fatal("内联 value 读不回来")
	}
	if v != "1234567" {
		t.Fatalf("内联 value = %q, want %q", v, "1234567")
	}
	if _, err := p.Get_opt("inkey"); err == nil {
		t.Fatal("内联记录被当成了偏移解析")
	}
}

func TestInlineRoundTripAcrossSizes(t *testing.T) {
	p := newTestPersister(t)
	cases := []string{"", "a", "1234567", "12345678", "123456789", string(make([]byte, 500))}
	for i, want := range cases {
		key := "k" + string(rune('A'+i))
		p.PutInline(key, want)
		got, ok := p.GetInline(key)
		if !ok {
			t.Fatalf("长度 %d 的 value 读不回来", len(want))
		}
		if got != want {
			t.Fatalf("长度 %d：读回 %d 字节, want %d 字节", len(want), len(got), len(want))
		}
	}
}

func TestGetInlineMissingKey(t *testing.T) {
	p := newTestPersister(t)
	if _, ok := p.GetInline("nope"); ok {
		t.Fatal("不存在的 key 报告为内联命中")
	}
}

func TestAppliedIndexTravelsWithData(t *testing.T) {
	p := &Persister{}
	if _, err := p.Init(filepath.Join(t.TempDir(), "db"), true); err != nil {
		t.Skipf("RocksDB not available: %v", err)
	}
	defer p.Close()

	if _, ok, err := p.GetApplied(); ok || err != nil {
		t.Fatalf("fresh db: ok=%v err=%v, want (false, nil)", ok, err)
	}
	p.PutOffsetApplied("42", 1234, 7)
	if a, ok, _ := p.GetApplied(); !ok || a != 7 {
		t.Fatalf("after PutOffsetApplied: %d/%v, want 7/true", a, ok)
	}
	if off, err := p.Get_opt("42"); err != nil || off != 1234 {
		t.Fatalf("data row: off=%d err=%v", off, err)
	}
	p.PutInlineApplied("43", "small", 8)
	p.PutValueApplied("44", "plain", 9)
	p.SetApplied(10)
	if a, _, _ := p.GetApplied(); a != 10 {
		t.Fatalf("applied = %d, want 10", a)
	}
	if v, ok := p.GetInline("43"); !ok || v != "small" {
		t.Fatalf("inline row: %q/%v", v, ok)
	}
	// the metadata key must never look like user data to a scan
	if !IsMetaKey([]byte(appliedIndexKey)) || IsMetaKey([]byte("1")) {
		t.Fatal("IsMetaKey misclassifies keys")
	}
}
