package raft

import (
	"os"
	"path/filepath"
	"testing"
)

// PadKey/UnpadKey 的往返行为。
//
// 这里同时钉住"能还原的"和"还原不了的"。后者是已知局限，写成用例不是认可该行为，
// 而是让它在被改动时立刻显形——真正修好的那天这些用例会失败，提醒同步更新。
func TestPadUnpadRoundTrip(t *testing.T) {
	p := &Persister{}

	// 能还原：不含前导零、长度不超过 KeyLength 的 key，加上 "0" 这个特例
	for _, k := range []string{"0", "1", "9", "42", "1000", "999999999"} {
		if got := p.UnpadKey(p.PadKey(k)); got != k {
			t.Errorf("往返失败 key=%q -> padded=%q -> %q", k, p.PadKey(k), got)
		}
	}
}

func TestUnpadKeyZero(t *testing.T) {
	p := &Persister{}
	padded := p.PadKey("0")
	if len(padded) != KeyLength {
		t.Fatalf("PadKey(\"0\") 长度 %d，期望 %d", len(padded), KeyLength)
	}
	// 修复前这里返回空串，key "0" 于是从 SCAN 结果和 sortedFileCache 里消失
	if got := p.UnpadKey(padded); got != "0" {
		t.Errorf("UnpadKey(%q) = %q，期望 \"0\"", padded, got)
	}
}

// 以下三个用例记录 PadKey 会让不同的 key 落到同一个存储位置的情形。
// 每一种都会让先写入的 value 被后写入的静默覆盖。
//
// 论文实验碰不到：key 由 strconv.Itoa(i) 生成，既无前导零也不超过 10 字符。
// 但若把 Nezha 当通用 KV 对外提供服务，这三类 key 都会损坏数据。
// 根治要换填充方案（例如定长排序前缀 + 原始 key），会改动存储格式并波及
// SCAN、GC、sortedFileCache 所有读路径。

func TestPadKeyLeadingZeroCollides(t *testing.T) {
	p := &Persister{}
	// "007" 与 "7" 补齐后是同一个串，原始长度在写入时就丢了
	if p.PadKey("007") != p.PadKey("7") {
		t.Errorf("已知局限发生变化：PadKey(\"007\")=%q 与 PadKey(\"7\")=%q 不再相同；"+
			"若已换掉填充方案，请把本用例改为断言两者不同",
			p.PadKey("007"), p.PadKey("7"))
	}
	// 取回来只会是去掉前导零的那个
	if got := p.UnpadKey(p.PadKey("007")); got != "7" {
		t.Errorf("已知局限发生变化：UnpadKey(PadKey(\"007\")) = %q，此前为 \"7\"", got)
	}
}

func TestPadKeyTruncatesLongKeys(t *testing.T) {
	p := &Persister{}
	// 超过 KeyLength 的 key 被截断，于是只有前 KeyLength 个字符参与区分——
	// 前缀相同、仅在第 11 个字符之后不同的 key 全部撞到同一个存储位置。
	a, b := "user_00001_profile", "user_00001_settings"
	if len(a) <= KeyLength || len(b) <= KeyLength {
		t.Fatalf("用例失效：%q/%q 未超过 KeyLength=%d", a, b, KeyLength)
	}
	if a[:KeyLength] != b[:KeyLength] {
		t.Fatalf("用例失效：两个 key 的前 %d 个字符本就不同", KeyLength)
	}
	if p.PadKey(a) != p.PadKey(b) {
		t.Errorf("已知局限发生变化：PadKey(%q)=%q 与 PadKey(%q)=%q 不再相同；"+
			"若已改为拒绝超长 key 或保留完整 key，请更新本用例",
			a, p.PadKey(a), b, p.PadKey(b))
	}
}

func TestPadKeyMisreadsAlreadyPaddedKeys(t *testing.T) {
	p := &Persister{}
	// 长度恰好等于 KeyLength 且以足够多 0 开头的 key，会被判定为"已经补齐过"
	// 而原样返回，跳过补齐逻辑。对于本就该被补齐的用户 key，这是误判。
	k := "0000001234"
	if len(k) != KeyLength {
		t.Fatalf("用例失效：%q 长度不等于 KeyLength=%d", k, KeyLength)
	}
	if p.PadKey(k) != k {
		t.Errorf("已知局限发生变化：PadKey(%q) = %q，此前原样返回；"+
			"若已去掉已填充判定，请更新本用例", k, p.PadKey(k))
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
	if !IsMetaKey([]byte(appliedIndexKey)) || IsMetaKey([]byte(p.PadKey("1"))) {
		t.Fatal("IsMetaKey misclassifies keys")
	}
}
