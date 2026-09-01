package raft

import "testing"

// PadKey/UnpadKey 的往返行为。这里同时钉住"能还原的"和"还原不了的"——
// 后者是 PadKey 有损带来的已知局限，写成用例是为了让它在被改动时立刻显形，
// 而不是留一个看起来无害的 TrimLeft 继续悄悄吃掉前导零。
func TestPadUnpadRoundTrip(t *testing.T) {
	p := &Persister{}

	// 能还原：不含前导零的 key，加上 "0" 这个特例
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

func TestUnpadKeyLeadingZeroIsLossy(t *testing.T) {
	p := &Persister{}
	// 记录已知局限：PadKey 把原始长度丢了，"007" 取回来是 "7"。
	// 这个断言不是在认可该行为，而是防止它在无人察觉时改变——
	// 真正修好的那天，这个用例会失败，提醒同步更新。
	if got := p.UnpadKey(p.PadKey("007")); got != "7" {
		t.Errorf("已知局限发生变化：UnpadKey(PadKey(\"007\")) = %q，此前为 \"7\"；"+
			"若已换掉填充方案，请把本用例改为期望 \"007\"", got)
	}
}
