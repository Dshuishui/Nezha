package kvstore

import "testing"

// inlineThreshold 的边界此前没有任何覆盖，而它的判据被抄在三处
// （apply.go 写入、partition.go GC 预热、read.go 读回填）。
// 三处只要有一处的比较写反，长度正好落在阈值上的 value 就会被一边写成内联、
// 另一边按偏移去 valuelog 找——一个只在某个特定长度上出现的 NOKEY。
//
// 现在三处都走 shouldInline，这里钉住它的语义。

func TestShouldInlineBoundaries(t *testing.T) {
	kvs := &KVServer{inlineThreshold: 512}
	cases := []struct {
		name string
		n    int
		want bool
	}{
		{"空 value", 0, true},
		{"阈值减一", 511, true},
		{"正好等于阈值", 512, false}, // 严格小于：等于阈值的不内联
		{"阈值加一", 513, false},
		{"远大于阈值", 1 << 20, false},
	}
	for _, c := range cases {
		if got := kvs.shouldInline(c.n); got != c.want {
			t.Errorf("%s：shouldInline(%d) = %v; want %v", c.name, c.n, got, c.want)
		}
	}
}

// 阈值 0：一条都不内联，空 value 也不例外（len 0 不小于 0）。
// main.go 在 -inlinePlacement 开着时会拒绝这个组合，但判据本身仍须自洽——
// 内联缓存的回填路径不看那个开关。
func TestShouldInlineZeroThresholdInlinesNothing(t *testing.T) {
	kvs := &KVServer{inlineThreshold: 0}
	for _, n := range []int{0, 1, 64, 512} {
		if kvs.shouldInline(n) {
			t.Errorf("阈值 0 时 shouldInline(%d) = true; want false", n)
		}
	}
}

// 阈值为负同样是"一条都不内联"，不能因为 len 是非负数就意外全部内联。
func TestShouldInlineNegativeThresholdInlinesNothing(t *testing.T) {
	kvs := &KVServer{inlineThreshold: -1}
	for _, n := range []int{0, 1, 64} {
		if kvs.shouldInline(n) {
			t.Errorf("阈值 -1 时 shouldInline(%d) = true; want false", n)
		}
	}
}

// 阈值极大：所有 value 都内联，valuelog 里一条 Put 记录都不该有偏移被用到。
// 这一档本身不会出错，测它是为了让"全内联"成为一个跑过的配置而不是推想。
func TestShouldInlineHugeThresholdInlinesEverything(t *testing.T) {
	kvs := &KVServer{inlineThreshold: 1 << 30}
	for _, n := range []int{0, 1, 512, 16 << 10, (1 << 30) - 1} {
		if !kvs.shouldInline(n) {
			t.Errorf("阈值 1GB 时 shouldInline(%d) = false; want true", n)
		}
	}
	if kvs.shouldInline(1 << 30) {
		t.Error("正好等于阈值仍应为 false")
	}
}
