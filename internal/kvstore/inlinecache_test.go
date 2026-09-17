package kvstore

import (
	"fmt"
	"testing"
)

// 预算为 0 时禁用；nil 缓存的所有方法都必须安全
func TestInlineCacheDisabled(t *testing.T) {
	c := NewInlineCache(0)
	if c != nil {
		t.Fatal("预算为 0 应返回 nil")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatal("nil 缓存 Get 应返回 false")
	}
	c.Add("k", "v") // 不能 panic
	if h, m, b, e := c.Stats(); h+m != 0 || b != 0 || e != 0 {
		t.Fatalf("nil 缓存 Stats 应全为 0，得到 %d %d %d %d", h, m, b, e)
	}
}

func TestInlineCacheGetAdd(t *testing.T) {
	c := NewInlineCache(1 << 20)
	c.Add("a", "hello")
	v, ok := c.Get("a")
	if !ok || string(v) != "hello" {
		t.Fatalf("Get(a) = %q,%v; want hello,true", v, ok)
	}
	if _, ok := c.Get("missing"); ok {
		t.Fatal("不存在的 key 应返回 false")
	}
	h, m, _, _ := c.Stats()
	if h != 1 || m != 1 {
		t.Fatalf("hits=%d misses=%d; want 1,1", h, m)
	}
}

// 核心：写入远超预算的数据后，字节数必须仍在预算内
func TestInlineCacheStaysWithinBudget(t *testing.T) {
	const budget = 64 * 1024 // 64 KB
	c := NewInlineCache(budget)
	val := string(make([]byte, 64)) // 64B value，接近 AVP 目标场景

	for i := 0; i < 20000; i++ { // 20000×(64+96) ≈ 3.2 MB，是预算的 50 倍
		c.Add(fmt.Sprintf("key%08d", i), val)
	}

	_, _, bytes, entries := c.Stats()
	if bytes > budget {
		t.Fatalf("字节数 %d 超出预算 %d", bytes, budget)
	}
	if bytes <= 0 || entries <= 0 {
		t.Fatalf("缓存不应为空: bytes=%d entries=%d", bytes, entries)
	}
	// 最近写入的应该还在（LRU 语义）
	if _, ok := c.Get("key00019999"); !ok {
		t.Fatal("最近写入的 key 应仍在缓存中")
	}
	// 最早写入的应该已被淘汰
	if _, ok := c.Get("key00000000"); ok {
		t.Fatal("最早写入的 key 应已被淘汰")
	}
}

// 缓存不能持有调用方字符串的底层内存别名
func TestInlineCacheCopiesValue(t *testing.T) {
	c := NewInlineCache(1 << 20)
	buf := []byte("original")
	c.Add("k", string(buf))
	buf[0] = 'X'
	v, _ := c.Get("k")
	if string(v) != "original" {
		t.Fatalf("缓存值被外部修改污染: %q", v)
	}
}

// 同一个 key 反复 Add：字节数必须只算一份。
//
// 既有用例全部用互不相同的 key，于是"替换已存在的 key"这一档一次都没测过。
// 而它在读路径上是常态：回填发生在未命中之后，Zipf 热点下多个并发读者会同时
// 判为未命中、各自回填一次。
func TestInlineCacheRepeatedAddCountsOnce(t *testing.T) {
	c := NewInlineCache(1 << 20)
	val := string(make([]byte, 64))
	for i := 0; i < 1000; i++ {
		c.Add("hot", val)
	}
	_, _, bytes, entries := c.Stats()
	want := int64(len(val) + inlineEntryOverhead)
	if entries != 1 {
		t.Fatalf("条目数 = %d; want 1", entries)
	}
	if bytes != want {
		t.Fatalf("字节数 = %d; want %d——同一个 key 被重复计了 %.1f 份",
			bytes, want, float64(bytes)/float64(want))
	}
}

// 替换成更大的 value：字节数要跟着变大，也不能把旧的那份留在账上。
func TestInlineCacheReplaceAdjustsBytes(t *testing.T) {
	c := NewInlineCache(1 << 20)
	c.Add("k", "tiny")
	c.Add("k", string(make([]byte, 400)))
	_, _, bytes, entries := c.Stats()
	want := int64(400 + inlineEntryOverhead)
	if entries != 1 || bytes != want {
		t.Fatalf("替换后 entries=%d bytes=%d; want 1 / %d", entries, bytes, want)
	}
	if v, ok := c.Get("k"); !ok || len(v) != 400 {
		t.Fatalf("Get 取到 %d 字节、ok=%v; want 400 / true", len(v), ok)
	}
}

// 热 key 反复回填之后，缓存必须还能用。
//
// 这是上一条的后果：字节数只增不减时，淘汰循环会一路淘到 lru 空了仍然超预算，
// 此后每次 Add 都被紧随其后的淘汰立刻清空——缓存永久失效，而对外只表现为
// 内联命中率掉到 0。
func TestInlineCacheStillWorksAfterHotKeyRefills(t *testing.T) {
	const budget = 64 * 1024
	c := NewInlineCache(budget)
	val := string(make([]byte, 64))

	for i := 0; i < 5000; i++ { // 单个热 key 反复回填，累计远超预算
		c.Add("hot", val)
	}
	if _, ok := c.Get("hot"); !ok {
		t.Fatal("热 key 自己都被淘汰掉了")
	}
	for i := 0; i < 100; i++ { // 之后正常写入一批冷 key
		c.Add(fmt.Sprintf("cold%03d", i), val)
	}

	_, _, bytes, entries := c.Stats()
	if entries == 0 {
		t.Fatalf("缓存已空——字节账目虚高把它清干净了（bytes=%d, 预算 %d）", bytes, budget)
	}
	if bytes > budget {
		t.Fatalf("字节数 %d 超出预算 %d", bytes, budget)
	}
	if _, ok := c.Get("cold099"); !ok {
		t.Fatal("刚写入的冷 key 不在缓存里——写进去就被立刻淘汰了")
	}
}

// 空 value 也是合法的 value：inlineThreshold 的判据是 len(v) < 阈值，
// 0 恒小于任何正阈值，所以空值走的就是内联路径。
// 缓存必须把它与"这个 key 不在缓存里"区分开。
func TestInlineCacheEmptyValueIsNotAMiss(t *testing.T) {
	c := NewInlineCache(1 << 20)
	c.Add("k", "")
	v, ok := c.Get("k")
	if !ok {
		t.Fatal("空 value 被当成未命中——读路径会据此去 valuelog 找一份不存在的记录")
	}
	if len(v) != 0 {
		t.Fatalf("取回 %d 字节; want 0", len(v))
	}
	_, _, bytes, entries := c.Stats()
	if entries != 1 || bytes != inlineEntryOverhead {
		t.Errorf("entries=%d bytes=%d; want 1 / %d", entries, bytes, inlineEntryOverhead)
	}
}

// 单条就超预算：淘汰干净之后字节数必须归零，否则缓存从此再也装不进任何东西。
func TestInlineCacheOversizedEntryLeavesNoResidue(t *testing.T) {
	c := NewInlineCache(128)
	c.Add("big", string(make([]byte, 4096)))
	_, _, bytes, entries := c.Stats()
	if entries != 0 {
		t.Fatalf("超预算的条目留下了 %d 条", entries)
	}
	if bytes != 0 {
		t.Fatalf("缓存空了但字节数 = %d; want 0——残值会让之后每次 Add 都被立刻清空", bytes)
	}
	// 之后正常大小的条目必须装得进去
	c.Add("ok", "small")
	if _, ok := c.Get("ok"); !ok {
		t.Fatal("正常大小的条目也装不进去了")
	}
}
