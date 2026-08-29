package main

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
