package main

import (
	"sync"

	lru "github.com/hashicorp/golang-lru"
)

// InlineCache 是按字节预算限制的小值缓存（AVP 的加速层）。
//
// 早期实现用无界 map 把 GC 时遇到的所有小值全部驻留内存，内存随数据集线性增长：
// 100GB 的 64B 小值约需 190GB 内存，普通机器无法运行。改为有界后内存变成固定预算，
// Zipf 访问下少量内存即可覆盖绝大部分请求，未命中的冷 key 退回 sortedFile 读取。
type InlineCache struct {
	mu       sync.Mutex
	lru      *lru.Cache
	curBytes int64
	maxBytes int64
	hits     uint64
	misses   uint64
}

// NewInlineCache 创建一个字节预算为 maxBytes 的缓存；maxBytes<=0 返回 nil（表示禁用）。
func NewInlineCache(maxBytes int64) *InlineCache {
	if maxBytes <= 0 {
		return nil
	}
	// 条目数上限只作兜底，真正的约束是字节预算。按每条最小开销约 128B 估算。
	countLimit := int(maxBytes / 128)
	if countLimit < 1 {
		countLimit = 1
	}
	c := &InlineCache{maxBytes: maxBytes}
	l, err := lru.NewWithEvict(countLimit, func(k, v interface{}) {
		// 由 lru 自身按条目数淘汰时同步扣减字节计数
		if b, ok := v.([]byte); ok {
			c.curBytes -= int64(len(b)) + inlineEntryOverhead
		}
	})
	if err != nil {
		return nil
	}
	c.lru = l
	return c
}

// inlineEntryOverhead 是每条缓存除 value 字节外的估算开销（key 字符串 + LRU 链表节点 + map 槽位）
const inlineEntryOverhead = 96

func (c *InlineCache) Get(key string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if v, ok := c.lru.Get(key); ok {
		c.hits++
		return v.([]byte), true
	}
	c.misses++
	return nil, false
}

// Add 接收 string（Entry.Value 的原生类型）；转 []byte 时 Go 自带拷贝，
// 缓存不会持有调用方缓冲区的引用。
func (c *InlineCache) Add(key string, val string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := []byte(val)
	c.lru.Add(key, cp)
	c.curBytes += int64(len(cp)) + inlineEntryOverhead
	// 超预算则淘汰最旧的，直到回到预算内（onEvict 回调负责扣减 curBytes）
	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		c.lru.RemoveOldest()
	}
}

// Stats 返回命中数、未命中数、当前字节数、条目数
func (c *InlineCache) Stats() (hits, misses uint64, bytes int64, entries int) {
	if c == nil {
		return 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.curBytes, c.lru.Len()
}
