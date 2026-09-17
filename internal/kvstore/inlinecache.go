package kvstore

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
//
// 同一个 key 被重复 Add 是常态而非边角情形：读路径在**未命中之后**回填
// （read.go 的 readFromSortedFile），而 Zipf 热点下同一个热 key 很容易被多个
// 并发读者同时判为未命中，于是各自回填一次。
//
// 而 lru.Add 对已存在的 key 只是替换 value，**不触发 onEvict 回调**
// （hashicorp/golang-lru 的语义：命中则 MoveToFront + 改值并直接返回）。
// 所以旧 value 的字节必须由这里自己扣掉。漏掉的后果不是"统计数字略大"：
// curBytes 只增不减，淘汰循环因此越淘越多，直到 lru 空了仍然 curBytes > maxBytes；
// 此后每次 Add 都会被紧随其后的淘汰立刻清空，**缓存永久失效**，
// 而对外只表现为内联命中率一路掉到 0——AVP 的主要收益数字就是它。
func (c *InlineCache) Add(key string, val string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := []byte(val)
	// Peek 不改动 LRU 次序、也不计入命中/未命中，只为拿到将被替换掉的那份的大小。
	if old, ok := c.lru.Peek(key); ok {
		if b, ok := old.([]byte); ok {
			c.curBytes -= int64(len(b)) + inlineEntryOverhead
		}
	}
	c.lru.Add(key, cp)
	c.curBytes += int64(len(cp)) + inlineEntryOverhead
	// 超预算则淘汰最旧的，直到回到预算内（onEvict 回调负责扣减 curBytes）
	for c.curBytes > c.maxBytes && c.lru.Len() > 0 {
		c.lru.RemoveOldest()
	}
	// 空缓存的字节数只能是 0。单条就超预算时循环会把它淘汰干净而 curBytes 仍有残值，
	// 留着它等于把上面那个"永久失效"的状态原样保留下来。
	if c.lru.Len() == 0 {
		c.curBytes = 0
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
