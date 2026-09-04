package raft

import (
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/errors"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/linxGnu/grocksdb"

	"strings"
	"sync"
	// "strconv"
)

const KeyLength = 10

var ErrKeyNotFound = errors.New("key not found")

// var ErrNoKey = "NOKEY"

type Persister struct {
	// db *leveldb.DB
	db   *grocksdb.DB
	ro   *grocksdb.ReadOptions
	wo   *grocksdb.WriteOptions
	muRO sync.Mutex
	muWO sync.Mutex
}

// PadKey 函数用于将给定的键填充到指定长度
func (p *Persister) PadKey(key string) string {
	// 检查键是否已经被填充：
	// 1、首先检查键的长度是否已经等于 KeyLength。
	// 2、如果长度相等，再检查是否以足够数量的 "0" 开头，这表明键可能已经被填充过。
	if len(key) == KeyLength && strings.HasPrefix(key, strings.Repeat("0", KeyLength-4)) {
		// 键已经被填充，直接返回
		return key
	}

	if len(key) > KeyLength {
		// 如果键长度超过指定长度，进行截断
		return key[:KeyLength]
	}

	// 使用0在左侧填充
	return fmt.Sprintf("%0*s", KeyLength, key)
}

// UnpadKey 去除键的填充。
//
// 已知局限：PadKey 是有损的——"0"、"00"、"000" 补齐后是同一个串，原始长度在
// 写入时就丢了，这里无从还原。因此本函数只对"不含前导零的 key"成立，另加
// key=="0" 这一个能判定的特例（全零串只可能来自它）。
//
// 换句话说 "007" 存进去、取出来会变成 "7"。要根治得换掉填充方案（例如用 0x00
// 填充，它不会与十进制 key 的字符集相撞，且左填充的排序性质不变），那会改动
// 存储格式并波及 SCAN、GC、sortedFileCache 所有读路径，不在本次修复范围内。
func (p *Persister) UnpadKey(paddedKey string) string {
	unpadded := strings.TrimLeft(paddedKey, "0")
	if unpadded == "" && paddedKey != "" {
		// 全零串：唯一可能的原始 key 就是 "0"。剥光后返回空串会让这个 key
		// 在 SCAN 结果和 sortedFileCache 里彻底失踪。
		return "0"
	}
	return unpadded
}

// disableWAL 关掉存储引擎自己的预写日志，供 PASV 使用。
//
// 设成包级而非 Init 的参数，是因为 GC 期间还会新建存储引擎实例——那些实例必须
// 继承同样的设置，否则一轮 GC 之后 PASV 会悄悄变回 Original。
var disableWAL bool

// SetDisableWAL 必须在任何 Init 之前调用。
func SetDisableWAL(v bool) { disableWAL = v }

func (p *Persister) Init(path string, disableCache bool) (*Persister, error) {
	var err error
	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	opts := grocksdb.NewDefaultOptions()

	if disableCache {
		// 完全禁用所有缓存
		bbto.SetNoBlockCache(true)               // 禁用块缓存
		bbto.SetCacheIndexAndFilterBlocks(false) // 禁用索引和过滤器块的缓存
		// bbto.SetFilterPolicy(nil)                // 禁用 Bloom Filter
		opts.SetAllowMmapReads(false) // 关闭预读/内存映射读取
	} else {
		// 启用缓存
		bbto.SetBlockCache(grocksdb.NewLRUCache(3 << 30))
		bbto.SetCacheIndexAndFilterBlocks(true)
	}

	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	p.db, err = grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, fmt.Errorf("open db failed: %w", err)
	}

	p.wo = grocksdb.NewDefaultWriteOptions()
	// PASV 的做法：去掉存储引擎自己的 WAL，消除"Raft 日志 + 存储引擎 WAL"这层
	// 双重日志。Raft 日志与 SSTable 的冗余仍在，所以它相对 Original 只有有限的
	// 改善（论文实测 +26.5%）。
	if disableWAL {
		p.wo.DisableWAL(true)
	}
	p.ro = grocksdb.NewDefaultReadOptions()

	if disableCache {
		p.ro.SetFillCache(false) // 防止读取操作填充缓存
	}

	p.muRO = sync.Mutex{}
	p.muWO = sync.Mutex{}

	return p, nil
}

func (p *Persister) Close() {
	p.muRO.Lock()
	defer p.muRO.Unlock()
	if p.ro != nil {
		p.ro.Destroy()
		p.ro = nil
	}
	p.muWO.Lock()
	defer p.muWO.Unlock()
	if p.wo != nil {
		p.wo.Destroy()
		p.wo = nil
	}
	if p.db != nil {
		p.db.Close()
		p.db = nil
	}
}

// RocksDB 里存的东西现在有两种，靠首字节区分。
//
// 原先靠长度判断（Get_opt 检查 len != 8 即报错），那在只存偏移时够用，
// 一旦开始内联 value 就会撞车：一个 7 字节的 value 加上标记正好也是 8 字节。
// 标记字节让两者无论长度如何都能分开。
const (
	TagOffset = byte(0x00) // 其后 8 字节为 valuelog 偏移（KV 分离）
	TagInline = byte(0x01) // 其后即 value 本身（小值内联）
)

// DecodeOffsetRecord 从存储引擎的一条记录里取出 valuelog 偏移。
//
// 记录格式是 [Tag, offset8] 共 9 字节，偏移在 [1:]。这件事此前散落在各处各写
// 一遍，于是同一个错误犯了两次：SCAN 的 ReadValueFromNewFile 和 GC 的主循环
// 都按 [0:8] 解析，把标记字节当成了偏移的最低位——算出来的是"真实偏移左移
// 8 位再截断"，看着像个合法偏移，seek 过去却落在文件的任意位置。
//
// GC 因此读到 EOF 而失败，SCAN 因此返回空。所有需要偏移的地方都应当走这里，
// 不要再各自解析。
func DecodeOffsetRecord(raw []byte) (int64, error) {
	if len(raw) == 0 {
		return 0, errors.New("empty offset record")
	}
	switch raw[0] {
	case TagOffset:
		if len(raw) != 9 {
			return 0, fmt.Errorf("invalid offset record size: %d", len(raw))
		}
		return int64(binary.LittleEndian.Uint64(raw[1:])), nil
	case TagInline:
		return 0, errors.New(ErrInlineValue)
	}
	return 0, fmt.Errorf("unknown record tag: 0x%02x", raw[0])
}

func (p *Persister) Put_opt(key string, value int64) {
	// 不要创建新的 wo，使用对象中已经配置好的
	// wo := grocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()

	valueBytes := make([]byte, 9)
	valueBytes[0] = TagOffset
	binary.LittleEndian.PutUint64(valueBytes[1:], uint64(value))
	paddedKey := p.PadKey(key)

	p.muWO.Lock()
	defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(paddedKey), valueBytes)
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Put(key string, value string) {
	// 不要创建新的 wo，使用对象中已经配置好的
	// wo := grocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()

	paddedKey := p.PadKey(key)

	p.muWO.Lock()
	defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(paddedKey), []byte(value))
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

// PutInline 把小 value 直接存进存储引擎，不经 valuelog。
// 读取时一次点查即可拿到 value，省去"查偏移再读日志文件"的第二次 I/O；
// 且它随存储引擎持久化，重启后依然有效，不像内存缓存要等 GC 重建。
func (p *Persister) PutInline(key string, value string) {
	buf := make([]byte, 1+len(value))
	buf[0] = TagInline
	copy(buf[1:], value)
	paddedKey := p.PadKey(key)

	p.muWO.Lock()
	defer p.muWO.Unlock()
	if err := p.db.Put(p.wo, []byte(paddedKey), buf); err != nil {
		util.EPrintf("PutInline key %v failed, err: %v", key, err)
	}
}

// GetInline 取内联 value。第二个返回值为 false 表示这个 key 不是内联存储的
// （或不存在），调用方应回落到偏移查找路径。
func (p *Persister) GetInline(key string) (string, bool) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := p.db.Get(ro, []byte(p.PadKey(key)))
	if err != nil {
		return "", false
	}
	defer slice.Free()
	if !slice.Exists() {
		return "", false
	}
	b := slice.Data()
	if len(b) == 0 || b[0] != TagInline {
		return "", false
	}
	return string(b[1:]), true
}

func (p *Persister) Get_opt(key string) (int64, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	paddedKey := p.PadKey(key)
	// fmt.Printf("Attempting to get key: %s (padded: %s)\n", key, paddedKey)
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	slice, err := p.db.Get(ro, []byte(paddedKey))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return 0, err
	}
	defer slice.Free()
	valueBytes := slice.Data()
	// if slice.Size() == 0 {
	// 	return -1, nil
	// }
	if !slice.Exists() {
		// return -1, ErrKeyNotFound
		return -1, nil
	}
	// 只认标记，不做长度兼容。曾经写过一个"len==8 视为旧格式偏移"的分支，
	// 它恰好把要防的碰撞又放了回来：7 字节的内联 value 加上标记正好 8 字节，
	// 于是被当成偏移解析。而且旧格式本身就有歧义——偏移量为 1 时，
	// 小端编码的首字节就是 0x01，与内联标记无法区分。
	// TagInline 表示这个 key 的 value 内联在存储引擎里，没有偏移可言；
	// 调用方应改走 GetInline，拿到该错误说明分流逻辑漏了一处。
	return DecodeOffsetRecord(valueBytes)
}

func (p *Persister) Get(key string) (string, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	paddedKey := p.PadKey(key)
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	slice, err := p.db.Get(ro, []byte(paddedKey))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return "", err
	}
	defer slice.Free()
	valueBytes := slice.Data()
	if slice.Size() == 0 {
		return ErrNoKey, errors.New("巴嘎，没有这个key")
	}
	return string(valueBytes), nil
}

// ScanRange 执行范围查询，使用固定长度的string类型键
func (p *Persister) ScanRange_opt(startKey, endKey string) (map[string]int64, error) {
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	result := make(map[string]int64)

	paddedStartKey := p.PadKey(startKey)
	paddedEndKey := p.PadKey(endKey)

	it := p.db.NewIterator(ro)
	defer it.Close()

	for it.Seek([]byte(paddedStartKey)); it.Valid(); it.Next() { // Valid判断键是否存在，不存在就直接下一个
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()

		// 检查是否超出范围
		if string(key.Data()) > paddedEndKey {
			break
		}

		// 解析值
		valueInt64, err := parseValueInt64(value.Data())
		if err != nil {
			return nil, fmt.Errorf("error parsing value: %v", err)
		}

		// 存储去除填充的键
		originalKey := p.UnpadKey(string(key.Data()))
		result[originalKey] = valueInt64
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	return result, nil
}

// parseValueInt64 解析值为 int64。
// 原先要求恰好 8 字节，加上标记字节后记录变成 9 字节，于是这个函数必然失败。
func parseValueInt64(value []byte) (int64, error) {
	return DecodeOffsetRecord(value)
}

func (p *Persister) GetDb() (db *grocksdb.DB) {
	return p.db
}

func (p *Persister) ScanRange(startKey, endKey string) (map[string]string, error) {
	// p.muRO.Lock()
	// defer p.muRO.Unlock()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	result := make(map[string]string)

	paddedStartKey := p.PadKey(startKey)
	paddedEndKey := p.PadKey(endKey)
	// fmt.Printf("startkey:%v,endkey:%v\n", paddedStartKey, paddedEndKey)

	it := p.db.NewIterator(ro)
	defer it.Close()

	for it.Seek([]byte(paddedStartKey)); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()

		// 检查是否超出范围
		if string(key.Data()) > paddedEndKey {
			break
		}

		// 直接使用字符串值
		valueString := string(value.Data())

		// 存储去除填充的键
		originalKey := p.UnpadKey(string(key.Data()))
		result[originalKey] = valueString
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	return result, nil
}

// ---- crash recovery: the applied log index travels with the data ----
//
// The index is kept inside the store rather than in a state file because it must agree
// exactly with the rows that made it in: a crash between two files would replay or skip an
// entry. One WriteBatch gives RocksDB atomicity, and under -syncWAL the marker has the same
// durability as the data at no extra fsync.
//
// The key starts with 0x00. PadKey only ever produces printable keys, so there is no
// collision; GC's full-store iteration and range scans skip it (see IsMetaKey).
const appliedIndexKey = "\x00applied_index"

// IsMetaKey reports whether a store key is recovery metadata rather than user data.
func IsMetaKey(k []byte) bool {
	return len(k) > 0 && k[0] == 0
}

func encodeApplied(applied int) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(applied))
	return b
}

// writeWithApplied writes one data row and the applied index in a single WriteBatch.
func (p *Persister) writeWithApplied(paddedKey []byte, value []byte, applied int) error {
	wb := grocksdb.NewWriteBatch()
	defer wb.Destroy()
	if paddedKey != nil {
		wb.Put(paddedKey, value)
	}
	wb.Put([]byte(appliedIndexKey), encodeApplied(applied))
	p.muWO.Lock()
	defer p.muWO.Unlock()
	return p.db.Write(p.wo, wb)
}

// PutOffsetApplied is Put_opt plus the applied index, as one atomic batch.
func (p *Persister) PutOffsetApplied(key string, offset int64, applied int) {
	valueBytes := make([]byte, 9)
	valueBytes[0] = TagOffset
	binary.LittleEndian.PutUint64(valueBytes[1:], uint64(offset))
	if err := p.writeWithApplied([]byte(p.PadKey(key)), valueBytes, applied); err != nil {
		util.EPrintf("PutOffsetApplied key %v failed, err: %v", key, err)
	}
}

// PutInlineApplied is PutInline plus the applied index.
func (p *Persister) PutInlineApplied(key string, value string, applied int) {
	buf := make([]byte, 1+len(value))
	buf[0] = TagInline
	copy(buf[1:], value)
	if err := p.writeWithApplied([]byte(p.PadKey(key)), buf, applied); err != nil {
		util.EPrintf("PutInlineApplied key %v failed, err: %v", key, err)
	}
}

// PutValueApplied is Put (baseline: the value itself goes into the store) plus the applied index.
func (p *Persister) PutValueApplied(key string, value string, applied int) {
	if err := p.writeWithApplied([]byte(p.PadKey(key)), []byte(value), applied); err != nil {
		util.EPrintf("PutValueApplied key %v failed, err: %v", key, err)
	}
}

// SetApplied advances only the applied index (no-op entries, or rows written to another store).
func (p *Persister) SetApplied(applied int) {
	if err := p.writeWithApplied(nil, nil, applied); err != nil {
		util.EPrintf("SetApplied %d failed, err: %v", applied, err)
	}
}

// GetApplied reads the applied index back; (0, false) when the store has none.
func (p *Persister) GetApplied() (int, bool, error) {
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := p.db.Get(ro, []byte(appliedIndexKey))
	if err != nil {
		return 0, false, err
	}
	defer v.Free()
	if !v.Exists() || v.Size() != 8 {
		return 0, false, nil
	}
	return int(binary.LittleEndian.Uint64(v.Data())), true, nil
}
