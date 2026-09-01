package raft

import (
	"gitee.com/dong-shuishui/FlexSync/util"

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
	// p.wo.DisableWAL(true)  // pasv 这里添加，关闭 WAL
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
	// var value int64
	// for i := uint(0); i < 8; i++ {
	// 	value |= int64(valueBytes[i]) << (i * 8)
	// }
	return int64(binary.LittleEndian.Uint64(valueBytes)), nil
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

	// 如果键不存在，设定值为-1，使得在读取磁盘文件时，标志该key不存在，就不用去查找默认值为0的偏移量了
	// 遍历结束，现在检查是否有缺失的键
	// 解析起始和结束键为整数
	// 下面的不用进行，因为范围查询，针对不存在的key直接不返回即可
	// startInt, err := strconv.ParseInt(startKey, 10, 64)
	// if err != nil {
	// 	return nil, fmt.Errorf("error parsing startKey: %v", err)
	// }
	// endInt, err := strconv.ParseInt(endKey, 10, 64)
	// if err != nil {
	// 	return nil, fmt.Errorf("error parsing endKey: %v", err)
	// }
	// // 遍历结束，现在检查是否有缺失的键
	// for i := startInt; i <= endInt; i++ {
	//     // keyStr := fmt.Sprintf("%010d", i) // 生成预期的键
	// 	stringValue := strconv.FormatInt(i, 10) // 将 int64 转换为 string
	//     if _, exists := result[stringValue]; !exists {
	//         result[stringValue] = -1 // 如果键不存在，赋值默认值
	//     }
	// }

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

	// 遍历结束，现在检查是否有缺失的键.如果键不存在，设定值为NOKEY
	// 解析起始和结束键为整数
	// startInt, err := strconv.ParseInt(startKey, 10, 64)
	// if err != nil {
	// 	return nil, fmt.Errorf("error parsing startKey: %v", err)
	// }
	// endInt, err := strconv.ParseInt(endKey, 10, 64)
	// if err != nil {
	// 	return nil, fmt.Errorf("error parsing endKey: %v", err)
	// }
	// 遍历结束，现在检查是否有缺失的键
	// for i := startInt; i <= endInt; i++ {
	//     // keyStr := fmt.Sprintf("%010d", i) // 生成预期的键
	// 	stringValue := strconv.FormatInt(i, 10) // 将 int64 转换为 string
	//     if _, exists := result[stringValue]; !exists {
	//         result[stringValue] = "NOKEY" // 如果键不存在，赋值默认值
	//     }
	// }

	return result, nil
}
