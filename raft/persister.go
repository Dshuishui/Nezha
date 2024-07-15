package raft

import (
	"gitee.com/dong-shuishui/FlexSync/util"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/tecbot/gorocksdb"
	"fmt"
	 "encoding/binary"
	 "errors"
	 "sync"
	"strings"
	"strconv"
)

const KeyLength = 10
var ErrKeyNotFound = errors.New("key not found")

type Persister struct {
	// db *leveldb.DB
	db *gorocksdb.DB
	ro   *gorocksdb.ReadOptions
	wo   *gorocksdb.WriteOptions
    muRO sync.Mutex
	muWO sync.Mutex
}

// PadKey 函数用于将给定的键填充到指定长度
func PadKey(key string) string {
    if len(key) > KeyLength {
        // 如果键长度超过指定长度，进行截断
        return key[:KeyLength]
    }
    // 使用0在左侧填充
    return fmt.Sprintf("%0*s", KeyLength, key)
}

// UnpadKey 去除键的填充
func UnpadKey(paddedKey string) string {
	return strings.TrimLeft(paddedKey, "0")
}

// Init 初始化 RocksDB 数据库，并根据 `disableCache` 参数设置缓存
func (p *Persister) Init(path string, disableCache bool) (*Persister, error) {
    var err error
    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    if !disableCache {
        bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30)) // 开关缓存
    }
    opts := gorocksdb.NewDefaultOptions()
    opts.SetBlockBasedTableFactory(bbto)
    opts.SetCreateIfMissing(true)

    db, err := gorocksdb.OpenDb(opts, path)
    if err != nil {
        return nil, fmt.Errorf("open db failed: %w", err)
    }
	return &Persister{		// 服用读写实例
        db: db,
		wo: gorocksdb.NewDefaultWriteOptions(),
        ro: gorocksdb.NewDefaultReadOptions(),
    },nil
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

func (p *Persister) Put_opt(key string, value int64) {
	// wo := gorocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()
	valueBytes := make([]byte, 8)
	// for i := uint(0); i < 8; i++ {
	// 	valueBytes[i] = byte((value >> (i * 8)) & 0xff)		// 一个字节一个字节的转换
	// }
	binary.LittleEndian.PutUint64(valueBytes, uint64(value))
	paddedKey := PadKey(key)
	p.muWO.Lock()
    defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(paddedKey), valueBytes)
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Put(key string, value string) {
	// wo := gorocksdb.NewDefaultWriteOptions()
	// defer wo.Destroy()
	paddedKey := PadKey(key)
	p.muWO.Lock()
    defer p.muWO.Unlock()
	err := p.db.Put(p.wo, []byte(paddedKey), []byte(value))
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Get_opt(key string) (int64, error) {
	// ro := gorocksdb.NewDefaultReadOptions()
	// defer ro.Destroy()

	paddedKey := PadKey(key)
	p.muRO.Lock()
    defer p.muRO.Unlock()
	slice, err := p.db.Get(p.ro, []byte(paddedKey))
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
		return -1, ErrKeyNotFound
	}
	if len(valueBytes) != 8 {
        return 0, errors.New("invalid value size")
    }
	// var value int64
	// for i := uint(0); i < 8; i++ {
	// 	value |= int64(valueBytes[i]) << (i * 8)
	// }
	return int64(binary.LittleEndian.Uint64(valueBytes)), nil
}

func (p *Persister) Get(key string) (string, error) {
	// ro := gorocksdb.NewDefaultReadOptions()
	// defer ro.Destroy()

	paddedKey := PadKey(key)
	p.muRO.Lock()
    defer p.muRO.Unlock()
	slice, err := p.db.Get(p.ro, []byte(paddedKey))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return "", err
	}
	defer slice.Free()
	valueBytes := slice.Data()
	if slice.Size() == 0 {
		return ErrNoKey, nil
	}
	return string(valueBytes), nil
}

// ScanRange 执行范围查询，使用固定长度的string类型键
func (p *Persister) ScanRange_opt(startKey, endKey string) (map[string]int64, error) {
	p.muRO.Lock()
	defer p.muRO.Unlock()
	result := make(map[string]int64)
	
	paddedStartKey := PadKey(startKey)
	paddedEndKey := PadKey(endKey)
	
	it := p.db.NewIterator(p.ro)
	defer it.Close()
	
	for it.Seek([]byte(paddedStartKey)); it.Valid(); it.Next() {	// Valid判断键是否存在，不存在就直接下一个
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
		originalKey := UnpadKey(string(key.Data()))
		result[originalKey] = valueInt64
	}
	
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}

	// 如果键不存在，设定值为-1，使得在读取磁盘文件时，标志该key不存在，就不用去查找默认值为0的偏移量了
    // 遍历结束，现在检查是否有缺失的键
	// 解析起始和结束键为整数
	startInt, err := strconv.ParseInt(startKey, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing startKey: %v", err)
	}
	endInt, err := strconv.ParseInt(endKey, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing endKey: %v", err)
	}
	// 遍历结束，现在检查是否有缺失的键
    for i := startInt; i <= endInt; i++ {
        // keyStr := fmt.Sprintf("%010d", i) // 生成预期的键
		stringValue := strconv.FormatInt(i, 10) // 将 int64 转换为 string
        if _, exists := result[stringValue]; !exists {
            result[stringValue] = -1 // 如果键不存在，赋值默认值
        }
    }
	
	return result, nil
}

// parseValueInt64 解析值为 int64
func parseValueInt64(value []byte) (int64, error) {
	if len(value) != 8 {
		return 0, fmt.Errorf("invalid value length: expected 8, got %d", len(value))
	}
	return int64(binary.LittleEndian.Uint64(value)), nil
}

func (p *Persister) ScanRange(startKey, endKey string) (map[string]string, error) {
	p.muRO.Lock()
	defer p.muRO.Unlock()
	result := make(map[string]string)
	
	paddedStartKey := PadKey(startKey)
	paddedEndKey := PadKey(endKey)
	
	it := p.db.NewIterator(p.ro)
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
		originalKey := UnpadKey(string(key.Data()))
		result[originalKey] = valueString
	}
	
	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %v", err)
	}
	
	// 遍历结束，现在检查是否有缺失的键.如果键不存在，设定值为NOKEY
	// 解析起始和结束键为整数
	startInt, err := strconv.ParseInt(startKey, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing startKey: %v", err)
	}
	endInt, err := strconv.ParseInt(endKey, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("error parsing endKey: %v", err)
	}
	// 遍历结束，现在检查是否有缺失的键
    for i := startInt; i <= endInt; i++ {
        // keyStr := fmt.Sprintf("%010d", i) // 生成预期的键
		stringValue := strconv.FormatInt(i, 10) // 将 int64 转换为 string
        if _, exists := result[stringValue]; !exists {
            result[stringValue] = "NOKEY" // 如果键不存在，赋值默认值
        }
    }

	return result, nil
}

