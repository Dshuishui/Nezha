package raft

import (
	"gitee.com/dong-shuishui/FlexSync/util"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/tecbot/gorocksdb"
	"fmt"
	 "encoding/binary"
	 "errors"
)

const KeyLength = 10
var ErrKeyNotFound = errors.New("key not found")

type Persister struct {
	// db *leveldb.DB
	db *gorocksdb.DB
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

// 设置按需开关缓存
func (p *Persister) Init(path string, disableCache bool) {
    var err error
    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    if !disableCache {
        bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))	// 开关缓存
    }
    opts := gorocksdb.NewDefaultOptions()

    opts.SetBlockBasedTableFactory(bbto)
    opts.SetCreateIfMissing(true)
    p.db, err = gorocksdb.OpenDb(opts, path)
    if err != nil {
        util.EPrintf("Open db failed, err: %s", err)
    }
}

func (p *Persister) Put_opt(key string, value int64) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	valueBytes := make([]byte, 8)
	// for i := uint(0); i < 8; i++ {
	// 	valueBytes[i] = byte((value >> (i * 8)) & 0xff)		// 一个字节一个字节的转换
	// }
	binary.LittleEndian.PutUint64(valueBytes, uint64(value))
	paddedKey := PadKey(key)
	err := p.db.Put(wo, []byte(paddedKey), valueBytes)
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Put(key string, value string) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	paddedKey := PadKey(key)
	err := p.db.Put(wo, []byte(paddedKey), []byte(value))
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Get_opt(key string) (int64, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	paddedKey := PadKey(key)
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
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	paddedKey := PadKey(key)
	slice, err := p.db.Get(ro, []byte(paddedKey))
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

// func (p *Persister) Init(path string) {
// 	var err error
// 	// 数据存储路径和一些初始文件
// 	// 打开指定路径的LevelDB数据库文件，并将返回的leveldb.DB实例赋值给p.db字段，同时将可能的错误赋值给err变量。
// 	p.db, err = leveldb.OpenFile(path, nil)
// 	if err != nil {
// 		util.EPrintf("Open db failed, err: %s", err)
// 	}
// }

// func (p *Persister) Put_opt(key string, value int64) {
// 	valueBytes := make([]byte, 8)
// 	for i := uint(0); i < 8; i++ {
// 		valueBytes[i] = byte((value >> (i * 8)) & 0xff) // 一个字节一个字节的转换
// 	}
// 	err := p.db.Put([]byte(key), valueBytes, nil) // 转换成字节数组是因为LevelDB是只接受字节数组作为键和值的输入。
// 	if err != nil {
// 		util.EPrintf("Put key %v value %v failed, err: %v", key, value, err)
// 	}
// }

// func (p *Persister) Put(key string, value string) {
// 	err := p.db.Put([]byte(key), []byte(value), nil) // 转换成字节数组是因为LevelDB是只接受字节数组作为键和值的输入。
// 	if err != nil {
// 		util.EPrintf("Put key %s value %s failed, err: %s", key, value, err)
// 	}
// }

// func (p *Persister) Get_opt(key string) (int64, error) {
// 	valueBytes, err := p.db.Get([]byte(key), nil)
// 	if err != nil {
// 		if err == errors.ErrNotFound {
// 			return -1, nil
// 		}
// 		util.EPrintf("Get key %s failed, err: %s", key, err)
// 		return 0, err // 因为有返回值，所有需要return
// 	}
// 	var value int64
// 	for i := uint(0); i < 8; i++ {
// 		value |= int64(valueBytes[i]) << (i * 8)
// 	}
// 	return value, nil
// }

// func (p *Persister) Get(key string) (string, error) {
// 	valueBytes, err := p.db.Get([]byte(key), nil)
// 	if err != nil {
// 		if err == errors.ErrNotFound {
// 			return ErrNoKey, nil
// 		}
// 		util.EPrintf("Get key %s failed, err: %s", key, err)
// 		return "", err // 因为有返回值，所有需要return
// 	}
// 	return string(valueBytes), nil
// }