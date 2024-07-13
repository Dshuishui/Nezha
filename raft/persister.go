package raft

import (
	"gitee.com/dong-shuishui/FlexSync/util"

	// "github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/tecbot/gorocksdb"
)

type Persister struct {
	// db *leveldb.DB
	db *gorocksdb.DB
}

// func (p *Persister) Init(path string) {
// 	var err error
// 	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
// 	bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
// 	opts := gorocksdb.NewDefaultOptions()

// 	opts.SetBlockBasedTableFactory(bbto)
// 	opts.SetCreateIfMissing(true)
// 	p.db, err = gorocksdb.OpenDb(opts, path)
// 	if err != nil {
// 		util.EPrintf("Open db failed, err: %s", err)
// 	}
// }
// 设置按需开关缓存
func (p *Persister) Init(path string, disableCache bool) {
    var err error
    bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
    if !disableCache {
        bbto.SetBlockCache(gorocksdb.NewLRUCache(3 << 30))
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
	for i := uint(0); i < 8; i++ {
		valueBytes[i] = byte((value >> (i * 8)) & 0xff)		// 一个字节一个字节的转换
	}
	err := p.db.Put(wo, []byte(key), valueBytes)
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Put(key string, value string) {
	wo := gorocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	err := p.db.Put(wo, []byte(key), []byte(value))
	if err != nil {
		util.EPrintf("Put key %v value ** failed, err: %v", key, err)
	}
}

func (p *Persister) Get_opt(key string) (int64, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := p.db.Get(ro, []byte(key))
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return 0, err
	}
	defer slice.Free()
	valueBytes := slice.Data()
	if slice.Size() == 0 {
		return -1, nil
	}

	var value int64
	for i := uint(0); i < 8; i++ {
		value |= int64(valueBytes[i]) << (i * 8)
	}

	return value, nil
}

func (p *Persister) Get(key string) (string, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	slice, err := p.db.Get(ro, []byte(key))
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