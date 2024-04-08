package persister

import (
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	"github.com/syndtr/goleveldb/leveldb"
)

type Persister struct {
	// path string
	// leveldb.DB 是一个 Key-Value 数据库，相当于是一个对leveldb.DB数据库进行读写操作的接口，
	// 通过将db指针保存到Persister实例中，可以使用leveldb.DB中提供的方法来实现对数据库的读写操作。
	db *leveldb.DB
}

func (p *Persister) Init(path string) {
	var err error
	// 数据存储路径和一些初始文件
	// 打开指定路径的LevelDB数据库文件，并将返回的leveldb.DB实例赋值给p.db字段，同时将可能的错误赋值给err变量。
	p.db, err = leveldb.OpenFile(path, nil)
	if err != nil {
		util.EPrintf("Open db failed, err: %s", err)
	}
}

func (p *Persister) Put(key string, value string) {
	err := p.db.Put([]byte(key), []byte(value), nil)	// 转换成字节数组是因为LevelDB是只接受字节数组作为键和值的输入。
	if err != nil {
		util.EPrintf("Put key %s value %s failed, err: %s", key, value, err)
	}
}

func (p *Persister) Get(key string) []byte {
	value, err := p.db.Get([]byte(key), nil)
	if err != nil {
		util.EPrintf("Get key %s failed, err: %s", key, err)
		return nil	// 因为有返回值，所有需要return
	}
	return value
}
