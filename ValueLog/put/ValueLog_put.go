package main

import (
	"encoding/binary"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
)

// ValueLog represents the Value Log file for storing values.
type ValueLog struct {
	file      *os.File
	lock      sync.Mutex
	leveldb   *leveldb.DB
	valueLogPath string
}

// NewValueLog creates a new Value Log.
func NewValueLog(valueLogPath string, leveldbPath string) (*ValueLog, error) {
	vLog := &ValueLog{valueLogPath: valueLogPath}
	var err error
	// 以只写模式，追加模式的打开这个文件；如果指定的文件不存在，就创建这个文件。
	vLog.file, err = os.OpenFile(valueLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	vLog.leveldb, err = leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		return nil, err
	}
	return vLog, nil
}

// Put stores the key-value pair in the Value Log and updates LevelDB.
func (vl *ValueLog) Put(key []byte, value []byte) error {
	vl.lock.Lock()		// 有必要加锁限制对数据库的访问嘛
	defer vl.lock.Unlock()

	// Write <keysize, valuesize, key, value> to the Value Log.
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	buf := make([]byte, 8+keySize+valueSize)
	binary.BigEndian.PutUint32(buf[0:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valueSize)
	copy(buf[8:8+keySize], key)
	copy(buf[8+keySize:], value)
	if _, err := vl.file.Write(buf); err != nil {
		return err
	}

	// Update LevelDB with <key, addr>.
	// Here addr is a simple representation, in a real scenario, it would represent the file offset.
	addr := make([]byte, binary.MaxVarintLen64)
	// 用varint对“偏移量”进行编码，再存入addr中，使用较小的数占用较少的字节，并返回编码占用的字节数。
	n := binary.PutVarint(addr, int64(8+keySize+valueSize)) // Example offset calculation.
	return vl.leveldb.Put(key, addr[:n], nil)
}

func main() {
	// Initialize ValueLog and LevelDB (Paths would be specified here).
	valueLog, err := NewValueLog("valueLog.log", "leveldbPath")
	if err != nil {
		panic(err)
	}
	// Example Put operation.
	if err := valueLog.Put([]byte("key1"), []byte("value1")); err != nil {
		panic(err)
	}
}
