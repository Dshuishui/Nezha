package main

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	// "github.com/syndtr/goleveldb/leveldb/util"
)

// ValueLog represents the Value Log file for storing values.
type ValueLog struct {
	file         *os.File
	lock         sync.Mutex
	leveldb      *leveldb.DB
	valueLogPath string
}

// NewValueLog creates a new Value Log.
func NewValueLog(valueLogPath string, leveldbPath string) (*ValueLog, error) {
	vLog := &ValueLog{valueLogPath: valueLogPath}
	var err error
	vLog.file, err = os.OpenFile(valueLogPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
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
	// leveldb中会含有LOCK文件，用于防止数据库被多个进程同时访问。
	// vl.lock.Lock()
	// defer vl.lock.Unlock()

	// Calculate the position where the value will be written.
	position, err := vl.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	// Write <keysize, valuesize, key, value> to the Value Log.
	// 固定整数的长度，即四个字节
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

	// Update LevelDB with <key, position>.
	// 相当于把地址（指向keysize开始处）压缩一下
	positionBytes := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(positionBytes, position)
	return vl.leveldb.Put(key, positionBytes, nil)
}

// Get retrieves the value for a given key from the Value Log.
func (vl *ValueLog) Get(key []byte) ([]byte, error) {
	// vl.lock.Lock()
	// defer vl.lock.Unlock()

	// Retrieve the position from LevelDB.
	positionBytes, err := vl.leveldb.Get(key, nil)
	if err != nil {
		return nil, err
	}
	position, _ := binary.Varint(positionBytes)

	// Seek to the position in the Value Log.
	_, err = vl.file.Seek(position, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	// Read the key size and value size.
	var keySize, valueSize uint32
	sizeBuf := make([]byte, 8)
	if _, err := vl.file.Read(sizeBuf); err != nil {
		return nil, err
	}
	keySize = binary.BigEndian.Uint32(sizeBuf[0:4])
	valueSize = binary.BigEndian.Uint32(sizeBuf[4:8])

	// Skip over the key bytes.
	// 因为上面已经读取了keysize和valuesize，所以文件的偏移量自动往后移动了8个字节
	if _, err := vl.file.Seek(int64(keySize), os.SEEK_CUR); err != nil {
		return nil, err
	}

	// Read the value bytes.
	value := make([]byte, valueSize)
	if _, err := vl.file.Read(value); err != nil {
		return nil, err
	}

	return value, nil
}

func main() {
	// Initialize ValueLog and LevelDB (Paths would be specified here).
	// 在这个.代表的是打开的工作区或文件夹的根目录，即FlexSync。指向的是VSCode左侧侧边栏（Explorer栏）中展示的最顶层文件夹。
	valueLog, err := NewValueLog("./ValueLog/valueLog.log", "./ValueLog/leveldb_key_addr")
	if err != nil {
		panic(err)
	}
	// Example Put operation.
	if err := valueLog.Put([]byte("key3"), []byte("value3")); err != nil {
		panic(err)
	}
	// Example Get operation.
	value, err := valueLog.Get([]byte("key2"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Retrieved value: %s\n", value)
}
