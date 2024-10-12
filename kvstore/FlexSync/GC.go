package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"

	// "io"
	"os"
	"sort"
	"time"

	"gitee.com/dong-shuishui/FlexSync/raft"
	lru "github.com/hashicorp/golang-lru"
	"github.com/tecbot/gorocksdb"
)

func (kvs *KVServer) GarbageCollection() error {
	fmt.Println("Starting garbage collection...")
	startTime := time.Now()

	// Create a new file for sorted entries
	sortedFilePath := "/home/DYC/Gitee/FlexSync/raft/RaftState_sorted.log"
	if _, err := os.Stat(sortedFilePath); err == nil {
		fmt.Println("Sorted file already exists. Skipping garbage collection.")
		return nil
	}
	sortedFile, err := os.Create(sortedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create sorted file: %v", err)
	}
	defer sortedFile.Close()

	// Open the original RaftState.log file
	oldFile, err := os.Open(kvs.oldLog)
	if err != nil {
		return fmt.Errorf("failed to open original RaftState.log: %v", err)
	}
	defer oldFile.Close()

	// 创建新的RocksDB实例
	persister_new, err := kvs.NewPersister() // 创建一个新的用于保存key和index的persister
	if err != nil {
		return fmt.Errorf("failed to create new persister: %v", err)
	}
	newPersister, err := persister_new.Init("/home/DYC/Gitee/FlexSync/kvstore/FlexSync/db_key_index_new", true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件
	newRaftStateLogPath := "/home/DYC/Gitee/FlexSync/raft/RaftState_new.log"
	if _, err := os.Stat(newRaftStateLogPath); err == nil {
		fmt.Println("New RaftState log file already exists. Skipping creation.")
	} else if os.IsNotExist(err) {
		newRaftStateLog, err := os.Create(newRaftStateLogPath)
		if err != nil {
			return fmt.Errorf("failed to create new RaftState log: %v", err)
		}
		defer newRaftStateLog.Close()
	} else {
		return fmt.Errorf("error checking new RaftState log file: %v", err)
	}

	// kvs.startGC = true

	// 切换到新的文件和RocksDB
	kvs.SwitchToNewFiles(newRaftStateLogPath, newPersister)

	// Create a buffered writer for the sorted file
	writer := bufio.NewWriter(sortedFile)

	// Iterate through RocksDB
	it := kvs.oldPersister.GetDb().NewIterator(gorocksdb.NewDefaultReadOptions())
	defer it.Close()

	var writeNum = 0

	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()

		// Get the index from RocksDB
		index := binary.LittleEndian.Uint64(value.Data())

		// Read the entry from RaftState.log
		entry, _, err := kvs.ReadEntryAtIndex(oldFile, int64(index))
		if err != nil {
			return fmt.Errorf("failed to read entry at index %d: %v", index, err)
		}

		// Write the entry to the sorted file
		err = kvs.WriteEntryToSortedFile(writer, entry)
		if err != nil {
			return fmt.Errorf("failed to write entry to sorted file: %v", err)
		}
		writeNum++
		if writeNum%200000 == 0 {
			fmt.Printf("成功写入 %d个entry \n", writeNum)
		}
	}

	// Flush the writer to ensure all data is written
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %v", err)
	}

	// Update KVServer to use the new sorted file
	err = kvs.CreateIndex(sortedFilePath)
	if err != nil {
		fmt.Println("创建索引有问题：", err)
	}

	// 添加验证步骤
	err = VerifySortedFile(sortedFilePath)
	if err != nil {
		return fmt.Errorf("verification of sorted file failed: %v", err)
	}

	fmt.Printf("Garbage collection completed in %v\n", time.Since(startTime))
	return nil
}

func (kvs *KVServer) NewPersister() (*raft.Persister, error) {
	p := &raft.Persister{}
	return p, nil
}

func (kvs *KVServer) ReadEntryAtIndex(file *os.File, index int64) (*raft.Entry, int64, error) {
	_, err := file.Seek(index, 0)
	if err != nil {
		return nil, -1, err
	}
	reader := bufio.NewReader(file)

	// 既然有一个规律，follower的偏移量统一比leader的偏移量小一个vsize的距离，那就手动补充这个差值
	// 具体问题有时间再去解决，问题的位置应该就是发生在raft层面向服务器层传输偏移量的那里
	if kvs.raft.GetLeaderId() == int32(kvs.me) {
		return ReadEntry(reader, index)
	}
	// indexCorrct := index+int64(kvs.valueSize)
	// return ReadEntry(reader, indexCorrct) // 暂时给64000
	return ReadEntry(reader, index+64000) // 暂时给64000

	// 如果GC成功证明是这个问题，那就改为拿到客户端的值后，判断value的大小，自动计算value大小并且补上差值
}

func (kvs *KVServer) WriteEntryToSortedFile(writer *bufio.Writer, entry *raft.Entry) error {
	keySize := uint32(len(entry.Key))
	valueSize := uint32(len(entry.Value))
	data := make([]byte, 20+keySize+valueSize)

	binary.LittleEndian.PutUint32(data[0:4], entry.Index)
	binary.LittleEndian.PutUint32(data[4:8], entry.CurrentTerm)
	binary.LittleEndian.PutUint32(data[8:12], entry.VotedFor)
	binary.LittleEndian.PutUint32(data[12:16], keySize)
	binary.LittleEndian.PutUint32(data[16:20], valueSize)

	copy(data[20:20+keySize], entry.Key)
	copy(data[20+keySize:], entry.Value)

	_, err := writer.Write(data)
	return err
}

func (kvs *KVServer) CreateIndex(sortedFilePath string) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.sortedFilePath = sortedFilePath

	// 创建索引，假设每1000个条目记录一次索引，稀疏索引，间隔一部分创建一个索引，找到第一个合适的，再进行线性查询
	index, err := kvs.CreateSortedFileIndex(sortedFilePath, 90)
	if err != nil {
		// 处理错误
		return err
	}
	kvs.sortedFileIndex = index
	fmt.Println("建立了索引，得到了针对已排序文件的稀疏索引")

	return nil

	// kvs.getFromFile = kvs.getFromSortedOrNew
	// kvs.scanFromFile = kvs.scanFromSortedOrNew
}

func (kvs *KVServer) CreateSortedFileIndex(filePath string, indexInterval int) (*SortedFileIndex, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var index []IndexEntry
	var offset int64 = 0
	entryCount := 0

	for {
		// 下面的offset为起始位置，entrysize为读取了一个entry之后要移动的距离大小
		entry, entrySize, err := ReadEntry(reader, offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		UnpadKey := kvs.persister.UnpadKey(entry.Key)
		if entryCount%indexInterval == 0 {
			index = append(index, IndexEntry{Key: UnpadKey, Offset: offset})
			// fmt.Printf("Added index entry: Key=%s, Offset=%d\n", UnpadKey, offset)
		}

		offset += entrySize
		entryCount++
		// if entryCount % 10000 == 0 {
        //     fmt.Printf("Processed %d entries, current offset: %d\n", entryCount, offset)
        // }
	}

	return &SortedFileIndex{Entries: index, FilePath: filePath}, nil
}

func (kvs *KVServer) processSortedFile() ([]*raft.Entry, error) {
	// 创建LRU缓存
	// 假设我们允许缓存占用 100MB 内存，每个条目占 20B
	// 100MB / 20B = 5,000,000 个条目
	cache, _ := lru.New(5000000)

	// 打开原始文件
	file, err := os.Open("/home/DYC/Gitee/FlexSync/raft/RaftState.log")
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// 创建一个map来存储最新的entries
	latestEntries := make(map[string]*raft.Entry)

	// 读取文件并处理entries
	reader := bufio.NewReader(file)
	var currentOffset int64 = 0
	entryCount := 1
	for {
		entry, entryOffset, err := ReadEntry(reader, currentOffset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading entry: %v", err)
		}

		// 更新当前偏移量
		currentOffset = entryOffset + int64(binary.Size(entry.Index)+binary.Size(entry.CurrentTerm)+
			binary.Size(entry.VotedFor)+8+len(entry.Key)+len(entry.Value))

		// fmt.Printf("此时读出的偏移量为:%d\n", currentOffset)
		// 验证entry是否有效
		entryCount++
		// if GC4.IsValidEntry(kvs, entry, entryOffset, cache) {
		if IsValidEntry(kvs, entry, entryOffset, cache) {
			latestEntries[entry.Key] = entry
			fmt.Println("一个有效的都没有？？？？？")
		}
		if entryCount%5000 == 1 {
			fmt.Printf("Processed %d entries, current offset: %d\n", entryCount, currentOffset)
		}
	}
	fmt.Printf("有效entry个数为：%d\n", len(latestEntries))

	// 将map转换为slice并排序
	sortedEntries := make([]*raft.Entry, 0, len(latestEntries))
	for _, entry := range latestEntries {
		sortedEntries = append(sortedEntries, entry)
	}
	sort.Slice(sortedEntries, func(i, j int) bool {
		return sortedEntries[i].Key < sortedEntries[j].Key
	})
	// fmt.Println("得到已排序的entry数组")

	return sortedEntries, nil
}

func IsValidEntry(kvs *KVServer, entry *raft.Entry, entryOffset int64, cache *lru.Cache) bool {
	if cachedOffset, ok := cache.Get(entry.Key); ok {
		// rocksdb中存在，说明已经查找过，并且rocksdb中有该key
		return cachedOffset.(int64) == entryOffset
	}
	position, err := kvs.persister.Get_opt(entry.Key)
	if err != nil {
		fmt.Printf("Error getting position for key %s: %v\n", entry.Key, err)
		return false
	}
	// if position == -1 {
	// 	// 说明rocksdb中没有该key，说明肯定无效
	// 	// fmt.Printf("rocksdb中没有key:%v\n", entry.Key)
	// 	fmt.Printf("rocksdb中没有key: key=%s, file offset=%d, db position=%d\n", entry.Key, entryOffset, position)
	// 	return false
	// } else {
	// 说明rocksdb中有该key，以rocksdb中的为主
	cache.Add(entry.Key, position)
	isValid := position == entryOffset
	if !isValid {
		fmt.Printf("无效 entry: key=%s, file offset=%d, db position=%d\n", entry.Key, entryOffset, position)
	}
	return isValid
	// }
}

func (kvs *KVServer) CheckDatabaseContent() error {
	if kvs.oldPersister == nil || kvs.oldPersister.GetDb() == nil {
		return fmt.Errorf("database is not initialized")
	}

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := kvs.oldPersister.GetDb().NewIterator(ro)
	if iter == nil {
		return fmt.Errorf("failed to create iterator")
	}
	defer iter.Close()

	count := 0
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if key == nil || value == nil {
			fmt.Printf("DB entry %d: <nil key or value>\n", count)
		} else {
			keyStr := string(key.Data())
			valueBytes := value.Data()

			// 尝试将值解释为 int64
			if len(valueBytes) == 8 {
				intValue := int64(binary.LittleEndian.Uint64(valueBytes))
				fmt.Printf("DB entry %d: key=%s, value as int64=%d\n", count, keyStr, intValue)
			} else {
				// 如果不是 8 字节，则显示十六进制表示
				fmt.Printf("DB entry %d: key=%s, value (hex)=%x\n", count, keyStr, valueBytes)
			}
		}

		key.Free()
		value.Free()

		count++
		if count >= 100 {
			fmt.Println("Stopping after 100 entries...")
			break
		}
	}

	if err := iter.Err(); err != nil {
		return fmt.Errorf("iterator error: %v", err)
	}

	fmt.Printf("Total entries checked: %d\n", count)
	if count == 0 {
		fmt.Println("Warning: No entries found in the database.")
	}

	return nil
}

func (kvs *KVServer) checkLogDBConsistency() error {
	logFile, err := os.Open("./raft/RaftState.log")
	if err != nil {
		return fmt.Errorf("failed to open log file: %v", err)
	}
	defer logFile.Close()

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := kvs.persister.GetDb().NewIterator(ro)
	defer iter.Close()

	reader := bufio.NewReader(logFile)
	var currentOffset int64 = 0

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		keyStr := string(key.Data())
		dbOffset := int64(binary.LittleEndian.Uint64(value.Data()))

		// 读取日志文件中的对应条目
		entry, entryOffset, err := ReadEntry(reader, currentOffset)
		if err != nil {
			return fmt.Errorf("error reading log entry: %v", err)
		}

		if keyStr != entry.Key {
			fmt.Printf("Mismatch: DB key=%s, Log key=%s\n", keyStr, entry.Key)
		}
		if dbOffset != entryOffset {
			fmt.Printf("Offset mismatch for key %s: DB offset=%d, Log offset=%d\n", keyStr, dbOffset, entryOffset)
		}

		currentOffset = entryOffset + int64(binary.Size(entry.Index)+binary.Size(entry.CurrentTerm)+
			binary.Size(entry.VotedFor)+8+len(entry.Key)+len(entry.Value))

		key.Free()
		value.Free()
	}

	return nil
}

// func (kvs *KVServer) GarbageCollection() error {
// 	fmt.Println("Starting garbage collection...")
// 	startTime := time.Now()

// 	// 创建新的文件用于接收新的写入
// 	currentLog := "./raft/RaftState_new.log"
// 	// newRaftStateLog, err := os.Create(currentLog)
// 	// if err != nil {
// 	//     return fmt.Errorf("failed to create new RaftState log: %v", err)
// 	// }
// 	// defer newRaftStateLog.Close()

// 	// 创建新的RocksDB实例
// 	persister_new, err := NewPersister() // 创建一个新的用于保存key和index的persister
// 	if err != nil {
// 		return fmt.Errorf("failed to create new persister: %v", err)
// 	}
// 	newPersister, err := persister_new.Init("./kvstore/FlexSync/db_key_index_new", true)
// 	if err != nil {
// 		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
// 	}

// 	// 切换到新的文件和RocksDB
// 	kvs.SwitchToNewFiles(currentLog, newPersister)

// 	err = kvs.checkDatabaseContent()
// 	if err != nil {
// 		fmt.Println("检查数据库内容有错误：", err)
// 	}

// 	// err = kvs.checkLogDBConsistency()
// 	// if err != nil {
// 	// 	fmt.Println("检查数据库内容有错误：", err)
// 	// }

// 	// 开始处理旧文件
// 	sortedEntries, err := kvs.processSortedFile()
// 	if err != nil {
// 		return fmt.Errorf("failed to process old file: %v", err)
// 	}
// 	fmt.Printf("Processed %d entries from old file\n", len(sortedEntries))

// 	// 写入新的排序文件
// 	sortedFilePath := "./raft/RaftState_sorted.log"
// 	err = GC4.WriteEntriesToNewFile(sortedEntries, sortedFilePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to write sorted file: %v", err)
// 	}

// 	fileInfo, err := os.Stat(sortedFilePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to stat new file: %v", err)
// 	}
// 	fmt.Printf("New sorted file size :%d bytes\n", fileInfo.Size())

// 	//  先不删除
// 	// // 删除旧的RaftState.log文件
// 	// err = os.Remove("./raft/RaftState.log")
// 	// if err != nil {
// 	//     return fmt.Errorf("failed to remove old RaftState.log: %v", err)
// 	// }

// 	// // 删除旧的RocksDB数据
// 	// err = os.RemoveAll("./kvstore/FlexSync/db_key_index")
// 	// if err != nil {
// 	//     return fmt.Errorf("failed to remove old RocksDB data: %v", err)
// 	// }

// 	// 更新KVServer的查询方法
// 	kvs.updateQueryMethods(sortedFilePath)

// 	fmt.Printf("Garbage collection completed in %v\n", time.Since(startTime))
// 	return nil
// }

func (kvs *KVServer) SwitchToNewFiles(newLog string, newPersister *raft.Persister) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	// 更新两个路径，使得垃圾回收与客户端请求并行执行
	kvs.currentLog = newLog
	fmt.Println("设置kvs.currentLog为", newLog)
	kvs.raft.SetCurrentLog(kvs.currentLog)
	// kvs.raft.currentLog = newLog		// 存储value的磁盘文件由raft操作，raft接触到的只有存储value的log文件
	kvs.persister = newPersister // 存储key和偏移量的rocksdb文件由kvs操作
	// 可能还需要更新其他相关的状态
}

func VerifySortedFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open sorted file: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var prevKey string
	var entryCount int

	for {
		entry, _, err := ReadEntry(reader, 0) // 使用之前定义的 readEntryHelper 函数
		if err != nil {
			if err == io.EOF {
				break // 文件读取结束
			}
			return fmt.Errorf("error reading entry: %v", err)
		}

		if prevKey != "" && entry.Key <= prevKey {
			return fmt.Errorf("file is not sorted: key %s comes after %s", entry.Key, prevKey)
		}
		// fmt.Printf("当前读出的key为: %s\n", entry.Key)

		prevKey = entry.Key
		entryCount++
	}

	fmt.Printf("Verification complete. File is correctly sorted. Total entries: %d\n", entryCount)
	return nil
}

func CheckLogFileStart(filename string, bytesToRead int) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	data := make([]byte, bytesToRead)
	n, err := file.Read(data)
	if err != nil && err != io.EOF {
		return fmt.Errorf("failed to read file: %v", err)
	}

	fmt.Printf("First %d bytes of %s:\n", n, filename)
	// fmt.Printf("As hex: %x\n", data[:n])
	// fmt.Printf("As string: %s\n", string(data[:n]))

	return nil
}

// 使用示例
func CompareLeaderAndFollowerLogs() error {
	leaderLogFile := "/home/DYC/Gitee/FlexSync/raft/RaftState_sorted.log"
	// followerLogFile := "./follower/raft/RaftState.log"

	fmt.Println("Checking Leader log:")
	if err := CheckLogFileStart(leaderLogFile, 1000); err != nil {
		return err
	}

	// fmt.Println("\nChecking Follower log:")
	// if err := CheckLogFileStart(followerLogFile, 1000); err != nil {
	//     return err
	// }

	return nil
}
