package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	// "io"
	"os"
	"sort"
	"time"

	"gitee.com/dong-shuishui/FlexSync/raft"
	lru "github.com/hashicorp/golang-lru"
	"github.com/linxGnu/grocksdb"
)

// 修改这三个全局变量的路径，需要在运行时根据用户指定的data目录动态设置
var (
	firstSortedFilePath      string
	firstNewRaftStateLogPath string
	firstNewPersisterPath    string
)

// 在main函数中或者适当的地方初始化这些路径
func InitGCPaths(dataDir string) {
	firstSortedFilePath = filepath.Join(dataDir, "data", "valuelog", "RaftState_sorted_1")
	firstNewRaftStateLogPath = filepath.Join(dataDir, "data", "valuelog", "newRaftState_1")
	firstNewPersisterPath = filepath.Join(dataDir, "data", "dbfile", "newKeyIndex_1")
}

// entryFromRecord 把存储引擎里的一条记录还原成待搬运的 entry。
//
// GC 的职责是回收 valuelog，可 AVP 内联的小值压根不在 valuelog 里——它的 value
// 就躺在这条记录内。此前 GC 一律拿记录去解偏移，遇到内联记录就报 ErrInlineValue，
// 于是整轮 GC 失败：实测 -inlinePlacement 下 200 个 GET 全部返回 NOKEY。
//
// 内联记录直接用手上的 key/value 构造 entry，与从 valuelog 读出来的走同一条
// 写入 sortedFile 的路径，读路径因此完全不用改；且下游那段"小值预热进
// inlineCache"照常生效，内联的快路径在 GC 之后依然成立。
func (kvs *KVServer) entryFromRecord(paddedKey string, raw []byte, logFile *os.File) (*raft.Entry, error) {
	if len(raw) > 0 && raw[0] == raft.TagInline {
		return &raft.Entry{Key: paddedKey, Value: string(raw[1:])}, nil
	}
	index, err := raft.DecodeOffsetRecord(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to decode offset record: %v", err)
	}
	entry, _, err := kvs.ReadEntryAtIndex(logFile, index)
	if err != nil {
		return nil, fmt.Errorf("failed to read entry at index %d: %v", index, err)
	}
	return entry, nil
}

func (kvs *KVServer) FirstGarbageCollection() error {
	fmt.Println("Starting garbage collection...")
	startTime := time.Now()

	// Create a new file for sorted entries
	lastUnderscoreIndex := strings.LastIndex(firstSortedFilePath, "_")
	if lastUnderscoreIndex == -1 {
		// 如果没有下划线，直接追加 kvs.numGC
		firstSortedFilePath = fmt.Sprintf("%s_%d", firstSortedFilePath, kvs.numGC+1)
	} else {
		// 提取下划线之前的部分，并追加新的 kvs.numGC
		firstSortedFilePath = fmt.Sprintf("%s_%d", firstSortedFilePath[:lastUnderscoreIndex], kvs.numGC+1)
	}
	if _, err := os.Stat(firstSortedFilePath); err == nil {
		fmt.Println("Sorted file already exists. Skipping garbage collection.")
		return nil
	}
	sortedFile, err := os.Create(firstSortedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create sorted file: %v", err)
	}
	defer sortedFile.Close()

	// 赋值旧文件变量
	kvs.oldPersister = kvs.persister     // 给old 数据库文件赋初始值
	kvs.oldLog = kvs.InitialRaftStateLog // 给old log文件赋值

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
	newPersister, err := persister_new.Init(firstNewPersisterPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件
	lastUnderscoreIndex = strings.LastIndex(firstNewRaftStateLogPath, "_")
	if lastUnderscoreIndex == -1 {
		// 如果没有下划线，直接追加 kvs.numGC
		firstNewRaftStateLogPath = fmt.Sprintf("%s_%d", firstNewRaftStateLogPath, kvs.numGC+1)
	} else {
		// 提取下划线之前的部分，并追加新的 kvs.numGC
		firstNewRaftStateLogPath = fmt.Sprintf("%s_%d", firstNewRaftStateLogPath[:lastUnderscoreIndex], kvs.numGC+1)
	}
	if _, err := os.Stat(firstNewRaftStateLogPath); err == nil {
		fmt.Println("New RaftState log file already exists. Skipping creation.")
	} else if os.IsNotExist(err) {
		newRaftStateLog, err := os.Create(firstNewRaftStateLogPath)
		if err != nil {
			return fmt.Errorf("failed to create new RaftState log: %v", err)
		}
		defer newRaftStateLog.Close()
	} else {
		return fmt.Errorf("error checking new RaftState log file: %v", err)
	}

	// 切换到新的文件和RocksDB
	kvs.SwitchToNewFiles(firstNewRaftStateLogPath, newPersister)
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1))

	// ============= 优化开始：边写边构建索引 =============

	// 初始化索引数据结构
	sparse := NewSparseIndexBuilder(kvs.indexBlockBytes)
	inlineCache := NewInlineCache(kvs.inlineCacheBytes)
	var currentOffset int64 = 0

	// 创建bufio.Writer来写入排序文件（末尾显式 Flush 并检查错误，不靠 defer）
	writer := bufio.NewWriter(sortedFile)

	// Read entries from RocksDB and write them in sorted order to the new file
	it := kvs.oldPersister.GetDb().NewIterator(grocksdb.NewDefaultReadOptions())
	defer it.Close()

	writeNum := 0
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()

		// 记录可能是 [TagOffset, offset8]（去 valuelog 取）也可能是
		// [TagInline, value]（value 就在这条记录里）——由 entryFromRecord 分流。
		entry, err := kvs.entryFromRecord(string(key.Data()), value.Data(), oldFile)
		if err != nil {
			return err
		}

		// 记录写入前的偏移量
		beforeWriteOffset := currentOffset

		// Write the entry to the sorted file for durability (always, regardless of inline decision)
		err = kvs.WriteEntryToSortedFile(writer, entry)
		if err != nil {
			return fmt.Errorf("failed to write entry to sorted file: %v", err)
		}

		// 计算entry的大小（与WriteEntryToSortedFile的格式一致）
		keySize := uint32(len(entry.Key))
		valueSize := uint32(len(entry.Value))
		entrySize := int64(20 + keySize + valueSize) // 20字节头部 + key + value

		unpadKey := kvs.persister.UnpadKey(entry.Key)

		// 每约 indexBlockBytes 记录一个块起点。索引项用文件里的 padded key，
		// 与查找时的比较保持一致。
		sparse.Observe(entry.Key, beforeWriteOffset, entrySize)

		// AVP: 小值在预算内预热进内联缓存，读命中即可免去文件 seek
		if len(entry.Value) < kvs.inlineThreshold {
			inlineCache.Add(unpadKey, entry.Value)
		}

		// 更新当前偏移量
		currentOffset += entrySize

		writeNum++
		if writeNum%200000 == 0 {
			fmt.Printf("成功写入 %d个entry \n", writeNum)
		}
	}

	// Flush 只是把缓冲交给内核。本函数返回 nil 后调用方会删掉源文件，
	// 所以排序文件必须先真正落盘：否则掉电时"源已删、目标还在页缓存里"，
	// 这一轮 GC 搬过来的数据就全没了。写路径每条都 fsync，GC 没理由例外。
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("failed to flush sorted file: %v", err)
	}
	if err := sortedFile.Sync(); err != nil {
		return fmt.Errorf("failed to fsync sorted file: %v", err)
	}

	// ============= 直接构建SortedFileIndex对象 =============

	// 使用加锁保护索引更新
	kvs.mu.Lock()
	kvs.firstSortedFilePath = firstSortedFilePath
	kvs.firstSortedFileIndex = &SortedFileIndex{
		Sparse:       sparse.Build(),
		FileSize:     currentOffset,
		InlineValues: inlineCache,
		FilePath:     firstSortedFilePath,
	}
	kvs.mu.Unlock()

	// 初始化LRU缓存，设置合适的缓存大小
	// err = kvs.initSortedFileCache(sortedFileCacheNums)
	// if err != nil {
	// 	fmt.Printf("Failed to initialize LRU cache: %v\n", err)
	// 	return err
	// }

	// 预热缓存
	// kvs.warmupCache(firstSortedFilePath)

	fmt.Println("建立了索引，得到了针对已排序文件的完整索引")
	kvs.filePool, err = NewFileDescriptorPool(firstSortedFilePath, 50)
	if err != nil {
		fmt.Printf("Failed to create file descriptor pool: %v\n", err)
		panic("创建文件描述符池失败")
	}
	fmt.Println("创建文件描述符池成功")

	fmt.Printf("First garbage collection completed in %v, processed %d entries\n", time.Since(startTime), writeNum)
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

	// 偏移在 leader 与 follower 上是同一套语义，不需要按角色区分。
	//
	// 此前这里对 follower 额外加了 64000 字节，注释说"follower 的偏移量统一比
	// leader 小一个 vsize"。真正的原因是日志文件当时用 O_APPEND 打开：POSIX 规定
	// 该模式下每次写入前偏移强制设为文件末尾，Seek 对写入位置完全无效，而 follower
	// 处理冲突日志走的正是"Seek 回退再覆盖"这条路——覆盖于是静默变成追加，文件
	// 末尾多出一条记录，后续偏移全部错位。O_APPEND 已改掉（O_CREATE|O_WRONLY，
	// 写入位置由 logOffset 自行维护），这个补偿也就没有意义了。
	//
	// 补偿本身还是个 bug：64000 是当年那次实验的 value 大小，写死在这里。两节点
	// 实测 value=1024 时，follower 的 GC 报 "failed to read entry at index
	// 10985842: EOF"，正是真实偏移加上 64000 越过了文件末尾；同一时刻 leader 无错。
	return ReadEntry(reader, index)
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

func (kvs *KVServer) CreateIndex(firstSortedFilePath string) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.firstSortedFilePath = firstSortedFilePath

	// 创建索引，假设每1个条目记录一次索引，稀疏索引，间隔一部分创建一个索引，找到第一个合适的，再进行线性查询
	index, err := kvs.CreateSortedFileIndex(firstSortedFilePath)
	if err != nil {
		// 处理错误
		return err
	}

	// index:=&SortedFileIndex{Entries: nil, FilePath: firstSortedFilePath}		//	测试

	kvs.firstSortedFileIndex = index

	// 初始化LRU缓存，设置合适的缓存大小
	// 这里假设缓存40000个key-value对
	// err = kvs.initSortedFileCache(sortedFileCacheNums)						    // 测试，err 不要 :=
	// if err != nil {
	// 	fmt.Printf("Failed to initialize LRU cache: %v\n", err)
	// 	return err
	// }

	// // 预热缓存
	// kvs.warmupCache(firstSortedFilePath)

	fmt.Println("建立了索引，得到了针对已排序文件的稀疏索引")
	kvs.filePool, err = NewFileDescriptorPool(firstSortedFilePath, 50)
	if err != nil {
		fmt.Printf("Failed to create file descriptor pool: %v\n", err)
		panic("创建文件描述符池失败")
	}
	fmt.Println("创建文件描述符池成功")
	// defer kvs.filePool.Close() // 程序退出时关闭池中的所有文件描述符

	return nil

	// kvs.getFromFile = kvs.getFromSortedOrNew
	// kvs.scanFromFile = kvs.scanFromSortedOrNew
}

// 预热缓存：读取最近的一些数据到缓存中
func (kvs *KVServer) warmupCache(filePath string) {
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Failed to open file for cache warmup: %v\n", err)
		return
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	count := 0
	maxWarmupEntries := sortedFileCacheNums / 2 // 预热的条目数量

	for count < maxWarmupEntries {
		entry, _, err := ReadEntry(reader, 0)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			fmt.Printf("Error reading entry during cache warmup: %v\n", err)
			return
		}

		// 添加到缓存
		kvs.sortedFileCache.Add(kvs.persister.UnpadKey(entry.Key), entry.Value)
		count++
	}

	fmt.Printf("Cache warmup completed with %d entries\n", count)
}

// CreateSortedFileIndex 扫描整个 sortedFile 重建索引（重启或 GC 合并后使用）。
//
// 原实现为每个 key 建一条 key→offset 的内存记录，索引内存随 key 数线性增长。
// 现在改建稀疏块索引，内存降为 O(块数)。注意此路径不重建内联缓存——那是纯加速层，
// 冷启动为空即可，读路径会在未命中时自然回填。
func (kvs *KVServer) CreateSortedFileIndex(filePath string) (*SortedFileIndex, error) {
	sparse, fileSize, err := kvs.BuildSparseIndex(filePath, kvs.indexBlockBytes)
	if err != nil {
		return nil, err
	}
	return &SortedFileIndex{
		Sparse:       sparse,
		FileSize:     fileSize,
		InlineValues: NewInlineCache(kvs.inlineCacheBytes),
		FilePath:     filePath,
	}, nil
}

func (kvs *KVServer) processSortedFile() ([]*raft.Entry, error) {
	// 创建LRU缓存
	// 假设我们允许缓存占用 100MB 内存，每个条目占 20B
	// 100MB / 20B = 5,000,000 个条目
	cache, _ := lru.New(5000000)

	// 打开原始文件
	file, err := os.Open("/home/DYC/Gitee/FlexSync/raft/valuelog/RaftState.log")
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

	ro := grocksdb.NewDefaultReadOptions()
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
		if count >= 10 {
			fmt.Printf("Stopping after %v entries...\n", count)
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

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := kvs.persister.GetDb().NewIterator(ro)
	defer iter.Close()

	reader := bufio.NewReader(logFile)
	var currentOffset int64 = 0

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		keyStr := string(key.Data())
		dbOffset, err := raft.DecodeOffsetRecord(value.Data())
		if err != nil {
			return fmt.Errorf("failed to decode offset record for key %s: %v", keyStr, err)
		}

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
// 	firstSortedFilePath := "./raft/RaftState_sorted.log"
// 	err = GC4.WriteEntriesToNewFile(sortedEntries, firstSortedFilePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to write sorted file: %v", err)
// 	}

// 	fileInfo, err := os.Stat(firstSortedFilePath)
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
// 	kvs.updateQueryMethods(firstSortedFilePath)

// 	fmt.Printf("Garbage collection completed in %v\n", time.Since(startTime))
// 	return nil
// }

// waitOldVersionApplied 等到所有落在旧日志文件（版本 oldVersion）里的条目都 apply 完毕。
//
// 切换文件之后、对旧库建迭代器之前必须等这一步。RocksDB 迭代器是创建时刻的快照：
// 一条已提交但还没 apply 的旧版本条目，会在快照之后才被写进旧库，GC 看不见它，
// 随后旧文件被删、旧库被弃，这个 key 就没了。apply 是事件驱动的，正常只落后几微秒，
// 所以此前从未被测出来；但 follower 落后或高并发下的 apply 积压都能把窗口撑开。
func (kvs *KVServer) waitOldVersionApplied(oldVersion int32) {
	start := time.Now()
	for {
		v, pending := kvs.raft.OldestPendingVersion()
		if !pending || v > oldVersion {
			if waited := time.Since(start); waited > 50*time.Millisecond {
				fmt.Printf("GC 等待旧版本条目 apply 完毕，用时 %v\n", waited)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
}

func (kvs *KVServer) SwitchToNewFiles(newLog string, newPersister *raft.Persister) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.startGC = true
	kvs.numGC++

	// 更新两个路径，使得垃圾回收与客户端请求并行执行
	kvs.currentLog = newLog
	fmt.Println("设置kvs.currentLog为", newLog)
	kvs.raft.SetCurrentLogVersioned(kvs.currentLog, int32(kvs.numGC))
	// kvs.raft.currentLog = newLog		// 存储value的磁盘文件由raft操作，raft接触到的只有存储value的log文件

	kvs.persister = newPersister // 存储key和偏移量的rocksdb文件由kvs操作
	kvs.raft.SetCurrentPersister(kvs.persister)
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
	leaderLogFile := "/home/DYC/Gitee/FlexSync/raft/valuelog/RaftState_sorted.log"
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
