package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"strings"

	"io"
	"os"

	// "sort"
	"time"

	// lru "github.com/hashicorp/golang-lru"
	"gitee.com/dong-shuishui/FlexSync/raft"
	"github.com/tecbot/gorocksdb"
)

//	type keyOffset struct{
//		key string
//		offset int64
//	}
//
// var anotherSortedFilePath = "/home/DYC/Gitee/FlexSync/raft/valuelog/RaftState_anotherSorted.log"
var logPathToCheck = "/home/DYC/Gitee/FlexSync/raft/valuelog"
var dbPathToCheck = "/home/DYC/Gitee/FlexSync/kvstore/FlexSync/dbfile"
var anotherNewRaftStateLogPath = "/home/DYC/Gitee/FlexSync/raft/valuelog/newRaftState_1"
var anotherNewPersisterPath = "/home/DYC/Gitee/FlexSync/kvstore/FlexSync/dbfile/newKeyIndex_1"

const sortedFileCacheNums = 4000

// ensurePathExists 检查路径是否存在，如果不存在则创建它
func ensurePathExists(path string) error {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// 如果路径不存在，创建该路径
		err := os.MkdirAll(path, 0755) // 0755 是目录权限
		if err != nil {
			return fmt.Errorf("failed to create directory %s: %v", path, err)
		}
		// fmt.Printf("Directory created: %s\n", path)
	} else if err != nil {
		// 如果其他错误发生
		return fmt.Errorf("error checking directory %s: %v", path, err)
	} else {
		// 如果路径存在
		// fmt.Printf("Directory already exists: %s\n", path)
	}
	return nil
}

func (kvs *KVServer) AnotherGarbageCollection() error {
	err := kvs.MergedGarbageCollection()
	return err
}

func (kvs *KVServer) AnotherSwitchToNewFiles(newLog string, newPersister *raft.Persister) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.numGC++
	kvs.raft.SetNumGC(kvs.numGC)

	// 赋值旧文件变量
	kvs.oldPersister = kvs.persister // 给old 数据库文件赋初始值
	kvs.oldLog = kvs.currentLog      // 给old log文件赋值

	// 更新两个路径，使得垃圾回收与客户端请求并行执行
	kvs.currentLog = newLog
	fmt.Println("设置kvs.currentLog为", newLog)
	kvs.raft.SetCurrentLog(kvs.currentLog)
	// kvs.raft.currentLog = newLog		// 存储value的磁盘文件由raft操作，raft接触到的只有存储value的log文件

	kvs.persister = newPersister // 存储key和偏移量的rocksdb文件由kvs操作
	kvs.raft.SetCurrentPersister(kvs.persister)
	// 可能还需要更新其他相关的状态
}

func (kvs *KVServer) MergedGarbageCollection() error {
	fmt.Printf("Starting garbage collection... -- another %v\n", kvs.numGC)
	startTime := time.Now()

	// 创建新的RocksDB实例===========
	persister_new, err := kvs.NewPersister() // 创建一个新的用于保存key和index的persister
	if err != nil {
		return fmt.Errorf("failed to create new persister: %v", err)
	}
	lastUnderscoreIndex := strings.LastIndex(anotherNewPersisterPath, "_")
	if lastUnderscoreIndex == -1 {
		// 如果没有下划线，直接追加 kvs.numGC
		anotherNewPersisterPath = fmt.Sprintf("%s_%d", anotherNewPersisterPath, kvs.numGC+1)
	} else {
		// 提取下划线之前的部分，并追加新的 kvs.numGC
		anotherNewPersisterPath = fmt.Sprintf("%s_%d", anotherNewPersisterPath[:lastUnderscoreIndex], kvs.numGC+1)
	}
	newPersister, err := persister_new.Init(anotherNewPersisterPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件=============
	lastUnderscoreIndex = strings.LastIndex(anotherNewRaftStateLogPath, "_")
	if lastUnderscoreIndex == -1 {
		// 如果没有下划线，直接追加 kvs.numGC
		anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath, kvs.numGC+1)
	} else {
		// 提取下划线之前的部分，并追加新的 kvs.numGC
		anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath[:lastUnderscoreIndex], kvs.numGC+1)
	}
	// anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath, kvs.numGC)
	if _, err := os.Stat(anotherNewRaftStateLogPath); err == nil {
		fmt.Println("New RaftState log file already exists. Skipping creation.")
	} else if os.IsNotExist(err) {
		newRaftStateLog, err := os.Create(anotherNewRaftStateLogPath)
		if err != nil {
			return fmt.Errorf("failed to create new RaftState log: %v", err)
		}
		defer newRaftStateLog.Close()
	} else {
		return fmt.Errorf("error checking new RaftState log file: %v", err)
	}

	kvs.anotherStartGC = true

	// 切换到新的文件和RocksDB
	kvs.AnotherSwitchToNewFiles(anotherNewRaftStateLogPath, newPersister)

	// 创建合并文件
	// Create a temporary file for the merged sorted entries  1
	// mergedSortedFilePath := kvs.lastSortedFileIndex.FilePath + "_merged"
	var mergedSortedFilePath string
	lastUnderscoreIndex = strings.LastIndex(kvs.lastSortedFileIndex.FilePath, "_")
	if lastUnderscoreIndex == -1 {
		// 如果没有下划线，直接追加 kvs.numGC
		mergedSortedFilePath = fmt.Sprintf("%s_%d", mergedSortedFilePath, kvs.numGC)
	} else {
		// 提取下划线之前的部分，并追加新的 kvs.numGC
		mergedSortedFilePath = fmt.Sprintf("%s_%d", mergedSortedFilePath[:lastUnderscoreIndex], kvs.numGC)
	}
	// mergedSortedFilePath := fmt.Sprintf("%s_merged_%d", kvs.lastSortedFileIndex.FilePath, kvs.numGC)
	kvs.anotherSortedFilePath = mergedSortedFilePath
	if _, err := os.Stat(mergedSortedFilePath); err == nil {
		fmt.Println("Sorted file already exists. Skipping garbage collection.")
		return nil
	}
	mergedFile, err := os.Create(mergedSortedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %v", err)
	}
	defer mergedFile.Close()

	// Open the existing sorted file  2
	existingSortedFile, err := os.Open(kvs.lastSortedFileIndex.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open existing sorted file: %v", err)
	}
	defer existingSortedFile.Close()

	// Open the original RaftState.log file  3
	oldFile, err := os.Open(kvs.oldLog)
	if err != nil {
		return fmt.Errorf("failed to open original RaftState.log: %v", err)
	}
	defer oldFile.Close()

	// Create buffered writer for the merged file   2 + 3 -> 1   =============
	writer := bufio.NewWriter(mergedFile)

	// 在合并之前验证两个输入源的有序性
	// if err := kvs.verifyOldDatabaseOrder(oldFile); err != nil {
	// 	return err
	// }

	// if err := kvs.verifySortedFileOrder(); err != nil {
	// 	return err
	// }

	// Create a channel for entries from the old database
	oldEntryChan := make(chan *raft.Entry, 1000)
	existingEntryChan := make(chan *raft.Entry, 1000)

	// Start goroutine to read from old database
	go func() {
		defer close(oldEntryChan)
		it := kvs.oldPersister.GetDb().NewIterator(gorocksdb.NewDefaultReadOptions())
		defer it.Close()

		for it.SeekToFirst(); it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()
			defer key.Free()
			defer value.Free()

			index := binary.LittleEndian.Uint64(value.Data())
			entry, _, err := kvs.ReadEntryAtIndex(oldFile, int64(index))
			if err != nil {
				fmt.Printf("Error reading entry at index %d: %v\n", index, err)
				continue
			}
			oldEntryChan <- entry
		}
	}()

	// Start goroutine to read from existing sorted file
	go func() {
		defer close(existingEntryChan)
		reader := bufio.NewReader(existingSortedFile)
		for {
			entry, _, err := ReadEntry(reader, 0)
			if err != nil {
				if err == io.EOF {
					break
				}
				fmt.Printf("Error reading sorted file: %v\n", err)
				break
			}
			existingEntryChan <- entry
		}
	}()

	// Merge entries and write to new file
	var oldEntry, existingEntry *raft.Entry
	var oldOk, existingOk bool

	oldEntry, oldOk = <-oldEntryChan
	existingEntry, existingOk = <-existingEntryChan

	writeCount := 0
	for oldOk || existingOk {
		var entryToWrite *raft.Entry

		switch {
		case !existingOk: // Only old entries left
			entryToWrite = oldEntry
			oldEntry, oldOk = <-oldEntryChan
		case !oldOk: // Only existing entries left
			entryToWrite = existingEntry
			existingEntry, existingOk = <-existingEntryChan
		default: // Both channels have entries
			if oldEntry.Key < existingEntry.Key {
				entryToWrite = oldEntry
				oldEntry, oldOk = <-oldEntryChan
			} else if oldEntry.Key > existingEntry.Key {
				entryToWrite = existingEntry
				existingEntry, existingOk = <-existingEntryChan
			} else { // Same key, take the newer one（that is the entry from old database, instead of the entry from the existing sorted file） from old database
				entryToWrite = oldEntry
				oldEntry, oldOk = <-oldEntryChan
				existingEntry, existingOk = <-existingEntryChan
			}
		}

		if entryToWrite != nil {
			err := kvs.WriteEntryToSortedFile(writer, entryToWrite)
			if err != nil {
				return fmt.Errorf("failed to write merged entry: %v", err)
			}
			writeCount++
			if writeCount%100000 == 0 {
				fmt.Printf("Merged %d entries\n", writeCount)
			}
		}
	}

	// Flush the writer
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush writer: %v", err)
	}

	// Verify the merged file
	// if err := VerifySortedFile(mergedSortedFilePath); err != nil {
	// 	return fmt.Errorf("verification of merged file failed: %v", err)
	// }

	// Replace the old sorted file with the merged one
	// if err := os.Rename(mergedSortedFilePath, kvs.firstSortedFilePath); err != nil {
	//     return fmt.Errorf("failed to replace old sorted file: %v", err)
	// }

	// Update the index for the merged file
	if err := kvs.AnotherCreateIndex(mergedSortedFilePath); err != nil {
		return fmt.Errorf("failed to create index for merged file: %v", err)
	}

	kvs.anotherEndGC = true

	fmt.Printf("Merged garbage collection completed in %v\n - round %v\n", time.Since(startTime), kvs.numGC)
	return nil
}

// 分别验证两个数据源的有序性
func (kvs *KVServer) verifyOldDatabaseOrder(file *os.File) error {
	fmt.Println("开始验证旧数据库的有序性...")
	var prevKey string
	count := 0

	it := kvs.oldPersister.GetDb().NewIterator(gorocksdb.NewDefaultReadOptions())
	defer it.Close()

	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		defer key.Free()
		defer value.Free()

		index := binary.LittleEndian.Uint64(value.Data())
		entry, _, err := kvs.ReadEntryAtIndex(file, int64(index))
		if err != nil {
			return fmt.Errorf("读取索引 %d 失败: %v", index, err)
		}

		if prevKey != "" && entry.Key <= prevKey {
			return fmt.Errorf("旧数据库无序: 键值 %s 出现在 %s 之后，在第 %d 个条目",
				entry.Key, prevKey, count)
		}
		prevKey = entry.Key
		count++

		if count%100000 == 0 {
			fmt.Printf("已验证 %d 个条目\n", count)
		}
	}

	fmt.Printf("旧数据库验证完成: 共 %d 个条目，确认有序\n", count)
	return nil
}

func (kvs *KVServer) verifySortedFileOrder() error {
	fmt.Println("开始验证已排序文件的有序性...")
	var prevKey string
	count := 0

	file, err := os.Open(kvs.lastSortedFileIndex.FilePath)
	if err != nil {
		return fmt.Errorf("打开排序文件失败: %v", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	for {
		entry, _, err := ReadEntry(reader, 0)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("读取排序文件失败: %v", err)
		}

		if prevKey != "" && entry.Key <= prevKey {
			return fmt.Errorf("排序文件无序: 键值 %s 出现在 %s 之后，在第 %d 个条目",
				entry.Key, prevKey, count)
		}
		prevKey = entry.Key
		count++

		if count%100000 == 0 {
			fmt.Printf("已验证 %d 个条目\n", count)
		}
	}

	fmt.Printf("排序文件验证完成: 共 %d 个条目，确认有序\n", count)
	return nil
}

func (kvs *KVServer) AnotherCreateIndex(SortedFilePath string) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	// 创建索引，假设每1个条目记录一次索引，稀疏索引，间隔一部分创建一个索引，找到第一个合适的，再进行线性查询
	index, err := kvs.CreateSortedFileIndex(SortedFilePath)
	if err != nil {
		// 处理错误
		return err
	}
	kvs.anothersortedFileIndex = index

	// 初始化LRU缓存，设置合适的缓存大小
	err = kvs.initSortedFileCache(sortedFileCacheNums)
	if err != nil {
		fmt.Printf("Failed to initialize LRU cache: %v\n", err)
		return err
	}

	// 预热缓存
	kvs.warmupCache(SortedFilePath)

	fmt.Println("建立了索引，得到了针对已排序文件的稀疏索引")
	kvs.filePool, err = NewFileDescriptorPool(SortedFilePath, 50)
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

// 更新 GC 后合并的过程，之前的合并方式有问题，问题如下：
// oldEntryChan是从无序的文件中读取的一个个entry，
// 而exixstingEntryChan是从一个有序的文件中读取一个个entry，
// 当有序的文件合并完时，无序的文件还有数据，则现有的代码会直接简单将剩余的无序读出的一个个entry写入，
// 但是这是不对的，可能无序后面多余的entry写入会存在小于前者已经写入的entry的数据。

// 上述也是没问题的，因为是从数据库文件遍历key对应的offset，再去找对应的entry，所以自然就带有去重的功能。

// func (kvs *KVServer) MergedGarbageCollection() error {
// 	fmt.Printf("Starting garbage collection... -- another %v\n", kvs.numGC)
// 	startTime := time.Now()

// 	// 创建新的RocksDB实例===========
// 	persister_new, err := kvs.NewPersister() // 创建一个新的用于保存key和index的persister
// 	if err != nil {
// 		return fmt.Errorf("failed to create new persister: %v", err)
// 	}
// 	anotherNewPersisterPath = fmt.Sprintf("%s_%d", anotherNewPersisterPath, kvs.numGC)
// 	newPersister, err := persister_new.Init(anotherNewPersisterPath, true)
// 	if err != nil {
// 		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
// 	}

// 	// 创建新的RaftState日志文件=============
// 	anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath, kvs.numGC)
// 	if _, err := os.Stat(anotherNewRaftStateLogPath); err == nil {
// 		fmt.Println("New RaftState log file already exists. Skipping creation.")
// 	} else if os.IsNotExist(err) {
// 		newRaftStateLog, err := os.Create(anotherNewRaftStateLogPath)
// 		if err != nil {
// 			return fmt.Errorf("failed to create new RaftState log: %v", err)
// 		}
// 		defer newRaftStateLog.Close()
// 	} else {
// 		return fmt.Errorf("error checking new RaftState log file: %v", err)
// 	}

// 	kvs.anotherStartGC = true

// 	// 切换到新的文件和RocksDB
// 	kvs.AnotherSwitchToNewFiles(anotherNewRaftStateLogPath, newPersister)

// 	// Create a temporary file for the merged sorted entries  1
// 	// mergedSortedFilePath := kvs.lastSortedFileIndex.FilePath + "_merged"
// 	mergedSortedFilePath := fmt.Sprintf("%s_merged_%d", kvs.lastSortedFileIndex.FilePath, kvs.numGC)
// 	kvs.anotherSortedFilePath = mergedSortedFilePath
// 	if _, err := os.Stat(mergedSortedFilePath); err == nil {
// 		fmt.Println("Sorted file already exists. Skipping garbage collection.")
// 		return nil
// 	}
// 	mergedFile, err := os.Create(mergedSortedFilePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to create merged file: %v", err)
// 	}
// 	defer mergedFile.Close()

// 	// Open the existing sorted file  2
// 	existingSortedFile, err := os.Open(kvs.lastSortedFileIndex.FilePath)
// 	if err != nil {
// 		return fmt.Errorf("failed to open existing sorted file: %v", err)
// 	}
// 	defer existingSortedFile.Close()

// 	// Open the original RaftState.log file  3
// 	oldFile, err := os.Open(kvs.oldLog)
// 	if err != nil {
// 		return fmt.Errorf("failed to open original RaftState.log: %v", err)
// 	}
// 	defer oldFile.Close()

// 	// Create buffered writer for the merged file   2 + 3 -> 1   =============
// 	writer := bufio.NewWriter(mergedFile)

// 	// 1. 首先从旧的 RocksDB 中读取所有 entries 并排序
//     var oldEntries []*raft.Entry
//     it := kvs.oldPersister.GetDb().NewIterator(gorocksdb.NewDefaultReadOptions())
//     defer it.Close()

//     // 读取所有旧数据
//     for it.SeekToFirst(); it.Valid(); it.Next() {
//         key := it.Key()
//         value := it.Value()
//         defer key.Free()
//         defer value.Free()

//         index := binary.LittleEndian.Uint64(value.Data())
//         entry, _, err := kvs.ReadEntryAtIndex(oldFile, int64(index))
//         if err != nil {
//             fmt.Printf("Error reading entry at index %d: %v\n", index, err)
//             continue
//         }
//         oldEntries = append(oldEntries, entry)
//     }

//     // 对旧数据进行排序
//     sort.Slice(oldEntries, func(i, j int) bool {
//         return oldEntries[i].Key < oldEntries[j].Key
//     })

//     // 2. 从已排序文件中读取 entries
//     var sortedEntries []*raft.Entry
//     reader := bufio.NewReader(existingSortedFile)
//     for {
//         entry, _, err := ReadEntry(reader, 0)
//         if err != nil {
//             if err == io.EOF {
//                 break
//             }
//             return fmt.Errorf("error reading sorted file: %v", err)
//         }
//         sortedEntries = append(sortedEntries, entry)
//     }

//     // 3. 合并两个已排序的切片
//     // writer := bufio.NewWriter(mergedFile)
//     i, j := 0, 0
//     writeCount := 0

//     // 合并写入函数
//     writeEntry := func(entry *raft.Entry) error {
//         err := kvs.WriteEntryToSortedFile(writer, entry)
//         if err != nil {
//             return fmt.Errorf("failed to write merged entry: %v", err)
//         }
//         writeCount++
//         if writeCount%100000 == 0 {
//             fmt.Printf("Merged %d entries\n", writeCount)
//         }
//         return nil
//     }

//     // 合并两个已排序的数组
//     for i < len(oldEntries) && j < len(sortedEntries) {
//         if oldEntries[i].Key < sortedEntries[j].Key {
//             if err := writeEntry(oldEntries[i]); err != nil {
//                 return err
//             }
//             i++
//         } else if oldEntries[i].Key > sortedEntries[j].Key {
//             if err := writeEntry(sortedEntries[j]); err != nil {
//                 return err
//             }
//             j++
//         } else { // 相同key，使用旧数据库中的较新条目
//             if err := writeEntry(oldEntries[i]); err != nil {
//                 return err
//             }
//             i++
//             j++
//         }
//     }

//     // 写入剩余的条目
//     for ; i < len(oldEntries); i++ {
//         if err := writeEntry(oldEntries[i]); err != nil {
//             return err
//         }
//     }

//     for ; j < len(sortedEntries); j++ {
//         if err := writeEntry(sortedEntries[j]); err != nil {
//             return err
//         }
//     }

//     // 刷新写入器
//     if err := writer.Flush(); err != nil {
//         return fmt.Errorf("failed to flush writer: %v", err)
//     }

// 	// Verify the merged file
// 	if err := VerifySortedFile(mergedSortedFilePath); err != nil {
// 		return fmt.Errorf("verification of merged file failed: %v", err)
// 	}

// 	// Replace the old sorted file with the merged one
// 	// if err := os.Rename(mergedSortedFilePath, kvs.firstSortedFilePath); err != nil {
// 	//     return fmt.Errorf("failed to replace old sorted file: %v", err)
// 	// }

// 	// Update the index for the merged file
// 	if err := kvs.AnotherCreateIndex(mergedSortedFilePath); err != nil {
// 		return fmt.Errorf("failed to create index for merged file: %v", err)
// 	}

// 	kvs.anotherEndGC = true

// 	fmt.Printf("Merged garbage collection completed in %v\n - round %v", time.Since(startTime), kvs.numGC)
// 	return nil
// }
