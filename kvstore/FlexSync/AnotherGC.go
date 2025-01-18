package main

import (
	"bufio"
	"encoding/binary"
	"fmt"

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
var anotherSortedFilePath = "/home/DYC/Gitee/FlexSync/raft/RaftState_anotherSorted.log"
var anotherNewRaftStateLogPath = "/home/DYC/Gitee/FlexSync/raft/RaftState_anotherNew.log"
var anotherNewPersisterPath = "/home/DYC/Gitee/FlexSync/kvstore/FlexSync/db_key_index_anotherNew"

func (kvs *KVServer) AnotherGarbageCollection() error {
	fmt.Printf("Starting garbage collection... -- another %v\n", kvs.numGC)
	startTime := time.Now()

	// Create a new file for sorted entries
	if _, err := os.Stat(anotherSortedFilePath); err == nil {
		fmt.Println("Sorted file already exists. Skipping garbage collection.")
		return nil
	}
	sortedFile, err := os.Create(anotherSortedFilePath)
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
	newPersister, err := persister_new.Init(anotherNewPersisterPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件
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
	err = kvs.CreateIndex(anotherSortedFilePath)
	if err != nil {
		fmt.Println("创建索引有问题：", err)
	}

	// 添加验证步骤
	err = VerifySortedFile(anotherSortedFilePath)
	if err != nil {
		return fmt.Errorf("verification of sorted file failed: %v", err)
	}

	kvs.anotherEndGC = true

	fmt.Printf("Garbage collection completed in %v\n -- another %v", time.Since(startTime), kvs.numGC)
	return nil
}

func (kvs *KVServer) AnotherSwitchToNewFiles(newLog string, newPersister *raft.Persister) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

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
	anotherNewPersisterPath =  fmt.Sprintf("%s_%d", anotherNewPersisterPath, kvs.numGC)
	newPersister, err := persister_new.Init(anotherNewPersisterPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件=============
	anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath, kvs.numGC)
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

	// Create a temporary file for the merged sorted entries  1
	// mergedSortedFilePath := kvs.lastSortedFileIndex.FilePath + "_merged"
	mergedSortedFilePath := fmt.Sprintf("%s_merged_%d", kvs.lastSortedFileIndex.FilePath, kvs.numGC)
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
	if err := VerifySortedFile(mergedSortedFilePath); err != nil {
		return fmt.Errorf("verification of merged file failed: %v", err)
	}

	// Replace the old sorted file with the merged one
	// if err := os.Rename(mergedSortedFilePath, kvs.firstSortedFilePath); err != nil {
	//     return fmt.Errorf("failed to replace old sorted file: %v", err)
	// }

	// Update the index for the merged file
	if err := kvs.AnotherCreateIndex(mergedSortedFilePath); err != nil {
		return fmt.Errorf("failed to create index for merged file: %v", err)
	}

	fmt.Printf("Merged garbage collection completed in %v\n - round %v", time.Since(startTime), kvs.numGC)
	return nil
}

func (kvs *KVServer) AnotherCreateIndex(SortedFilePath string) error {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()

	kvs.firstSortedFilePath = firstSortedFilePath

	// 创建索引，假设每1个条目记录一次索引，稀疏索引，间隔一部分创建一个索引，找到第一个合适的，再进行线性查询
	index, err := kvs.CreateSortedFileIndex(SortedFilePath)
	if err != nil {
		// 处理错误
		return err
	}
	kvs.anothersortedFileIndex = index

	// 初始化LRU缓存，设置合适的缓存大小
	// 这里假设缓存40000个key-value对
	err = kvs.initSortedFileCache(4000)
	if err != nil {
		fmt.Printf("Failed to initialize LRU cache: %v\n", err)
		return err
	}

	// 预热缓存
	kvs.warmupCache(firstSortedFilePath)

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