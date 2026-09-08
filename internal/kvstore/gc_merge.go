package kvstore

import (
	"fmt"
	"path/filepath"
	// "strings"

	"io"
	"os"

	// "sort"
	"time"

	// lru "github.com/hashicorp/golang-lru"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"github.com/linxGnu/grocksdb"
	"sync/atomic"
)

//	type keyOffset struct{
//		key string
//		offset int64
//	}
//
// var anotherSortedFilePath = "/home/DYC/Gitee/FlexSync/raft/valuelog/RaftState_anotherSorted.log"

// 修改这些全局变量的路径，需要在运行时根据用户指定的data目录动态设置
var (
	anotherNewRaftStateLogPath string
	anotherNewPersisterPath    string
)

// 在main函数中或者适当的地方初始化这些路径（添加到之前的InitGCPaths函数中）
func InitAnotherGCPaths(dataDir string) {
	anotherNewRaftStateLogPath = filepath.Join(dataDir, "data", "valuelog", "newRaftState_1")
	anotherNewPersisterPath = filepath.Join(dataDir, "data", "dbfile", "newKeyIndex_1")
}

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

func (kvs *KVServer) AnotherSwitchToNewFiles(newLog string, newPersister *raft.Persister, newDBPath string) {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	kvs.anotherStartGC = true
	kvs.numGC++
	kvs.oldDBPath = kvs.currentDBPath
	kvs.currentDBPath = newDBPath

	// 赋值旧文件变量
	kvs.oldPersister = kvs.persister // 给old 数据库文件赋初始值
	kvs.oldLog = kvs.currentLog      // 给old log文件赋值

	// 更新两个路径，使得垃圾回收与客户端请求并行执行
	kvs.currentLog = newLog
	fmt.Println("设置kvs.currentLog为", newLog)
	// 带上版本：偏移与"它属于哪个文件"必须在同一把 logMu 下一起确定，
	// 否则切换窗口内写入的那几条会记成旧版本、偏移却是新文件的。
	kvs.raft.SetCurrentLogVersioned(kvs.currentLog, int32(kvs.numGC))
	// kvs.raft.currentLog = newLog		// 存储value的磁盘文件由raft操作，raft接触到的只有存储value的log文件

	kvs.persister = newPersister // 存储key和偏移量的rocksdb文件由kvs操作
	kvs.raft.SetCurrentPersister(kvs.persister)

	kvs.gcInProgress = true // see SwitchToNewFiles
	kvs.saveKVState()
}

func (kvs *KVServer) MergedGarbageCollection() error {
	fmt.Printf("Starting garbage collection... -- another %v\n", kvs.numGC+1)
	startTime := time.Now()

	// 切换只做一次。上一轮搬运若失败，切换的副作用（numGC 已推进、新库已挂上）
	// 仍然留着；此时重做切换会按已增过的序号再建一次库，撞上上次那个还开着的实例。
	if kvs.anotherStartGC && !kvs.anotherEndGC && kvs.switchedPersister != nil {
		fmt.Println("检测到上一轮切换已完成但搬运未结束，跳过切换直接重做搬运")
		return kvs.mergeIntoSortedFile(startTime)
	}

	// 创建新的RocksDB实例===========
	persister_new, err := kvs.NewPersister() // 创建一个新的用于保存key和index的persister
	if err != nil {
		return fmt.Errorf("failed to create new persister: %v", err)
	}
	anotherNewPersisterPath = fmt.Sprintf("%s_%d", anotherNewPersisterPath, kvs.numGC+1)
	newPersister, err := persister_new.Init(anotherNewPersisterPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件=============
	anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", anotherNewRaftStateLogPath, kvs.numGC+1)
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
	kvs.AnotherSwitchToNewFiles(anotherNewRaftStateLogPath, newPersister, anotherNewPersisterPath)
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1)) // see the function's comment in GC.go
	kvs.switchedPersister = newPersister

	return kvs.mergeIntoSortedFile(startTime)
}

// mergeIntoSortedFile 把旧库里的记录合并进新的排序文件。
// 从 MergedGarbageCollection 拆出来，好让搬运失败后的重试能直接重入这一步，
// 而不必再走一遍已经生效的切换。
func (kvs *KVServer) mergeIntoSortedFile(startTime time.Time) error {
	// 归并产物同样是一组分区，基名由上一组的基名派生  1
	mergedSortedFilePath := fmt.Sprintf("%s_merged_%d", kvs.lastPartitions.Base(), kvs.numGC)
	kvs.anotherSortedFilePath = mergedSortedFilePath
	if _, err := os.Stat(partitionPath(mergedSortedFilePath, 0)); err == nil {
		fmt.Println("Sorted file already exists. Skipping garbage collection.")
		return nil
	}

	// 按 key 序读出上一组分区的全部 entry  2
	// 各分区内部有序、区间又互不重叠，顺次拼接即得一条全局有序流，归并逻辑因此不必关心
	// 上一轮的产物被切成了几个文件。
	existingReader, closeExisting, err := kvs.lastPartitions.openStream()
	if err != nil {
		return fmt.Errorf("failed to open existing partitions: %v", err)
	}
	defer closeExisting()

	// Open the original RaftState.log file  3
	oldFile, err := os.Open(kvs.oldLog)
	if err != nil {
		return fmt.Errorf("failed to open original RaftState.log: %v", err)
	}
	defer oldFile.Close()

	// ============= 优化开始：边写边构建索引 =============

	// 归并输出按 key 升序产出，正是 partitionWriter 要求的调用顺序   2 + 3 -> 1   =============
	pw := kvs.newPartitionWriter(mergedSortedFilePath)

	// Create a channel for entries from the old database
	oldEntryChan := make(chan *raft.Entry, 1000)
	existingEntryChan := make(chan *raft.Entry, 1000)

	// readErr 收集两个读取 goroutine 的失败。
	//
	// 这两个 goroutine 原先把错误吞掉——一个 continue 跳过读不出来的记录，另一个
	// 遇到非 EOF 就 break 提前收尾。主流程因此拿不到任何信号，整轮 GC 照常报成功，
	// 随后 os.Remove(kvs.oldLog) 把源文件删掉：那些没搬过去的数据就此永久丢失，
	// 而存储引擎里的偏移还指着一个已经不存在的文件。
	//
	// GC 是数据搬运，搬不动就必须让整轮失败、保住源文件等下一轮重试。
	// 少搬一条也不能当作成功。
	var readErr atomic.Value

	// Start goroutine to read from old database
	go func() {
		defer close(oldEntryChan)
		it := kvs.oldPersister.GetDb().NewIterator(grocksdb.NewDefaultReadOptions())
		defer it.Close()

		for it.SeekToFirst(); it.Valid(); it.Next() {
			key := it.Key()
			value := it.Value()
			defer key.Free()
			defer value.Free()
			if raft.IsMetaKey(key.Data()) {
				continue // recovery metadata; do not migrate it
			}

			entry, err := kvs.entryFromRecord(string(key.Data()), value.Data(), oldFile)
			if err != nil {
				readErr.Store(fmt.Errorf("读取旧库记录失败（key=%q）: %v", key.Data(), err))
				return
			}
			oldEntryChan <- entry
		}
	}()

	// Start goroutine to read from the existing partitions (a single ordered stream)
	go func() {
		defer close(existingEntryChan)
		for {
			entry, _, err := ReadEntry(existingReader, 0)
			if err != nil {
				if err == io.EOF {
					break // 正常读完
				}
				readErr.Store(fmt.Errorf("读取已排序文件失败: %v", err))
				return
			}
			existingEntryChan <- entry
		}
	}()

	// ============= 优化的合并写入逻辑：边写边建索引 =============

	// Merge entries and write to new file while building index
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
			// 稀疏索引、内联缓存预热、分区滚动都在 Add 里
			if err := pw.Add(entryToWrite); err != nil {
				pw.Abort()
				return fmt.Errorf("failed to write merged entry: %v", err)
			}

			writeCount++
			if writeCount%100000 == 0 {
				fmt.Printf("Merged %d entries\n", writeCount)
			}
		}
	}

	// 读取端出过错就不能往下走：合并产物此刻是不完整的，而调用方在本函数返回 nil
	// 之后会删掉源文件。宁可整轮失败、留着源文件等下一轮重试，也不能拿一份缺数据的
	// 排序文件顶替它。
	if e := readErr.Load(); e != nil {
		pw.Abort()
		return fmt.Errorf("合并中止，源文件保持不动: %v", e.(error))
	}

	// 每个分区在封口时 Flush + fsync：调用方在本函数返回 nil 之后会删掉源文件，产物必须先
	// 真正落盘（与第一轮同理，见 gc_first.go）。
	//
	// Each step is timed and reported: a GC round that stalls the node long enough for
	// followers to call an election has to be pinned to one step, and the merge progress
	// lines alone cannot separate "still merging" from "blocked in fsync".
	tMerge := time.Since(startTime)
	tStep := time.Now()
	parts, err := pw.Finish()
	if err != nil {
		pw.Abort()
		return err
	}
	tSeal := time.Since(tStep)
	tStep = time.Now()

	// ============= 直接构建分区清单，避免事后重扫 =============

	// 使用加锁保护索引更新
	kvs.mu.Lock()
	kvs.anotherSortedFilePath = mergedSortedFilePath
	kvs.anotherPartitions = parts
	kvs.mu.Unlock()

	// 预热缓存
	// kvs.warmupCache(mergedSortedFilePath)

	fmt.Printf("建立了索引，得到了针对 %d 个分区的完整索引\n", parts.Len())
	fmt.Printf("[GC-PHASE] round=%d merge=%v flush=%v fsync=%v seal=%v index=%v partitions=%d bytes=%d\n",
		kvs.numGC, tMerge.Round(time.Millisecond), pw.flushTime.Round(time.Millisecond),
		pw.syncTime.Round(time.Millisecond), tSeal.Round(time.Millisecond),
		time.Since(tStep).Round(time.Millisecond), parts.Len(), pw.total)

	kvs.anotherEndGC = true
	kvs.switchedPersister = nil // 本轮已完整结束，重入标记随之作废

	fmt.Printf("Merged garbage collection completed in %v - round %v, processed %d entries\n",
		time.Since(startTime), kvs.numGC, writeCount)
	return nil
}

// 更新 GC 后合并的过程，之前的合并方式有问题，问题如下：
// oldEntryChan是从无序的文件中读取的一个个entry，
// 而exixstingEntryChan是从一个有序的文件中读取一个个entry，
// 当有序的文件合并完时，无序的文件还有数据，则现有的代码会直接简单将剩余的无序读出的一个个entry写入，
// 但是这是不对的，可能无序后面多余的entry写入会存在小于前者已经写入的entry的数据。

// 上述也是没问题的，因为是从数据库文件遍历key对应的offset，再去找对应的entry，所以自然就带有去重的功能。
