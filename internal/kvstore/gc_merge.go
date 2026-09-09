package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

//	type keyOffset struct{
//		key string
//		offset int64
//	}
//
// var anotherSortedFilePath = "/home/DYC/Gitee/FlexSync/raft/valuelog/RaftState_anotherSorted.log"

// 归并轮产物的**基名**，运行时按 data 目录设置。每轮的实际路径由 mergeRoundPaths 派生，
// 这两个变量本身不再被改写。
//
// 原先是每轮就地累加：anotherNewRaftStateLogPath = fmt.Sprintf("%s_%d", 自己, numGC+1)。
// 第 2 轮得到 newRaftState_1_2，第 3 轮就成了 newRaftState_1_2_3，第 4 轮 _1_2_3_4 ——
// 名字随轮数越滚越长，而恢复逻辑按固定规则去猜路径，对不上就找不到文件。
// 这个坑一直被 `numGC >= 2` 的两轮上限盖着，**放开轮数上限（P2）就会立刻踩到**。
var (
	anotherNewRaftStateLogBase string
	anotherNewPersisterBase    string
)

// 在main函数中或者适当的地方初始化这些路径（添加到之前的InitGCPaths函数中）
func InitAnotherGCPaths(dataDir string) {
	anotherNewRaftStateLogBase = filepath.Join(dataDir, "data", "valuelog", "newRaftState")
	anotherNewPersisterBase = filepath.Join(dataDir, "data", "dbfile", "newKeyIndex")
}

// mergeRoundPaths 给出第 round 轮归并要用的新日志与新存储引擎路径。
// 基名固定，只按轮号加一个后缀，因此第 N 轮永远是 <基名>_N，与轮数无关地可预测——
// 崩溃恢复要按同样的规则重新算出这两个路径。
func mergeRoundPaths(round int) (logPath, dbPath string) {
	return fmt.Sprintf("%s_%d", anotherNewRaftStateLogBase, round),
		fmt.Sprintf("%s_%d", anotherNewPersisterBase, round)
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
		return kvs.absorbTail(startTime)
	}

	// 本轮的路径由基名加轮号派生，不再就地改写全局变量——见 InitAnotherGCPaths 的注释。
	newLogPath, newDBPath := mergeRoundPaths(kvs.numGC + 1)

	// 创建新的RocksDB实例===========
	persister_new, err := kvs.NewPersister() // 创建一个新的用于保存key和index的persister
	if err != nil {
		return fmt.Errorf("failed to create new persister: %v", err)
	}
	newPersister, err := persister_new.Init(newDBPath, true)
	if err != nil {
		return fmt.Errorf("failed to initialize new RocksDB: %v", err)
	}

	// 创建新的RaftState日志文件=============
	if _, err := os.Stat(newLogPath); err == nil {
		fmt.Println("New RaftState log file already exists. Skipping creation.")
	} else if os.IsNotExist(err) {
		newRaftStateLog, err := os.Create(newLogPath)
		if err != nil {
			return fmt.Errorf("failed to create new RaftState log: %v", err)
		}
		defer newRaftStateLog.Close()
	} else {
		return fmt.Errorf("error checking new RaftState log file: %v", err)
	}

	kvs.anotherStartGC = true

	// 切换到新的文件和RocksDB
	kvs.AnotherSwitchToNewFiles(newLogPath, newPersister, newDBPath)
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1)) // see the function's comment in GC.go
	kvs.switchedPersister = newPersister

	return kvs.absorbTail(startTime)
}

// 更新 GC 后合并的过程，之前的合并方式有问题，问题如下：
// oldEntryChan是从无序的文件中读取的一个个entry，
// 而exixstingEntryChan是从一个有序的文件中读取一个个entry，
// 当有序的文件合并完时，无序的文件还有数据，则现有的代码会直接简单将剩余的无序读出的一个个entry写入，
// 但是这是不对的，可能无序后面多余的entry写入会存在小于前者已经写入的entry的数据。

// 上述也是没问题的，因为是从数据库文件遍历key对应的offset，再去找对应的entry，所以自然就带有去重的功能。
