package kvstore

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// kvState is what the KV layer persists for crash recovery. It is written at two
// low-frequency points only, the GC file switch and the round's completion; the write
// path never touches it.
type kvState struct {
	NumGC      int    `json:"num_gc"`
	CurrentLog string `json:"current_log"`
	CurrentDB  string `json:"current_db"`
	// SortedFile 是最近一轮完成的分区组的**基名**；为空表示还没跑过 GC。
	// 它本身不是一个文件，实际数据在 Partitions 列出的 <基名>.pN 里。
	SortedFile string `json:"sorted_file"`
	// Partitions 是分区清单，按 key 升序。重启时按它直接重建分区边界，不必先扫描
	// 全部数据文件才知道被切成了几段、各覆盖哪一段 key。
	Partitions   []partitionMeta `json:"partitions"`
	GCInProgress bool            `json:"gc_in_progress"`
	OldLog       string          `json:"old_log"` // the next two are meaningful only while GCInProgress
	OldDB        string          `json:"old_db"`
}

func (kvs *KVServer) stateFilePath() string {
	return filepath.Join(kvs.dataDir, "data", "kv_state.json")
}

// saveKVState writes the GC-related state atomically. The caller holds kvs.mu, or runs on
// the GC goroutine while the state is stable.
func (kvs *KVServer) saveKVState() {
	st := kvState{
		NumGC:        kvs.numGC,
		CurrentLog:   kvs.currentLog,
		CurrentDB:    kvs.currentDBPath,
		SortedFile:   kvs.sortedFilePath,
		Partitions:   kvs.lastPartitions.manifest(),
		GCInProgress: kvs.gcInProgress,
	}
	if kvs.gcInProgress {
		st.OldLog = kvs.oldLog
		st.OldDB = kvs.oldDBPath
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		panic(fmt.Sprintf("marshal kv state: %v", err))
	}
	if err := raft.WriteFileAtomic(kvs.stateFilePath(), data); err != nil {
		// Without a durable state file GC would delete old files that a restart still
		// needs; stopping is the only safe choice.
		panic(fmt.Sprintf("persist kv state: %v", err))
	}
}

func loadKVState(path string) (kvState, bool, error) {
	var st kvState
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return st, false, nil
	}
	if err != nil {
		return st, false, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, false, fmt.Errorf("parse %s: %v", path, err)
	}
	return st, true, nil
}

// finishFirstGC completes round one after a successful migration; the trigger loop and
// the recovery redo share it. Order matters: persist the new log base and the new state
// first, delete the old file last. A crash in between leaves the old file on disk with
// the state already pointing at the new one, which is merely an unreferenced file.
func (kvs *KVServer) finishFirstGC(startTime time.Time) {
	fmt.Printf("垃圾回收完成，共花费了%v\n", time.Since(startTime))
	kvs.raft.PersistLogBase()
	kvs.mu.Lock()
	kvs.lastGCFinish = true
	kvs.FirstGC = false
	kvs.lastPartitions = kvs.firstPartitions // 更新本轮的变量为上一次
	kvs.sortedFilePath = kvs.firstSortedFilePath
	kvs.gcInProgress = false
	kvs.saveKVState()
	kvs.mu.Unlock()
	if err := os.Remove(kvs.oldLog); err != nil {
		fmt.Println("第 1 轮删除旧文件出现了错误: ", err)
	}
	fmt.Println("第 1 轮垃圾回收完成，等待下 1 轮垃圾回收，且已删除 oldLog 指向的文件")
}

// finishAnotherGC completes the second (merging) round after a successful migration.
func (kvs *KVServer) finishAnotherGC(startTime time.Time) {
	fmt.Printf("垃圾回收完成，共花费了%v\n", time.Since(startTime))
	kvs.raft.PersistLogBase()
	kvs.mu.Lock()
	kvs.anotherStartGC, kvs.anotherEndGC = false, false
	kvs.lastGCFinish = true
	kvs.lastPartitions = kvs.anotherPartitions // 更新本轮的变量为上一次
	kvs.sortedFilePath = kvs.anotherSortedFilePath
	kvs.gcInProgress = false
	kvs.saveKVState()
	kvs.mu.Unlock()
	if err := os.Remove(kvs.oldLog); err != nil {
		fmt.Printf("第 %v 轮垃圾回收删除旧文件出现了错误: %v\n", kvs.numGC, err)
	}
	fmt.Printf("第 %v 轮垃圾回收完成，等待下一轮垃圾回收，且已删除 oldLog 指向的文件\n", kvs.numGC)
}

// recoverOrInit decides whether this start is a fresh node or a recovery from disk and
// restores the KV-layer state. It returns the log files Raft must replay (oldest first)
// and the applied index; a fresh node gets an empty list.
//
// The test is whether kv_state.json exists. A fresh node writes an initial state file
// here, so a restart after a crash that precedes the first GC also takes the recovery path.
func (kvs *KVServer) recoverOrInit(initialDB string) (files []raft.LogFile, applied int) {
	st, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		log.Fatalf("read KV state file: %v", err)
	}
	if !ok {
		if _, err := kvs.persister.Init(initialDB, true); err != nil {
			log.Fatalf("Failed to initialize database: %v", err)
		}
		kvs.numGC = 0
		kvs.currentDBPath = initialDB
		kvs.currentLog = kvs.InitialRaftStateLog
		kvs.saveKVState()
		return nil, 0
	}

	fmt.Printf("[RECOVER] state: numGC=%d currentLog=%s currentDB=%s sorted=%q gcInProgress=%v\n",
		st.NumGC, st.CurrentLog, st.CurrentDB, st.SortedFile, st.GCInProgress)
	kvs.numGC = st.NumGC
	kvs.currentLog = st.CurrentLog
	kvs.currentDBPath = st.CurrentDB
	kvs.sortedFilePath = st.SortedFile
	if _, err := kvs.persister.Init(st.CurrentDB, true); err != nil {
		log.Fatalf("[RECOVER] open current RocksDB %s: %v", st.CurrentDB, err)
	}
	applied, _, err = kvs.persister.GetApplied()
	if err != nil {
		log.Fatalf("[RECOVER] read applied index: %v", err)
	}

	// Completed GC rounds: rebuild the partition set and set the read-path flags to the
	// "GC completed" position. 边界取自清单，稀疏索引仍要扫每个分区重建。
	if st.SortedFile != "" {
		parts, err := kvs.loadPartitionSet(st.SortedFile, st.Partitions)
		if err != nil {
			log.Fatalf("[RECOVER] rebuild partition set for %s: %v", st.SortedFile, err)
		}
		kvs.lastPartitions = parts
		switch {
		case st.NumGC >= 2 && !(st.GCInProgress && st.NumGC == 2):
			// round two completed
			kvs.anotherSortedFilePath = st.SortedFile
			kvs.anotherPartitions = parts
		default:
			// only round one completed (or round two in flight, in which case SortedFile
			// is round one's output)
			kvs.firstSortedFilePath = st.SortedFile
			kvs.firstPartitions = parts
		}
		kvs.FirstGC = false
		kvs.startGC = true
		kvs.lastGCFinish = true
		fmt.Printf("[RECOVER] partition set rebuilt: %s (%d partitions, %d bytes)\n",
			st.SortedFile, parts.Len(), parts.TotalSize())
	}

	if st.GCInProgress {
		// Switch done, migration not: entries with index > applied in the old log are in
		// no sorted file yet, so the old log and index come back too and the migration is
		// redone afterwards.
		kvs.gcInProgress = true
		kvs.oldLog = st.OldLog
		kvs.oldDBPath = st.OldDB
		oldP := &raft.Persister{}
		if _, err := oldP.Init(st.OldDB, true); err != nil {
			log.Fatalf("[RECOVER] open old RocksDB %s: %v", st.OldDB, err)
		}
		kvs.oldPersister = oldP
		if a, ok, err := oldP.GetApplied(); err == nil && ok && a > applied {
			applied = a // the old index may be ahead (crash before the first apply after the switch)
		}
		files = append(files, raft.LogFile{Path: st.OldLog, Version: int32(st.NumGC - 1)})
		if st.NumGC == 1 {
			kvs.FirstGC = true
			kvs.startGC = true
		} else {
			kvs.anotherStartGC, kvs.anotherEndGC = true, false
			kvs.switchedPersister = kvs.persister
			kvs.lastGCFinish = false
		}
		fmt.Printf("[RECOVER] GC round %d was interrupted; its migration will be redone (old log %s)\n", st.NumGC, st.OldLog)
	}
	files = append(files, raft.LogFile{Path: st.CurrentLog, Version: int32(st.NumGC)})
	fmt.Printf("[RECOVER] applied=%d; rebuilding the Raft log from %d file(s)\n", applied, len(files))
	return files, applied
}

// resumeInterruptedGC redoes the migration of the interrupted round once recovery is done
// and the loops are running. A partial output file from the crashed attempt is removed first.
func (kvs *KVServer) resumeInterruptedGC() {
	startTime := time.Now()
	if kvs.numGC == 1 {
		sorted := firstSortedFilePath // InitGCPaths already names round one's output (.../RaftState_sorted_1)
		// 崩溃那次可能已经封口了几个分区，残留的 .pN 必须先清干净：重做会从 .p0 重新编号，
		// 留下的旧文件会被当成本轮产物的一部分。
		if err := removePartitionFiles(sorted); err != nil {
			log.Fatalf("[RECOVER] remove partial partitions of %s: %v", sorted, err)
		}
		oldFile, err := os.Open(kvs.oldLog)
		if err != nil {
			log.Fatalf("[RECOVER] open old log: %v", err)
		}
		defer oldFile.Close()
		kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
		if err := kvs.firstGCMigrate(sorted, oldFile, startTime); err != nil {
			log.Fatalf("[RECOVER] redo GC round 1 migration: %v", err)
		}
		if kvs.firstPartitions == nil {
			log.Fatalf("[RECOVER] GC round 1 redo produced no index")
		}
		kvs.finishFirstGC(startTime)
		return
	}
	merged := fmt.Sprintf("%s_merged_%d", kvs.lastPartitions.Base(), kvs.numGC)
	if err := removePartitionFiles(merged); err != nil {
		log.Fatalf("[RECOVER] remove partial partitions of %s: %v", merged, err)
	}
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
	if err := kvs.absorbTail(startTime); err != nil {
		log.Fatalf("[RECOVER] redo GC round %d migration: %v", kvs.numGC, err)
	}
	if kvs.anotherPartitions == nil {
		log.Fatalf("[RECOVER] GC round %d redo produced no index", kvs.numGC)
	}
	kvs.finishAnotherGC(startTime)
}
