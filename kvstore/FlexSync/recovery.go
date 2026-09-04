package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/raft"
)

// kvState is what the KV layer persists for crash recovery. It is written at two
// low-frequency points only, the GC file switch and the round's completion; the write
// path never touches it.
type kvState struct {
	NumGC        int    `json:"num_gc"`
	CurrentLog   string `json:"current_log"`
	CurrentDB    string `json:"current_db"`
	SortedFile   string `json:"sorted_file"` // latest completed sorted file; empty = no GC yet
	GCInProgress bool   `json:"gc_in_progress"`
	OldLog       string `json:"old_log"` // the next two are meaningful only while GCInProgress
	OldDB        string `json:"old_db"`
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
	kvs.lastSortedFileIndex = kvs.firstSortedFileIndex // 更新本轮的变量为上一次
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
	kvs.lastSortedFileIndex = kvs.anothersortedFileIndex // 更新本轮的变量为上一次
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

	// Completed GC rounds: rebuild the sorted-file index and set the read-path flags to
	// the "GC completed" position.
	if st.SortedFile != "" {
		if _, err := os.Stat(st.SortedFile); err != nil {
			log.Fatalf("[RECOVER] sorted file named by the state file is unavailable: %v", err)
		}
		switch {
		case st.NumGC >= 2 && !(st.GCInProgress && st.NumGC == 2):
			// round two completed
			if err := kvs.AnotherCreateIndex(st.SortedFile); err != nil {
				log.Fatalf("[RECOVER] rebuild merged sorted-file index: %v", err)
			}
			kvs.anotherSortedFilePath = st.SortedFile
			kvs.lastSortedFileIndex = kvs.anothersortedFileIndex
		default:
			// only round one completed (or round two in flight, in which case SortedFile
			// is round one's output)
			if err := kvs.CreateIndex(st.SortedFile); err != nil {
				log.Fatalf("[RECOVER] rebuild sorted-file index: %v", err)
			}
			kvs.lastSortedFileIndex = kvs.firstSortedFileIndex
		}
		kvs.FirstGC = false
		kvs.startGC = true
		kvs.lastGCFinish = true
		fmt.Printf("[RECOVER] sorted-file index rebuilt: %s\n", st.SortedFile)
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
		_ = os.Remove(sorted)
		sortedFile, err := os.Create(sorted)
		if err != nil {
			log.Fatalf("[RECOVER] create sorted file: %v", err)
		}
		defer sortedFile.Close()
		oldFile, err := os.Open(kvs.oldLog)
		if err != nil {
			log.Fatalf("[RECOVER] open old log: %v", err)
		}
		defer oldFile.Close()
		kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
		if err := kvs.firstGCMigrate(sortedFile, sorted, oldFile, startTime); err != nil {
			log.Fatalf("[RECOVER] redo GC round 1 migration: %v", err)
		}
		if kvs.firstSortedFileIndex == nil {
			log.Fatalf("[RECOVER] GC round 1 redo produced no index")
		}
		kvs.finishFirstGC(startTime)
		return
	}
	merged := fmt.Sprintf("%s_merged_%d", kvs.lastSortedFileIndex.FilePath, kvs.numGC)
	_ = os.Remove(merged)
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
	if err := kvs.mergeIntoSortedFile(startTime); err != nil {
		log.Fatalf("[RECOVER] redo GC round %d migration: %v", kvs.numGC, err)
	}
	if kvs.anothersortedFileIndex == nil {
		log.Fatalf("[RECOVER] GC round %d redo produced no index", kvs.numGC)
	}
	kvs.finishAnotherGC(startTime)
}
