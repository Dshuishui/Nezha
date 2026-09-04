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

// kvState 是 KV 层为崩溃恢复落盘的状态。只在两个低频时刻写：GC 切换文件时、GC 完成时。
// 写路径不碰它。
type kvState struct {
	NumGC        int    `json:"num_gc"`
	CurrentLog   string `json:"current_log"`
	CurrentDB    string `json:"current_db"`
	SortedFile   string `json:"sorted_file"` // 最近一轮完成的排序文件；空 = 尚未 GC
	GCInProgress bool   `json:"gc_in_progress"`
	OldLog       string `json:"old_log"` // 以下两项仅 GCInProgress 时有意义
	OldDB        string `json:"old_db"`
}

func (kvs *KVServer) stateFilePath() string {
	return filepath.Join(kvs.dataDir, "data", "kv_state.json")
}

// saveKVState 把当前 GC 相关状态原子写盘。调用方持有 kvs.mu，或处于 GC 协程且状态稳定。
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
		// 状态写不下去就不能继续：否则 GC 删了旧文件，重启后却不知道该读哪里。
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

// finishFirstGC 是第一轮 GC 搬运成功之后的收尾，触发循环与恢复重做共用。
// 顺序：先把新基址和新状态落盘，再删旧文件——崩在中间时旧文件还在、状态指向新文件，
// 重启只会多出一个没人引用的旧文件。
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

// finishAnotherGC 是第二轮（合并式）GC 搬运成功之后的收尾。
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

// recoverOrInit 决定本次启动是全新节点还是从磁盘恢复，并把 KV 层状态接回来。
// 返回 Raft 恢复所需的日志文件列表（旧到新）与已 apply 下标；全新节点返回空列表。
//
// 全新与恢复的判据是 kv_state.json 是否存在。全新节点在这里就写一份初始状态，
// 于是"第一次 GC 之前崩溃"的重启也能走恢复路径。
func (kvs *KVServer) recoverOrInit(initialDB string) (files []raft.LogFile, applied int) {
	st, ok, err := loadKVState(kvs.stateFilePath())
	if err != nil {
		log.Fatalf("读取 KV 状态文件失败：%v", err)
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

	fmt.Printf("[RECOVER] 读到状态：numGC=%d currentLog=%s currentDB=%s sorted=%q gcInProgress=%v\n",
		st.NumGC, st.CurrentLog, st.CurrentDB, st.SortedFile, st.GCInProgress)
	kvs.numGC = st.NumGC
	kvs.currentLog = st.CurrentLog
	kvs.currentDBPath = st.CurrentDB
	kvs.sortedFilePath = st.SortedFile
	if _, err := kvs.persister.Init(st.CurrentDB, true); err != nil {
		log.Fatalf("[RECOVER] 打开当前 RocksDB %s 失败：%v", st.CurrentDB, err)
	}
	applied, _, err = kvs.persister.GetApplied()
	if err != nil {
		log.Fatalf("[RECOVER] 读取 applied 下标失败：%v", err)
	}

	// GC 已完成的轮次：重建排序文件索引，把读路径的开关拨到"GC 已完成"的位置。
	if st.SortedFile != "" {
		if _, err := os.Stat(st.SortedFile); err != nil {
			log.Fatalf("[RECOVER] 状态文件指向的排序文件不可用：%v", err)
		}
		switch {
		case st.NumGC >= 2 && !(st.GCInProgress && st.NumGC == 2):
			// 第二轮已完成
			if err := kvs.AnotherCreateIndex(st.SortedFile); err != nil {
				log.Fatalf("[RECOVER] 重建合并排序文件索引失败：%v", err)
			}
			kvs.anotherSortedFilePath = st.SortedFile
			kvs.lastSortedFileIndex = kvs.anothersortedFileIndex
		default:
			// 只有第一轮完成（或第二轮进行中，此时 SortedFile 是第一轮的输出）
			if err := kvs.CreateIndex(st.SortedFile); err != nil {
				log.Fatalf("[RECOVER] 重建排序文件索引失败：%v", err)
			}
			kvs.lastSortedFileIndex = kvs.firstSortedFileIndex
		}
		kvs.FirstGC = false
		kvs.startGC = true
		kvs.lastGCFinish = true
		fmt.Printf("[RECOVER] 排序文件索引已重建：%s\n", st.SortedFile)
	}

	if st.GCInProgress {
		// 切换已生效、搬运未完成：旧日志里 index > applied 的条目还没进任何排序文件，
		// 必须连同旧库一起恢复出来，随后重做搬运。
		kvs.gcInProgress = true
		kvs.oldLog = st.OldLog
		kvs.oldDBPath = st.OldDB
		oldP := &raft.Persister{}
		if _, err := oldP.Init(st.OldDB, true); err != nil {
			log.Fatalf("[RECOVER] 打开旧 RocksDB %s 失败：%v", st.OldDB, err)
		}
		kvs.oldPersister = oldP
		if a, ok, err := oldP.GetApplied(); err == nil && ok && a > applied {
			applied = a // 旧库里的 applied 可能更新（切换后第一条 apply 之前崩溃）
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
		fmt.Printf("[RECOVER] 第 %d 轮 GC 曾中断，重启后将重做搬运（旧日志 %s）\n", st.NumGC, st.OldLog)
	}
	files = append(files, raft.LogFile{Path: st.CurrentLog, Version: int32(st.NumGC)})
	fmt.Printf("[RECOVER] applied=%d，将从 %d 个日志文件重建 Raft 日志\n", applied, len(files))
	return files, applied
}

// resumeInterruptedGC 在恢复完成、各 loop 启动之后重做被打断的那一轮 GC 搬运。
// 输出文件若已有残缺的半成品，先删掉。
func (kvs *KVServer) resumeInterruptedGC() {
	startTime := time.Now()
	if kvs.numGC == 1 {
		sorted := firstSortedFilePath // InitGCPaths 给的就是第一轮的输出名 .../RaftState_sorted_1
		_ = os.Remove(sorted)
		sortedFile, err := os.Create(sorted)
		if err != nil {
			log.Fatalf("[RECOVER] 创建排序文件失败：%v", err)
		}
		defer sortedFile.Close()
		oldFile, err := os.Open(kvs.oldLog)
		if err != nil {
			log.Fatalf("[RECOVER] 打开旧日志失败：%v", err)
		}
		defer oldFile.Close()
		kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
		if err := kvs.firstGCMigrate(sortedFile, sorted, oldFile, startTime); err != nil {
			log.Fatalf("[RECOVER] 重做第 1 轮 GC 搬运失败：%v", err)
		}
		if kvs.firstSortedFileIndex == nil {
			log.Fatalf("[RECOVER] 重做第 1 轮 GC 后没有索引")
		}
		kvs.finishFirstGC(startTime)
		return
	}
	merged := fmt.Sprintf("%s_merged_%d", kvs.lastSortedFileIndex.FilePath, kvs.numGC)
	_ = os.Remove(merged)
	kvs.waitOldVersionApplied(int32(kvs.numGC - 1))
	if err := kvs.mergeIntoSortedFile(startTime); err != nil {
		log.Fatalf("[RECOVER] 重做第 %d 轮 GC 搬运失败：%v", kvs.numGC, err)
	}
	if kvs.anothersortedFileIndex == nil {
		log.Fatalf("[RECOVER] 重做第 %d 轮 GC 后没有索引", kvs.numGC)
	}
	kvs.finishAnotherGC(startTime)
}
