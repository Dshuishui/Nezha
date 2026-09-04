package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// hardState 是 Raft 要求在应答任何 RPC 之前落盘的那部分状态，外加"日志文件从哪一条
// 开始"的基址。日志本身就是 valuelog 文件，不在这里重复存。
//
//	CurrentTerm / VotedFor  标准 Raft 持久化状态，选举与看到更高任期时写
//	BaseIndex / BaseTerm    最老的仍保留的日志文件之前那一条的 index/term。
//	                        GC 删旧日志时推进；重启后 lastIncludedIndex/Term 取自这里，
//	                        并与文件里第一条记录的 index 交叉校验。
type hardState struct {
	CurrentTerm int   `json:"current_term"`
	VotedFor    int   `json:"voted_for"`
	BaseIndex   int   `json:"base_index"`
	BaseTerm    int32 `json:"base_term"`
}

// WriteFileAtomic 写临时文件、fsync、rename、再 fsync 目录。崩在任何一步，旧文件仍完整。
func WriteFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { tmp.Close(); os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// persistHardState 把 term/votedFor/base 落盘。调用方持有 rf.mu。
// 这是每次任期或投票变化时的一次小文件 fsync，只发生在选举路径上，不在写路径上。
func (rf *Raft) persistHardState() {
	if rf.stateFile == "" {
		return // 未启用持久化（单测或旧的调用方式）
	}
	hs := hardState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		BaseIndex:   rf.fileBaseIndex,
		BaseTerm:    rf.fileBaseTerm,
	}
	data, err := json.Marshal(hs)
	if err != nil {
		panic(fmt.Sprintf("marshal raft hard state: %v", err))
	}
	if err := WriteFileAtomic(rf.stateFile, data); err != nil {
		// 持久化失败不能继续当 leader/投票：宁可停机，也不能违反 Raft 的安全性前提。
		panic(fmt.Sprintf("persist raft hard state to %s: %v", rf.stateFile, err))
	}
}

// loadHardState 读取状态文件；文件不存在视为全新节点，返回 (zero, false, nil)。
func loadHardState(path string) (hardState, bool, error) {
	var hs hardState
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return hs, false, nil
	}
	if err != nil {
		return hs, false, err
	}
	if err := json.Unmarshal(data, &hs); err != nil {
		return hs, false, fmt.Errorf("parse %s: %v", path, err)
	}
	return hs, true, nil
}
