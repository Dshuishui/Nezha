package raft

import (
	"fmt"
)

// 装载一份快照之后，Raft 的日志状态要整体换掉。
//
// 与恢复的区别只在于"循环正在跑"：RecoverLog 的注释写着调用方保证还没有任何循环启动，
// 所以它不加锁。装快照发生在一个正在服务的节点上，于是这里负责把它包进正确的锁里，
// 并且负责换掉写句柄——快照带来的是另一个日志文件，旧句柄还指着本节点自己那份。

// InstallSnapshotState 用一份刚落盘的快照替换本节点的日志状态，返回替换后的最后一个 index。
//
// base/baseTerm 是快照的位点（比它更早的数据在分区文件里，日志里没有）；logFile 是随快照
// 送来的那段日志，已经放到最终位置；applied 是快照里存储引擎所对应的 applied index。
//
// 加锁顺序是 **rf.mu 外、logMu 内**。这不是随便选的：写路径本来就是这个顺序
// （WriteEntryToFile 要求调用方持有 rf.mu，自己再取 logMu），反过来嵌套会与写路径
// 互相等待。CutLog 之所以能先取 logMu，是因为它取完就放、不嵌套。
func (rf *Raft) InstallSnapshotState(base int, baseTerm int32, logFile LogFile, applied int) (int, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 先放掉本节点原来那个日志文件的句柄。不关的话，RecoverLog 会以 O_RDWR 再开一次
	// 同名或不同名的文件，而 rf.logOffset 仍指着旧文件的末尾——之后第一次写入就会
	// 落在新文件的错误位置上。
	rf.logMu.Lock()
	if rf.logWriter != nil {
		rf.logWriter.Flush()
		rf.logWriter = nil
	}
	if rf.logFile != nil {
		rf.logFile.Close()
		rf.logFile = nil
	}
	rf.logMu.Unlock()

	rf.lastIncludedIndex, rf.lastIncludedTerm = base, baseTerm
	rf.fileBaseIndex, rf.fileBaseTerm = base, baseTerm
	// stateLoaded 让 RecoverLog 去核对"状态文件说的基址"与"日志里第一条记录推出来的基址"
	// 是否一致。装快照时两者都来自同一份清单，所以这个核对是在检查落位有没有出错。
	rf.stateLoaded = true

	last, err := rf.RecoverLog([]LogFile{logFile}, applied)
	if err != nil {
		return 0, fmt.Errorf("replay the snapshot's log file: %w", err)
	}

	// 写句柄接到新日志上，放在 RecoverLog **之后**：它可能截掉末尾半条记录，
	// 而 openLogFile 用 Seek(0, END) 取写入位置，先开就会拿到截断前的长度。
	rf.logMu.Lock()
	rf.currentLog, rf.logVersion = logFile.Path, logFile.Version
	// pendingBase 是"下一次 GC 删掉旧文件时要提升成的基址"。装完快照，盘上最老的日志
	// 就是这一份，它的基址就是 base；不摆正的话下一轮 GC 会把一个属于旧历史的基址持久化。
	rf.pendingBaseIndex, rf.pendingBaseTerm = base, baseTerm
	err = rf.openLogFile(logFile.Path)
	rf.logMu.Unlock()
	if err != nil {
		return 0, fmt.Errorf("open the snapshot's log file: %w", err)
	}

	rf.persistHardState()
	return last, nil
}

// SnapshotNeeded reports whether peerId has fallen behind what the in-memory log can
// still serve, which is the condition the Raft paper gives for sending a snapshot:
// "Leaders resort to sending a snapshot only when they have already discarded the next
// log entry needed to replicate entries to the follower with AppendEntries."
//
// It is deliberately not a "how many entries behind" threshold. etcd does the same thing
// -- maybeSendAppend switches to a snapshot exactly when it cannot read term(Next-1) or
// entries(Next, ...) -- and the reason is that any separate threshold would be a second,
// independently tunable definition of "too far behind" that could disagree with the one
// the truncation policy already implies.
func (rf *Raft) SnapshotNeeded(peerId int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != ROLE_LEADER || rf.nextIndex == nil || peerId == rf.me {
		return false
	}
	return rf.index2LogPos(rf.nextIndex[peerId]) < 0
}
