package raft

import (
	"errors"
	"fmt"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
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
// （writeEntries 要求调用方持有 rf.mu，自己再取 logMu），反过来嵌套会与写路径
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

// ---- leader 侧：什么时候发、发完怎么算 ----
//
// 在此之前 leader 对每个 peer 只有 nextIndex / matchIndex 两个数，没有"这个 peer 正在收
// 快照"这个状态，所以只能"每轮都跳过、每轮都重试"。按 etcd/raft 的形状补一个最小的进度机：
//
//	replicating   正常发日志
//	snapshotting  正在发快照，**停发日志**，直到安装结果回来
//
// 三条不能省的要求，都是调研得来的（出处见 notes/REF-raft-snapshot.md）：
//
//  1. **回执必须接上。** etcd 对处于 StateSnapshot 的 peer 无条件 IsPaused()，一条 MsgApp
//     都不发；而 ReportSnapshot 的文档写得很直白：follower 装不上而失败又没被上报，
//     "it could end up in a limbo, never getting any updates from the leader"。
//     那正是我们现在"每轮都跳过这个 peer"的症状，所以少了回执等于把同一个 bug 换个形式
//     重做一遍。finishSnapshot 是那个回执。
//  2. **不给失联的 peer 发快照。** etcd 在 maybeSendSnapshot 之前先判 !pr.RecentActive。
//     一个真的联系不上的 peer 被跳过是**正确行为**，不是 bug——给它发几个 GB 只是白占
//     链路，而链路同时还要送日志。
//  3. **快照在飞的时候绝对不能压缩。** 见 compactLog 里那段守卫。
const (
	// maxInflightSnapshots 是同时在传的快照数上限。我们规模小，先定 1。
	// TiKV 是 concurrent-send-snap-limit / concurrent-recv-snap-limit 各 32，
	// CockroachDB 是 apply=1 / send=2。
	maxInflightSnapshots = 1
	// snapshotRetryPause 是一次失败之后暂停多久再重试同一个 peer。
	// etcd 的做法等价：转回 probe 之后要等一个心跳间隔才会再动它。
	snapshotRetryPause = 3 * time.Second
	// releaseDelayAfterSnapshot 是传输结束之后继续保护日志的时长，照搬 etcd 的同名常量。
	// 为什么还要多保护一段：接收端装完、回执到达、leader 更新 nextIndex 之间有间隔，
	// 这期间若恢复压缩，那个 peer 追到的位点可能又落在新的压缩点之前，于是
	// "likely entering a never ending loop of snapshots"（cockroachdb#8629 的原话）。
	releaseDelayAfterSnapshot = 30 * time.Second
	// peerActiveWindow 是"最近活跃"的窗口。取选举超时，与 etcd 清 RecentActive 的节奏
	// （每次 MsgCheckQuorum，也就是一个选举超时）一致。
	peerActiveWindow = minElectionTimeout
)

// ErrSnapshotUnavailable 表示状态机现在做不出快照，稍后重试即可，不是故障。
// 对应 etcd 的 ErrSnapshotTemporarilyUnavailable。
var ErrSnapshotUnavailable = errors.New("snapshot temporarily unavailable")

// SnapshotPayload 是状态机交出来的一份快照。Release 由 Raft 在传输结束后调用，
// 成功失败都会调用一次——那是分区文件引用与导出产物的唯一释放点。
type SnapshotPayload struct {
	LastIncludedIndex int
	LastIncludedTerm  int32
	// LastIndex 是接收端装完之后日志覆盖到的 index，leader 从它的下一个 index 继续复制。
	LastIndex int
	Files     []SSTableFile
	Release   func()
}

// SnapshotSource 由状态机实现：物化一份当前状态的快照。
type SnapshotSource func() (SnapshotPayload, error)

type peerProgress int

const (
	progressReplicating peerProgress = iota
	progressSnapshotting
)

type peerSnapshot struct {
	state      peerProgress
	index      int       // 在飞那次快照的 LastIndex，回执识别用
	startAt    time.Time // 这次传输开始的时刻，供诊断
	pauseUntil time.Time // 失败之后暂停重试到这个时刻
}

// SetSnapshotSource 打开给落后 peer 发快照的能力。rateBytesPerSec <= 0 表示不限速。
// 需在节点开始服务前调用。
func (rf *Raft) SetSnapshotSource(fn SnapshotSource, rateBytesPerSec int64) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.snapSource = fn
	rf.snapRate = rateBytesPerSec
}

// ensurePeerSnapLocked 惰性分配每 peer 的进度。单测里的 Raft 是用结构体字面量造的，
// 这些切片没有被 Make 初始化过。
func (rf *Raft) ensurePeerSnapLocked() {
	if len(rf.peerSnap) != len(rf.peers) {
		rf.peerSnap = make([]peerSnapshot, len(rf.peers))
	}
	if len(rf.peerActiveAt) != len(rf.peers) {
		rf.peerActiveAt = make([]time.Time, len(rf.peers))
	}
}

// notePeerActive 记下一次来自 peer 的回执。任何回执都算，成功与否无关——
// 这个字段回答的是"它还在不在"，不是"它跟上了没有"。调用方须持有 rf.mu。
func (rf *Raft) notePeerActive(peerId int) {
	rf.ensurePeerSnapLocked()
	if peerId >= 0 && peerId < len(rf.peerActiveAt) {
		rf.peerActiveAt[peerId] = time.Now()
	}
}

// peerRecentlyActiveLocked 是 etcd 的 pr.RecentActive。从没回执过的 peer 算不活跃：
// 零值时间距今远超窗口，正好是想要的语义。
func (rf *Raft) peerRecentlyActiveLocked(peerId int) bool {
	if peerId < 0 || peerId >= len(rf.peerActiveAt) {
		return false
	}
	return time.Since(rf.peerActiveAt[peerId]) < peerActiveWindow
}

// snapshotsInFlight 供 compactLog 判断现在能不能压缩。调用方须持有 rf.mu。
func (rf *Raft) snapshotsInFlightLocked() bool {
	return rf.inflightSnaps > 0 || time.Now().Before(rf.snapReleaseAt)
}

// maybeSendSnapshot 由复制循环每轮对每个 peer 调一次。
// 返回 true 表示这个 peer 本轮由快照负责，**不要**再给它发日志。
func (rf *Raft) maybeSendSnapshot(peerId int) bool {
	rf.mu.Lock()
	if rf.snapSource == nil || rf.role != ROLE_LEADER || rf.nextIndex == nil || peerId == rf.me {
		rf.mu.Unlock()
		return false
	}
	rf.ensurePeerSnapLocked()
	ps := &rf.peerSnap[peerId]
	if ps.state == progressSnapshotting {
		rf.mu.Unlock()
		return true // 正在传，停发日志
	}
	// 判据就是论文那条，不额外定"落后多少条"：
	// "Leaders resort to sending a snapshot only when they have already discarded the
	// next log entry needed to replicate entries to the follower with AppendEntries."
	// 多一个独立可调的阈值，就多一个"多远算太远"的定义，它迟早与截断策略隐含的那个不一致。
	if rf.index2LogPos(rf.nextIndex[peerId]) >= 0 {
		rf.mu.Unlock()
		return false // 内存日志还够，正常复制
	}
	if time.Now().Before(ps.pauseUntil) {
		rf.mu.Unlock()
		return true // 刚失败过。发日志也是白发（起点仍在压缩点之前），别刷屏
	}
	if !rf.peerRecentlyActiveLocked(peerId) {
		rf.mu.Unlock()
		// 心跳仍然照发，它回来了就会更新活跃时刻，下一轮自然进得来。
		return true
	}
	if rf.inflightSnaps >= maxInflightSnapshots {
		rf.mu.Unlock()
		return true // 排队等，别同时占满链路
	}
	ps.state = progressSnapshotting
	ps.startAt = time.Now()
	rf.inflightSnaps++
	source, rate := rf.snapSource, rf.snapRate
	next := rf.nextIndex[peerId]
	rf.mu.Unlock()

	fmt.Printf("[SNAPSHOT] peer[%d] 的 nextIndex=%d 已落在压缩点之前，开始给它发快照\n", peerId, next)
	go rf.shipSnapshot(peerId, source, rate)
	return true
}

// shipSnapshot 做一份快照并发给 peerId。无论走哪条路都必须走到 finishSnapshot，
// 否则那个 peer 会永远停在 snapshotting 上——既不收日志也不再收快照，
// 正是 etcd 说的 "end up in a limbo"。
func (rf *Raft) shipSnapshot(peerId int, source SnapshotSource, rate int64) {
	payload, err := source()
	if err != nil {
		if errors.Is(err, ErrSnapshotUnavailable) {
			util.DPrintf("RaftNode[%d] peer[%d] 暂时做不出快照，稍后重试", rf.me, peerId)
		} else {
			util.EPrintf("RaftNode[%d] peer[%d] 制作快照失败: %v", rf.me, peerId, err)
		}
		rf.finishSnapshot(peerId, 0, false)
		return
	}
	if payload.Release != nil {
		defer payload.Release()
	}

	rf.mu.Lock()
	rf.ensurePeerSnapLocked()
	rf.peerSnap[peerId].index = payload.LastIndex
	rf.mu.Unlock()

	span := SSTableSpan{
		Kind:            raftrpc.InstallSSTableKind_SNAPSHOT,
		End:             payload.LastIndex,
		Files:           payload.Files,
		RateBytesPerSec: rate,
	}
	t0 := time.Now()
	// 与 appendEntriesLoop 的 sendAppend 同一个做法：单测顶掉真正的传输，
	// 以便在不起 gRPC 的前提下驱动整条进度机。生产路径上它是 nil。
	send := rf.sendSnapshotFn
	if send == nil {
		send = rf.SendSSTable
	}
	resp, err := send(peerId, span)
	if err != nil {
		util.EPrintf("RaftNode[%d] 给 peer[%d] 发快照失败（%v 之后）: %v", rf.me, peerId, time.Since(t0), err)
		rf.finishSnapshot(peerId, 0, false)
		return
	}
	switch resp.Status {
	case raftrpc.InstallSSTableStatus_INGESTED, raftrpc.InstallSSTableStatus_SKIPPED:
		fmt.Printf("[SNAPSHOT] peer[%d] %s，它的日志现在到 %d（耗时 %v）\n",
			peerId, resp.Status, resp.Applied, time.Since(t0))
		rf.finishSnapshot(peerId, int(resp.Applied), true)
	case raftrpc.InstallSSTableStatus_STALE_TERM:
		// 对端的任期更高，本节点已经不是 leader 了。不改 nextIndex——那是下一任
		// leader 的事——只把状态放回去。
		util.DPrintf("RaftNode[%d] peer[%d] 用更高的任期拒绝了快照", rf.me, peerId)
		rf.finishSnapshot(peerId, 0, false)
	default:
		util.EPrintf("RaftNode[%d] peer[%d] 没装上快照: %s", rf.me, peerId, resp.Status)
		rf.finishSnapshot(peerId, 0, false)
	}
}

// finishSnapshot 是安装结果的回执。upto 是接收端装完之后日志覆盖到的 index。
func (rf *Raft) finishSnapshot(peerId int, upto int, ok bool) {
	rf.mu.Lock()
	rf.ensurePeerSnapLocked()
	ps := &rf.peerSnap[peerId]
	// 顺序要紧。etcd 在对应的位置写着 "the order here matters"：先清掉在飞记录，
	// 再把状态转回正常复制。反过来的话，中间那一瞬这个 peer 已经"可以发日志"了，
	// 而在飞计数还没减、压缩仍被挡着——不致命，但两个不变式短暂地互相矛盾，
	// 而这类矛盾正是后来读代码的人会当成"另一个 bug"去追的东西。
	ps.index = 0
	if rf.inflightSnaps > 0 {
		rf.inflightSnaps--
	}
	// 传输结束之后再保护一段日志，理由见 releaseDelayAfterSnapshot。
	rf.snapReleaseAt = time.Now().Add(releaseDelayAfterSnapshot)
	ps.state = progressReplicating
	if ok && upto > 0 && rf.role == ROLE_LEADER && rf.nextIndex != nil {
		if upto+1 > rf.nextIndex[peerId] {
			rf.nextIndex[peerId] = upto + 1
		}
		if upto > rf.matchIndex[peerId] {
			rf.matchIndex[peerId] = upto
			rf.updateCommitIndex()
		}
		// 装好了就不再是"永久卡住"，重新报警的资格要还给它。
		if rf.stuckReported != nil {
			delete(rf.stuckReported, peerId)
		}
	} else {
		ps.pauseUntil = time.Now().Add(snapshotRetryPause)
	}
	rf.mu.Unlock()
	rf.wakeReplication()
}

// resetPeerSnapLocked 卸任时清掉本任期的快照记账。在飞的传输仍会走到 finishSnapshot，
// 那里会再判一次角色，所以这里只需把状态摆回去。调用方须持有 rf.mu。
func (rf *Raft) resetPeerSnapLocked() {
	for i := range rf.peerSnap {
		rf.peerSnap[i] = peerSnapshot{}
	}
}
