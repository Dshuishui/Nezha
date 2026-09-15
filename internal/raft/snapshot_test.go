package raft

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// 这一组用例钉住的是一件事：**每一条路径都必须走到 finishSnapshot**。
// 少了回执，那个 peer 会永远停在 snapshotting 上——既不收日志也不再收快照。
// etcd 的 ReportSnapshot 文档把这个状态叫 "limbo"，而它正是本改造要消除的老症状
// （"每轮都跳过这个 peer"）换一种形式重现。

func leaderForSnapTest(peers int) *Raft {
	rf := &Raft{me: 0, peers: make([]string, peers), role: ROLE_LEADER, currentTerm: 5}
	rf.nextIndex = make([]int, peers)
	rf.matchIndex = make([]int, peers)
	rf.replicaWake = make(chan struct{}, 1)
	// 日志基址 1000、内存里 100 条：1001..1100 可服务，1000 及更早已压缩。
	rf.lastIncludedIndex = 1000
	rf.lastIncludedTerm = 4
	rf.log = mkLog(100, 5)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = 1101
	}
	return rf
}

// 日志还够用的 peer 不该被推去走快照：补日志比发几个 GB 的状态便宜得多。
func TestSnapshotNotSentWhileLogSuffices(t *testing.T) {
	rf := leaderForSnapTest(3)
	rf.snapSource = func() (SnapshotPayload, error) {
		t.Fatal("日志还够用却去做了快照")
		return SnapshotPayload{}, nil
	}
	rf.nextIndex[1] = 1050 // 仍在内存日志范围内
	rf.notePeerActive(1)
	if rf.maybeSendSnapshot(1) {
		t.Error("应当交回给正常日志复制")
	}
}

// 失联的 peer 被跳过是**正确行为**，不是 bug（etcd 在 maybeSendSnapshot 前判 !RecentActive）。
// 给一个联系不上的节点发几个 GB 只是白占链路，而链路同时还要送日志。
func TestSnapshotSkippedForInactivePeer(t *testing.T) {
	rf := leaderForSnapTest(3)
	called := false
	rf.snapSource = func() (SnapshotPayload, error) {
		called = true
		return SnapshotPayload{}, errors.New("不该被调用")
	}
	rf.nextIndex[1] = 500 // 落在压缩点之前
	// 不调 notePeerActive：从没回执过，零值时间距今远超窗口
	if !rf.maybeSendSnapshot(1) {
		t.Error("失联的 peer 本轮也不该发日志（起点在压缩点之前，发了也白发）")
	}
	time.Sleep(20 * time.Millisecond)
	if called {
		t.Error("给失联的 peer 做了快照")
	}
	rf.mu.Lock()
	inflight := rf.inflightSnaps
	rf.mu.Unlock()
	if inflight != 0 {
		t.Errorf("在飞快照数 = %d; want 0", inflight)
	}
}

// 制作失败也必须回执，而且要留一段重试间隔，否则复制循环会每 20ms 重试一次。
func TestSnapshotSourceFailureStillReports(t *testing.T) {
	rf := leaderForSnapTest(3)
	rf.snapSource = func() (SnapshotPayload, error) { return SnapshotPayload{}, ErrSnapshotUnavailable }
	rf.nextIndex[1] = 500
	rf.notePeerActive(1)
	if !rf.maybeSendSnapshot(1) {
		t.Fatal("应当由快照接手")
	}
	waitFor(t, func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.inflightSnaps == 0 && rf.peerSnap[1].state == progressReplicating
	}, "制作失败之后没有回执：peer 停在 snapshotting 上就是 etcd 说的 limbo")
	rf.mu.Lock()
	pause := rf.peerSnap[1].pauseUntil
	rf.mu.Unlock()
	if !time.Now().Before(pause) {
		t.Error("失败之后没有留重试间隔，复制循环会每个 tick 重试一次")
	}
	// 间隔内不该再发起
	if !rf.maybeSendSnapshot(1) {
		t.Error("重试间隔内本轮仍不该发日志")
	}
	rf.mu.Lock()
	inflight := rf.inflightSnaps
	rf.mu.Unlock()
	if inflight != 0 {
		t.Errorf("重试间隔内又发起了一次，在飞 = %d", inflight)
	}
}

// 装好之后：nextIndex/matchIndex 推到快照覆盖到的位置，状态转回正常复制，
// 而且"永久卡住"的报警资格要还回去（它不再是卡住的）。
func TestSnapshotSuccessAdvancesPeer(t *testing.T) {
	rf := leaderForSnapTest(3)
	rf.stuckReported = map[int]bool{1: true}
	// Release 由传输 goroutine 调用（shipSnapshot 的 defer，在 finishSnapshot 之后），
	// 所以这里必须是原子量并且单独等它——用普通 bool 会被 -race 抓到，而且会漏判。
	var released atomic.Bool
	rf.snapSource = func() (SnapshotPayload, error) {
		return SnapshotPayload{
			LastIncludedIndex: 1000, LastIncludedTerm: 4, LastIndex: 1100,
			Files:   []SSTableFile{{Path: "manifest"}},
			Release: func() { released.Store(true) },
		}, nil
	}
	rf.sendSnapshotFn = func(peerId int, span SSTableSpan) (*raftrpc.InstallSSTableResponse, error) {
		if span.Kind != raftrpc.InstallSSTableKind_SNAPSHOT {
			t.Errorf("载荷类型 = %v; want SNAPSHOT", span.Kind)
		}
		if span.End != 1100 {
			t.Errorf("span.End = %d; want 1100", span.End)
		}
		return &raftrpc.InstallSSTableResponse{
			Applied: 1100, Status: raftrpc.InstallSSTableStatus_INGESTED}, nil
	}
	rf.nextIndex[1] = 500
	rf.notePeerActive(1)
	if !rf.maybeSendSnapshot(1) {
		t.Fatal("应当由快照接手")
	}
	waitFor(t, func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.peerSnap[1].state == progressReplicating && rf.nextIndex[1] == 1101
	}, "装好之后 nextIndex 没有被推上去")
	waitFor(t, released.Load, "Release 没被调用：分区文件的引用和导出产物都泄漏了")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.matchIndex[1] != 1100 {
		t.Errorf("matchIndex = %d; want 1100", rf.matchIndex[1])
	}
	if rf.inflightSnaps != 0 {
		t.Errorf("在飞快照数 = %d; want 0", rf.inflightSnaps)
	}
	if rf.stuckReported[1] {
		t.Error("装好之后仍标着永久卡住——下次真卡住就不会再报了")
	}
}

// 同时最多一个在传。第二个 peer 要等，不能一起把链路占满。
func TestSnapshotConcurrencyLimit(t *testing.T) {
	rf := leaderForSnapTest(3)
	block := make(chan struct{})
	rf.snapSource = func() (SnapshotPayload, error) {
		<-block
		return SnapshotPayload{}, ErrSnapshotUnavailable
	}
	rf.nextIndex[1], rf.nextIndex[2] = 500, 500
	rf.notePeerActive(1)
	rf.notePeerActive(2)

	if !rf.maybeSendSnapshot(1) {
		t.Fatal("peer1 应当由快照接手")
	}
	waitFor(t, func() bool {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		return rf.inflightSnaps == 1
	}, "peer1 的传输没有开始")
	if !rf.maybeSendSnapshot(2) {
		t.Error("peer2 本轮也不该发日志")
	}
	rf.mu.Lock()
	inflight := rf.inflightSnaps
	state2 := rf.peerSnap[2].state
	rf.mu.Unlock()
	if inflight != 1 {
		t.Errorf("在飞快照数 = %d; want 1（上限是 1）", inflight)
	}
	if state2 == progressSnapshotting {
		t.Error("peer2 被标成正在收快照，但它其实在排队——这个状态会让它停收日志")
	}
	close(block)
}

// 快照在飞的时候压缩必须整个跳过，传完之后还要再保持一段。
// 少了这一条，落后节点追到的位点又会落在新的压缩点之前，于是没完没了地重发快照
// （CockroachDB 的原话是 "likely entering a never ending loop of snapshots"）。
func TestCompactionBlockedWhileSnapshotInFlight(t *testing.T) {
	rf := leaderForSnapTest(3)
	rf.mu.Lock()
	if rf.snapshotsInFlightLocked() {
		t.Error("什么都没发的时候压缩不该被挡")
	}
	rf.inflightSnaps = 1
	if !rf.snapshotsInFlightLocked() {
		t.Error("有快照在飞却允许压缩")
	}
	rf.inflightSnaps = 0
	rf.snapReleaseAt = time.Now().Add(time.Minute)
	if !rf.snapshotsInFlightLocked() {
		t.Error("传输刚结束的保护期内允许了压缩")
	}
	rf.snapReleaseAt = time.Now().Add(-time.Second)
	if rf.snapshotsInFlightLocked() {
		t.Error("保护期过了还在挡压缩")
	}
	rf.mu.Unlock()
}

// 卸任要丢掉本任期的快照记账，否则下个任期会带着上个任期的 snapshotting 状态，
// 那个 peer 从一开始就不收日志。
func TestSnapshotProgressClearedOnStepDown(t *testing.T) {
	rf := leaderForSnapTest(3)
	rf.mu.Lock()
	rf.ensurePeerSnapLocked()
	rf.peerSnap[1].state = progressSnapshotting
	rf.peerSnap[1].index = 1100
	rf.setRole(ROLE_FOLLOWER)
	state, idx := rf.peerSnap[1].state, rf.peerSnap[1].index
	rf.mu.Unlock()
	if state != progressReplicating || idx != 0 {
		t.Errorf("卸任之后仍留着快照进度：state=%v index=%d", state, idx)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}
