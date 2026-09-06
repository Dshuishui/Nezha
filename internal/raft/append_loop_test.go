package raft

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// appendEntriesLoop 维持心跳的方式是一条自续的链：一轮 doAppendEntries 的回复协程把
// 回执写进 SyncChans[peer]，循环收到后发起下一轮。下面三个测试钉住的是这条链在异常
// 之后必须能自己接上——它断了就等于 leader 不再发心跳，follower 每 3 秒选举一次，
// 而选出来的还是同一个发不出心跳的节点，集群永久卡在换届里。
//
// 原实现用一个进程级的 First 标志点火一次。term 在 RPC 飞行途中变化时，回复协程
// 写回的是 "NotLeader"，循环见到就退出本轮，链熄火；等这个节点再次当选，First 早已
// 用掉，链不会重新点火。实测中 node0 因此连任 108 次而没有任何复制。

// loopHarness 起一个只跑 appendEntriesLoop 的 Raft：复制调用被换成计数器，
// 因此不需要 gRPC、磁盘或对端。
type loopHarness struct {
	rf    *Raft
	sends []atomic.Int32
	stop  func()
}

// budget<=0 表示用默认的看门狗时限。
func newLoopHarness(t *testing.T, peers int, budget time.Duration) *loopHarness {
	t.Helper()
	h := &loopHarness{
		rf:    &Raft{me: 0, peers: make([]string, peers)},
		sends: make([]atomic.Int32, peers),
	}
	for i := 0; i < peers; i++ {
		h.rf.SyncChans = append(h.rf.SyncChans, make(chan string, 1000))
	}
	h.rf.replicaWake = make(chan struct{}, 1)
	// lastIndex()>0 是循环开工的前提（lastIncludedIndex+len(log)），给一条日志占位。
	h.rf.log = []*raftrpc.LogEntry{{Term: 1}}
	h.rf.sendAppend = func(peerId int) { h.sends[peerId].Add(1) }
	h.rf.appendRoundBudget = budget

	var once sync.Once
	done := make(chan struct{})
	h.stop = func() { once.Do(func() { close(done) }) }
	go func() {
		<-done
		h.rf.Kill()
	}()
	t.Cleanup(func() { h.stop(); time.Sleep(20 * time.Millisecond) })
	go h.rf.appendEntriesLoop()
	return h
}

func (h *loopHarness) becomeLeader() {
	h.rf.mu.Lock()
	h.rf.role = ROLE_LEADER
	h.rf.mu.Unlock()
}

func (h *loopHarness) stepDown() {
	h.rf.mu.Lock()
	h.rf.role = ROLE_FOLLOWER
	h.rf.mu.Unlock()
}

// waitSends 等 peer 的发送计数超过 want，超时即失败。
func (h *loopHarness) waitSends(t *testing.T, peer int, want int32, what string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.sends[peer].Load() > want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s: peer %d 的发送计数停在 %d，没有超过 %d", what, peer, h.sends[peer].Load(), want)
}

// 回执是 "NotLeader"（term 在 RPC 途中变了）时，只要本节点仍是 leader，
// 下一轮就必须照发。原实现在这里退出，链熄火。
func TestChainSurvivesNotLeaderReply(t *testing.T) {
	h := newLoopHarness(t, 3, 0)
	h.becomeLeader()
	h.waitSends(t, 1, 0, "首轮点火")

	before := h.sends[1].Load()
	h.rf.armSync(1, "NotLeader")
	h.waitSends(t, 1, before, "收到 NotLeader 之后")
}

// 卸任再当选必须重新点火每个 peer。原实现的 First 是一次性的，
// 第二个任期一条也发不出去。
func TestChainRestartsAfterReelection(t *testing.T) {
	h := newLoopHarness(t, 3, 0)
	h.becomeLeader()
	h.waitSends(t, 1, 0, "第一个任期")
	h.waitSends(t, 2, 0, "第一个任期")

	// 卸任，并模拟"回执迟到"：这些值属于上个任期，不能被下个任期当作已回执。
	h.stepDown()
	h.rf.armSync(1, "NotLeader")
	h.rf.armSync(2, "2")
	time.Sleep(50 * time.Millisecond)

	b1, b2 := h.sends[1].Load(), h.sends[2].Load()
	h.becomeLeader()
	h.waitSends(t, 1, b1, "重新当选后")
	h.waitSends(t, 2, b2, "重新当选后")
}

// 某条路径漏写回执时，看门狗必须在 appendRoundBudget 之后补发——链熄火的代价是
// 集群不再有 leader，不能指望每条返回路径都记得写回执。
func TestWatchdogResendsWhenNoReceipt(t *testing.T) {
	h := newLoopHarness(t, 3, 300*time.Millisecond)
	h.becomeLeader()
	h.waitSends(t, 1, 0, "首轮点火")

	// 一直不写回执：预算内不该重发，超过预算必须重发。
	got := h.sends[1].Load()
	time.Sleep(150 * time.Millisecond)
	if n := h.sends[1].Load(); n != got {
		t.Fatalf("预算未到就重发了：%d -> %d", got, n)
	}
	h.waitSends(t, 1, got, "超过预算之后")
}
