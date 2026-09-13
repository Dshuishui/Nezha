package raft

import (
	"testing"
	"time"
)

// 租约读（Raft 论文 §6.4 的 lease read）的安全性钉在几条不变量上，这里逐条锁住。
// 它们都不需要起 gRPC、不需要 RocksDB，所以本文件在任何机器上都能跑。

// 最要紧的一条：租约必须比选举超时短。
//
// 租约的含义是"在它到期之前，别的节点不可能被选成 leader，所以我可以拿本地状态应答
// 读"。而 follower 的选举计时从它**收到**心跳算起。租约一旦长到覆盖了 follower 的选举
// 窗口，就会出现：新 leader 已经选出并接受了写，旧 leader 还以为自己持有租约，继续拿
// 旧状态应答读——租约读于是比什么都不做更糟，它把"可能读到旧值"变成了"确定读到旧值
// 且不报错"。
//
// 调大租约必须**同时**调大选举超时，这个用例就是为了在只改一边时立刻失败。
func TestLeaseIsShorterThanElectionTimeout(t *testing.T) {
	if leaderLeaseDuration >= minElectionTimeout {
		t.Fatalf("租约 %v 不短于最小选举超时 %v：旧 leader 的租约会伸进新 leader 的任期里，"+
			"两个节点会同时自认为可以应答读", leaderLeaseDuration, minElectionTimeout)
	}
	// 余量留给调度抖动和两台机器晶振频率的差异。10% 是 TiKV 的取法
	// （raft-store-max-leader-lease=9s 配 10s 的选举超时）。
	margin := minElectionTimeout - leaderLeaseDuration
	if margin < minElectionTimeout/10 {
		t.Fatalf("租约与选举超时之间只剩 %v 余量（不足 10%%）：一次 GC 造成的停顿就能让"+
			"租约撑到新 leader 上任之后", margin)
	}
}

// 心跳间隔必须远小于租约，否则租约会在两次心跳之间反复过期，每次读都退回 ReadIndex。
func TestHeartbeatRenewsLeaseWellBeforeItExpires(t *testing.T) {
	if heartbeatInterval*2 >= leaderLeaseDuration {
		t.Fatalf("心跳间隔 %v 相对租约 %v 太大：丢一两个心跳租约就凉，读会频繁退回 ReadIndex",
			heartbeatInterval, leaderLeaseDuration)
	}
}

func leaseLeader() *Raft {
	rf := &Raft{me: 0, peers: make([]string, 3), currentTerm: 5}
	rf.setRole(ROLE_LEADER)
	rf.leaseTermNoop = 1
	return rf
}

func TestHoldsLeaseOnlyAfterGrant(t *testing.T) {
	rf := leaseLeader()
	if rf.HoldsLease() {
		t.Fatal("刚当选、还没有任何心跳被确认，不该持有租约")
	}
	rf.grantLease(time.Now())
	if !rf.HoldsLease() {
		t.Fatal("多数派确认了心跳之后应当持有租约")
	}
	if left := rf.LeaseRemaining(); left <= 0 || left > leaderLeaseDuration {
		t.Fatalf("租约剩余 %v，应当在 (0, %v] 之间", left, leaderLeaseDuration)
	}
}

// 租约从**心跳发出的时刻**起算，不是从收到回执起算。这里用一个早于租约时长的发出时刻
// 模拟"回执来得太晚"：那一轮心跳已经不能再证明任何事，租约必须已经过期。
func TestLeaseMeasuredFromSendTimeNotReplyTime(t *testing.T) {
	rf := leaseLeader()
	rf.grantLease(time.Now().Add(-leaderLeaseDuration - time.Second))
	if rf.HoldsLease() {
		t.Fatal("发出时刻已超过一个租约时长的心跳，其回执不该续出有效租约")
	}
}

// 迟到的回执不能把租约往回拽：它们确认的是同一个发出时刻，或更早的一轮。
func TestLateReplyDoesNotShortenLease(t *testing.T) {
	rf := leaseLeader()
	now := time.Now()
	rf.grantLease(now)
	before := rf.leaseDeadline.Load()
	rf.grantLease(now.Add(-heartbeatInterval)) // 上一轮的迟到回执
	if rf.leaseDeadline.Load() != before {
		t.Fatalf("更早一轮的回执把租约从 %d 改成了 %d", before, rf.leaseDeadline.Load())
	}
}

// 退位必须连带撤销租约，而且这件事挂在 setRole 上——它是改身份的唯一入口，
// 漏掉任何一处退位点，都意味着一个已被罢免的节点还在拿本地状态应答读。
func TestSetRoleClearsLease(t *testing.T) {
	for _, role := range []string{ROLE_FOLLOWER, ROLE_CANDIDATES} {
		rf := leaseLeader()
		rf.grantLease(time.Now())
		if !rf.HoldsLease() {
			t.Fatal("前置条件不成立：租约没发出来")
		}
		rf.setRole(role)
		if rf.HoldsLease() {
			t.Fatalf("转为 %s 之后仍报告持有租约", role)
		}
		if rf.leaseTermNoop != 0 {
			t.Fatalf("转为 %s 之后 leaseTermNoop 仍是 %d：下次当选会在 no-op apply 之前就发租约",
				role, rf.leaseTermNoop)
		}
	}
}

// 非 leader 永远不持有租约，即使 leaseDeadline 里还留着上一任的读数。
func TestNonLeaderNeverHoldsLease(t *testing.T) {
	rf := leaseLeader()
	rf.grantLease(time.Now())
	rf.leaderRole.Store(false) // 绕过 setRole，模拟镜像与 deadline 走散的最坏情况
	if rf.HoldsLease() {
		t.Fatal("leaderRole 为假时不该报告持有租约")
	}
	if rf.LeaseRemaining() != 0 {
		t.Fatal("非 leader 的租约剩余时间应为 0")
	}
}

func TestWaitApplied(t *testing.T) {
	rf := &Raft{me: 0, peers: make([]string, 3), lastApplied: 7}
	if !rf.WaitApplied(7, 0) {
		t.Fatal("已经追上的位点应当立刻返回真，且不受 timeout=0 影响")
	}
	if rf.WaitApplied(8, 20*time.Millisecond) {
		t.Fatal("apply 没追上时不该返回真")
	}

	rf2 := &Raft{me: 0, peers: make([]string, 3), lastApplied: 0}
	go func() {
		time.Sleep(10 * time.Millisecond)
		rf2.mu.Lock()
		rf2.lastApplied = 9
		rf2.mu.Unlock()
	}()
	if !rf2.WaitApplied(9, 2*time.Second) {
		t.Fatal("apply 在限时内追上了，应当返回真")
	}
}
