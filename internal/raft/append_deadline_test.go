package raft

import (
	"context"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// 选举超时衡量的是"多久没收到 leader 的消息"，不是"这次处理花了多久"。
// AppendEntries 的处理全程持有 rf.mu 并写盘，节点自身 GC 时磁盘被打满，一次调用
// 可能耗上几秒；只在进门时刷新 lastActiveTime 的话，electionLoop 拿到锁算出的
// elapses 就是这次的处理耗时，节点会把自己判成收不到心跳。所以离开时要再刷一次。
//
// 这里钉住的是刷新的门槛（谁有资格重置选举计时）；耗时本身由三节点跑里的
// [SLOW-APPEND] 探针观察。

func appendee(term int) *Raft {
	rf := &Raft{me: 1, peers: make([]string, 3), currentTerm: term, votedFor: -1, role: ROLE_FOLLOWER}
	rf.log = []*raftrpc.LogEntry{{Term: int32(term)}}
	rf.lastActiveTime = time.Now().Add(-10 * time.Second)
	rf.LastAppendTime = time.Now().Add(-10 * time.Second)
	return rf
}

func TestHeartbeatResetsElectionTimer(t *testing.T) {
	rf := appendee(5)
	if _, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 5, LeaderId: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if since := time.Since(rf.lastActiveTime); since > time.Second {
		t.Fatalf("当前任期 leader 的消息没有重置选举计时：lastActiveTime 在 %v 前", since)
	}
	if !rf.heardFromLeader {
		t.Fatal("heardFromLeader 应为真（§4.2.3 的判据）")
	}
}

// 任期更旧的请求不是当前 leader 发的，不能重置本节点的选举计时——否则一个掉队的
// 旧 leader 反复重试就能让整个集群永远选不出新 leader。
func TestStaleTermDoesNotResetElectionTimer(t *testing.T) {
	rf := appendee(5)
	before := rf.lastActiveTime
	if _, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 4, LeaderId: 0,
	}); err != nil {
		t.Fatal(err)
	}
	if !rf.lastActiveTime.Equal(before) {
		t.Fatalf("旧任期的请求重置了选举计时：%v -> %v", before, rf.lastActiveTime)
	}
}
