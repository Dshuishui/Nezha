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

// 一致性检查对**空 entries** 一样要做。
//
// 原先 `len(entries) == 0` 被无条件当成成功心跳，PrevLogIndex/PrevLogTerm 一眼都不看。
// 后果有两层，都不报错：leader 在没有新条目可发时也走 AppendEntries，收到 Success 就把
// matchIndex 记成"已复制到这里"，于是一个丢了日志的副本被记成完全跟上、永远拿不回数据；
// 而它随即进入提交多数派，leader 挂掉之后它可以当选，却不持有那些已提交的条目——
// 已提交数据丢失，这是安全性违背。
func TestEmptyAppendEntriesStillChecksConsistency(t *testing.T) {
	ctx := context.Background()

	// 一个日志被抹空的节点：任何 PrevLogIndex > 0 都必须被拒。
	empty := &Raft{me: 1, peers: make([]string, 3), currentTerm: 5, votedFor: -1, role: ROLE_FOLLOWER}
	reply, err := empty.AppendEntriesInRaft(ctx, &raftrpc.AppendEntriesInRaftRequest{
		Term: 5, LeaderId: 0, PrevLogIndex: 60000, PrevLogTerm: 5, LeaderCommit: 60000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Success {
		t.Error("日志为空的节点接受了 PrevLogIndex=60000 的空心跳——leader 会据此把它记成已跟上")
	}
	if reply.ConflictIndex != 1 {
		t.Errorf("ConflictIndex = %d; want 1（它一条都没有，得从头发）", reply.ConflictIndex)
	}
	if empty.commitIndex != 0 {
		t.Errorf("一致性检查没过却推进了 commitIndex 到 %d", empty.commitIndex)
	}

	// 日志对得上的节点，空心跳照常成功，并且照常推进 commitIndex。
	ok := &Raft{me: 1, peers: make([]string, 3), currentTerm: 5, votedFor: -1, role: ROLE_FOLLOWER,
		log: mkLog(10, 5), applySignal: make(chan struct{}, 1)}
	reply, err = ok.AppendEntriesInRaft(ctx, &raftrpc.AppendEntriesInRaftRequest{
		Term: 5, LeaderId: 0, PrevLogIndex: 10, PrevLogTerm: 5, LeaderCommit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Success {
		t.Errorf("日志一致的节点拒了空心跳（ConflictIndex=%d）——那会让复制退化成反复回退",
			reply.ConflictIndex)
	}
	if ok.commitIndex != 10 {
		t.Errorf("commitIndex = %d; want 10", ok.commitIndex)
	}

	// term 对不上：也必须拒，并带上冲突信息。
	diff := &Raft{me: 1, peers: make([]string, 3), currentTerm: 5, votedFor: -1, role: ROLE_FOLLOWER,
		log: mkLog(10, 3)} // 本地是 term 3，leader 说该位置是 term 5
	reply, err = diff.AppendEntriesInRaft(ctx, &raftrpc.AppendEntriesInRaftRequest{
		Term: 5, LeaderId: 0, PrevLogIndex: 10, PrevLogTerm: 5, LeaderCommit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Success {
		t.Error("term 不匹配却接受了空心跳")
	}
	if reply.ConflictTerm != 3 {
		t.Errorf("ConflictTerm = %d; want 3", reply.ConflictTerm)
	}
}
