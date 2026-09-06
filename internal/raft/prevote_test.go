package raft

import (
	"context"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// Raft 论文 §4.2.3 的防打扰规则：最小选举超时之内确认过 leader 还在工作，就忽略拉票。
//
// 少了它，一个从卡顿里回来的节点每次拉票都能把健康的 leader 拉下台——它自己选不上
// （日志更旧），但更高的任期迫使 leader 降级，leader 超时后重新当选，它再拉一次票。
// 实测 GC 期间 7 秒内换届 4 次就是这么来的。

// voter 造一个刚与 leader 通过消息的 follower。stateFile 留空表示不落盘。
func voter(t *testing.T, sinceLeaderContact time.Duration, term int) *Raft {
	t.Helper()
	rf := &Raft{
		me:              1,
		peers:           make([]string, 3),
		currentTerm:     term,
		votedFor:        -1,
		role:            ROLE_FOLLOWER,
		heardFromLeader: true,
		LastAppendTime:  time.Now().Add(-sinceLeaderContact),
	}
	rf.log = []*raftrpc.LogEntry{{Term: int32(term)}}
	return rf
}

// 拉票来自日志更旧的节点，且本节点刚收到过 leader 的消息：必须整个忽略，
// 尤其是不能跟着把任期升上去——降级正是从这里开始的。
func TestIgnoresVoteWhileLeaderIsAlive(t *testing.T) {
	rf := voter(t, 100*time.Millisecond, 2)
	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteRequest{
		Term: 3, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.VoteGranted {
		t.Fatal("与 leader 通信正常时不该投票")
	}
	if rf.currentTerm != 2 {
		t.Fatalf("任期被拉票抬高到 %d，leader 会因此降级", rf.currentTerm)
	}
	if rf.role != ROLE_LEADER && rf.votedFor != -1 {
		t.Fatalf("votedFor 被改成了 %d", rf.votedFor)
	}
}

// leader 用自己发出 AppendEntries 的时刻判断，同样忽略。这条正是实测里失效的场景：
// node0 是 leader，被 node2 的拉票反复拉下台。
func TestLeaderIgnoresDisruptiveVote(t *testing.T) {
	rf := voter(t, 50*time.Millisecond, 4)
	rf.me = 0
	rf.role = ROLE_LEADER
	rf.heardFromLeader = false // leader 不靠这个标志，靠自己的 LastAppendTime

	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteRequest{
		Term: 5, CandidateId: 2, LastLogIndex: 0, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.VoteGranted || rf.currentTerm != 4 || rf.role != ROLE_LEADER {
		t.Fatalf("leader 被拉票打扰了：granted=%v term=%d role=%s", reply.VoteGranted, rf.currentTerm, rf.role)
	}
}

// leader 真的没了（超过最小选举超时没有消息）时，规则不能挡住正常的换届。
func TestGrantsVoteAfterLeaderGoesSilent(t *testing.T) {
	rf := voter(t, minElectionTimeout+time.Second, 2)
	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteRequest{
		Term: 3, CandidateId: 2, LastLogIndex: 1, LastLogTerm: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.VoteGranted {
		t.Fatal("leader 已经沉默超过最小选举超时，必须允许换届")
	}
	if rf.currentTerm != 3 || rf.votedFor != 2 {
		t.Fatalf("term=%d votedFor=%d", rf.currentTerm, rf.votedFor)
	}
}

// 刚启动、还没和任何 leader 打过交道的节点不适用本规则，否则首次选举会被
// 各节点的启动时差平白推迟。
func TestFreshNodeStillVotes(t *testing.T) {
	rf := voter(t, 0, 0)
	rf.heardFromLeader = false

	reply, err := rf.RequestVote(context.Background(), &raftrpc.RequestVoteRequest{
		Term: 1, CandidateId: 2, LastLogIndex: 1, LastLogTerm: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.VoteGranted {
		t.Fatal("刚启动的节点必须能参与首次选举")
	}
}
