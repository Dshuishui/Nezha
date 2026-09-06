package raft

import (
	"context"
	"testing"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// 心跳必须与日志复制解耦。复制链是"一轮回执触发下一轮"，同一个 peer 同时只有一轮
// 在飞，心跳间隔因此等于一次 AppendEntries 的往返；三节点同时 GC 时实测能到 5~6 秒，
// 超过 3 秒的选举超时。日志复制慢可以接受，"还有没有 leader"不该由磁盘快慢来回答。

// 间隔必须给 follower 留出余量：一次心跳丢了还得有下一次，才不至于误判。
func TestHeartbeatIntervalLeavesRoomForRetries(t *testing.T) {
	if heartbeatInterval*4 > minElectionTimeout {
		t.Fatalf("心跳间隔 %v 相对选举超时 %v 太大：丢一两次心跳就会误判失联",
			heartbeatInterval, minElectionTimeout)
	}
}

// 心跳处理不写盘、不带日志，收到就重置选举计时——这正是它能在磁盘被打满时
// 依然管用的原因。
func TestHeartbeatResetsTimerWithoutTouchingLog(t *testing.T) {
	rf := appendee(5)
	before := len(rf.log)

	reply, err := rf.HeartbeatInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 5, LeaderId: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Success {
		t.Fatal("当前任期的心跳应当成功")
	}
	if since := time.Since(rf.lastActiveTime); since > time.Second {
		t.Fatalf("心跳没有重置选举计时：lastActiveTime 在 %v 前", since)
	}
	if len(rf.log) != before {
		t.Fatalf("心跳动了日志：%d -> %d", before, len(rf.log))
	}
	if rf.leaderId != 0 {
		t.Fatalf("心跳应当认下 leader，leaderId=%d", rf.leaderId)
	}
}

// 任期更旧的心跳不能重置计时，否则掉队的旧 leader 反复重试就能拖住换届。
func TestStaleHeartbeatDoesNotResetTimer(t *testing.T) {
	rf := appendee(5)
	before := rf.lastActiveTime

	reply, err := rf.HeartbeatInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 4, LeaderId: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Success {
		t.Fatal("旧任期的心跳不该成功")
	}
	if !rf.lastActiveTime.Equal(before) {
		t.Fatalf("旧任期的心跳重置了选举计时：%v -> %v", before, rf.lastActiveTime)
	}
}
