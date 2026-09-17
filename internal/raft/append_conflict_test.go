package raft

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
)

// follower 冲突截断此前只有 truncateLogFrom 的单元测试覆盖（字节计量对不对），
// 真正走 AppendEntriesInRaft 的那条路一次都没测过。集群脚本也测不到：它们都是
// 写完、等提交、再杀 leader，从不留下"未提交的尾巴"。
//
// 这里补的正是那个形态：follower 收下 term 1 的一段日志、它们**没有提交**，
// 随后 term 2 的新 leader 用不同的内容覆盖同样的下标。要检查的有三件事——
// 内存日志、偏移量队列、以及盘上的日志文件（重启后能否恢复）。

// conflictFollower 造一个能真写盘的 follower。stateFile 留空，于是 persistHardState
// 是 no-op，任期跳变不需要磁盘状态文件。
func conflictFollower(t *testing.T, term int) *Raft {
	t.Helper()
	rf := &Raft{
		me: 1, peers: make([]string, 3),
		currentTerm: term, votedFor: -1, role: ROLE_FOLLOWER,
		applySignal: make(chan struct{}, 1),
	}
	rf.currentLog = filepath.Join(t.TempDir(), "RaftState.log")
	rf.logMu.Lock()
	err := rf.openLogFile(rf.currentLog)
	rf.logMu.Unlock()
	if err != nil {
		t.Fatalf("openLogFile: %v", err)
	}
	t.Cleanup(func() { rf.CloseLogFile() })
	return rf
}

// sendEntries 把 [prev+1, prev+len] 这一段发给 follower。
func sendEntries(t *testing.T, rf *Raft, term, prev, prevTerm int, kvs ...[2]string) *raftrpc.AppendEntriesInRaftResponse {
	t.Helper()
	entries := make([]*raftrpc.LogEntry, len(kvs))
	for i, kv := range kvs {
		entries[i] = &raftrpc.LogEntry{
			Term: int32(term),
			Command: &raftrpc.DetailCod{
				Index: int32(prev + 1 + i), Term: int32(term),
				OpType: "Put", Key: kv[0], Value: kv[1],
			},
		}
	}
	reply, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: int32(term), LeaderId: 0,
		PrevLogIndex: int32(prev), PrevLogTerm: int32(prevTerm),
		Entries: entries,
	})
	if err != nil {
		t.Fatalf("AppendEntriesInRaft: %v", err)
	}
	return reply
}

// recoverAfter 关掉句柄再按恢复路径回放，返回恢复出的日志。
// 这一步是判据里最关键的：截断只做对内存不算做对，盘上留下的陈旧字节要等到重启才发作。
func recoverAfter(t *testing.T, rf *Raft) *Raft {
	t.Helper()
	rf.CloseLogFile()
	fresh := &Raft{persister: &Persister{}}
	if _, err := fresh.RecoverLog([]LogFile{{Path: rf.currentLog}}, 0); err != nil {
		t.Fatalf("截断之后节点起不来：RecoverLog: %v", err)
	}
	return fresh
}

func logValues(rf *Raft) []string {
	out := make([]string, len(rf.log))
	for i, e := range rf.log {
		out[i] = e.GetCommand().GetValue()
	}
	return out
}

// 冲突点在第一条（文件偏移 0）：整段尾巴都要被换掉。
//
// 偏移 0 不是边角情形——全新节点收到的第一条就在 0，GC 换文件之后写进新文件的第一条
// 也在 0（SetCurrentLog 把 logOffset 归零）。
func TestFollowerTruncatesConflictingTailFromFirstEntry(t *testing.T) {
	rf := conflictFollower(t, 1)

	// term 1 的 leader 发来三条，LeaderCommit=0：一条都没提交
	if r := sendEntries(t, rf, 1, 0, 0,
		[2]string{"a", "v1"}, [2]string{"b", "v2"}, [2]string{"c", "v3"}); !r.Success {
		t.Fatalf("term 1 的追加被拒：ConflictIndex=%d", r.ConflictIndex)
	}
	if len(rf.log) != 3 || len(rf.Offsets) != 3 || rf.Offsets[0] != 0 {
		t.Fatalf("前置状态不对：%d 条日志、Offsets=%v", len(rf.log), rf.Offsets)
	}
	if rf.commitIndex != 0 {
		t.Fatalf("commitIndex=%d，这一段本应全部未提交", rf.commitIndex)
	}

	// term 2 的新 leader 从 index 1 起就与它分叉
	if r := sendEntries(t, rf, 2, 0, 0, [2]string{"z", "w1"}); !r.Success {
		t.Fatalf("冲突覆盖被拒：ConflictIndex=%d", r.ConflictIndex)
	}

	if got := logValues(rf); len(got) != 1 || got[0] != "w1" {
		t.Fatalf("内存日志 = %v; want [w1]——冲突的尾巴没被截掉", got)
	}
	if rf.log[0].Term != 2 {
		t.Errorf("覆盖后第一条 term = %d; want 2", rf.log[0].Term)
	}
	if len(rf.Offsets) != 1 || rf.Offsets[0] != 0 {
		t.Errorf("Offsets = %v; want [0]", rf.Offsets)
	}
	if len(rf.offsetVersions) != len(rf.Offsets) {
		t.Errorf("offsetVersions %d 条、Offsets %d 条——两者错位，apply 会取到别的文件版本",
			len(rf.offsetVersions), len(rf.Offsets))
	}
	wantSize := int64(recordHeader + len("z") + len("w1"))
	if info, err := os.Stat(rf.currentLog); err != nil {
		t.Fatal(err)
	} else if info.Size() != wantSize {
		t.Errorf("日志文件 %d 字节; want %d——被截断的三条字节还留在盘上", info.Size(), wantSize)
	}

	fresh := recoverAfter(t, rf)
	if got := logValues(fresh); len(got) != 1 || got[0] != "w1" {
		t.Fatalf("恢复出的日志 = %v; want [w1]", got)
	}
}

// 冲突点在日志中间：前面对得上的部分必须原样留下，只换后面。
func TestFollowerTruncatesConflictingTailMidLog(t *testing.T) {
	rf := conflictFollower(t, 1)
	sendEntries(t, rf, 1, 0, 0,
		[2]string{"a", "v1"}, [2]string{"b", "v2"}, [2]string{"c", "v3"},
		[2]string{"d", "v4"}, [2]string{"e", "v5"})

	// 新 leader 说 index 1、2 一样，从 index 3 起换成自己的两条
	if r := sendEntries(t, rf, 2, 2, 1, [2]string{"x", "w3"}, [2]string{"y", "w4"}); !r.Success {
		t.Fatalf("冲突覆盖被拒：ConflictIndex=%d", r.ConflictIndex)
	}

	want := []string{"v1", "v2", "w3", "w4"}
	got := logValues(rf)
	if len(got) != len(want) {
		t.Fatalf("内存日志 = %v; want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("内存日志 = %v; want %v", got, want)
		}
	}
	if len(rf.Offsets) != 4 {
		t.Errorf("Offsets = %v; want 4 条", rf.Offsets)
	}

	fresh := recoverAfter(t, rf)
	gotR := logValues(fresh)
	if len(gotR) != len(want) {
		t.Fatalf("恢复出的日志 = %v; want %v", gotR, want)
	}
	for i := range want {
		if gotR[i] != want[i] {
			t.Fatalf("恢复出的日志 = %v; want %v", gotR, want)
		}
	}
	if fresh.log[2].Term != 2 || fresh.log[1].Term != 1 {
		t.Errorf("恢复后 term 序列 = [_, %d, %d, _]; want [_, 1, 2, _]", fresh.log[1].Term, fresh.log[2].Term)
	}
}

// 新 leader 的那一段比本地的短：覆盖之后日志必须**变短**，不能留下更长的旧尾巴。
// 这是"截断"与"逐条覆盖"的分水岭——只覆盖不截断的话，index 4、5 会活下来，
// 而新 leader 的日志里根本没有它们。
func TestFollowerTailShrinksToLeaderLog(t *testing.T) {
	rf := conflictFollower(t, 1)
	sendEntries(t, rf, 1, 0, 0,
		[2]string{"a", "v1"}, [2]string{"b", "v2"}, [2]string{"c", "v3"},
		[2]string{"d", "v4"}, [2]string{"e", "v5"})

	sendEntries(t, rf, 2, 1, 1, [2]string{"x", "w2"})

	if got := logValues(rf); len(got) != 2 || got[1] != "w2" {
		t.Fatalf("内存日志 = %v; want [v1 w2]", got)
	}
	if rf.lastIndex() != 2 {
		t.Errorf("lastIndex = %d; want 2", rf.lastIndex())
	}
	fresh := recoverAfter(t, rf)
	if got := logValues(fresh); len(got) != 2 || got[1] != "w2" {
		t.Fatalf("恢复出的日志 = %v; want [v1 w2]——index 3~5 的陈旧记录被回放了", got)
	}
}

// 一次 AppendEntries 带多条时，落盘必须是**一批**而不是逐条。
//
// leader 按编码字节数打包（doAppendEntries 里 totalSize >= threshold 才截断），
// 一次常常带几十上百条；follower 原先每条都 Flush + fsync 一次——代码写着"批量存储"，
// 而那个判据（index == rf.lastIndex()）在紧邻的 appendLog 之后恒为真。
func TestFollowerWritesOneBatchPerAppend(t *testing.T) {
	rf := conflictFollower(t, 1)
	const n = 40
	kvs := make([][2]string, n)
	for i := range kvs {
		kvs[i] = [2]string{"k", "value-bytes"}
	}

	writesBefore, _, _ := LogWriteBatching()
	sendEntries(t, rf, 1, 0, 0, kvs...)
	writesAfter, _, _ := LogWriteBatching()

	if got := writesAfter - writesBefore; got != 1 {
		t.Errorf("一次 AppendEntries 带 %d 条，写了日志文件 %d 次; want 1"+
			"——每次写入都是一次 Flush + fsync", n, got)
	}
	if len(rf.log) != n {
		t.Fatalf("%d 条日志; want %d", len(rf.log), n)
	}
	if len(rf.batchLog) != 0 {
		t.Errorf("返回前 batchLog 还剩 %d 条没落盘", len(rf.batchLog))
	}
	if len(rf.Offsets) != n {
		t.Errorf("Offsets %d 条; want %d", len(rf.Offsets), n)
	}
	fresh := recoverAfter(t, rf)
	if len(fresh.log) != n {
		t.Fatalf("恢复出 %d 条; want %d", len(fresh.log), n)
	}
}

// commitIndex 只能推进到**本次 AppendEntries 确认过的前缀末尾**，不能推进到本节点
// 自己日志的末尾。
//
// 差别只在"本节点留着一段未提交的分叉尾巴"时出现，而那正是故障切换后的常态：
// 旧 leader 写下一段提交不了的条目、被杀、重启，新 leader 探到共同前缀之后往往
// 先发一次**空** AppendEntries（心跳），带着一个很高的 LeaderCommit。
// 按 rf.lastIndex() 夹，那一段分叉条目就被提交并应用进状态机——而集群从未提交它们。
// 应用不可撤销：日志随后被截断，错值留在 RocksDB 里。
func TestEmptyAppendDoesNotCommitDivergentTail(t *testing.T) {
	rf := conflictFollower(t, 1)
	// term 1 的 leader 发来 5 条并提交前 2 条
	sendEntries(t, rf, 1, 0, 0,
		[2]string{"a", "v1"}, [2]string{"b", "v2"}, [2]string{"c", "v3"},
		[2]string{"d", "v4"}, [2]string{"e", "v5"})
	if _, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 1, LeaderId: 0, PrevLogIndex: 5, PrevLogTerm: 1, LeaderCommit: 2,
	}); err != nil {
		t.Fatal(err)
	}
	if rf.commitIndex != 2 {
		t.Fatalf("前置状态：commitIndex=%d, want 2", rf.commitIndex)
	}

	// 新 leader（term 2）探到 index 2 处匹配，先发一次空心跳，LeaderCommit 很高。
	// 它只确认到 PrevLogIndex=2；本节点的 3~5 是分叉的未提交条目。
	reply, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 2, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 99,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Success {
		t.Fatalf("一致性检查该过却被拒：ConflictIndex=%d", reply.ConflictIndex)
	}
	if rf.commitIndex > 2 {
		t.Fatalf("commitIndex 被推到 %d——本节点 3~5 是集群从未提交的分叉条目，"+
			"提交它们等于把错值应用进状态机，而应用不可撤销", rf.commitIndex)
	}
}

// 带条目时的上界是 PrevLogIndex + 接受的条目数，同样不是本节点日志的末尾。
func TestAppendCommitsOnlyUpToLastNewEntry(t *testing.T) {
	rf := conflictFollower(t, 1)
	sendEntries(t, rf, 1, 0, 0,
		[2]string{"a", "v1"}, [2]string{"b", "v2"}, [2]string{"c", "v3"},
		[2]string{"d", "v4"}, [2]string{"e", "v5"})

	// 新 leader 从 index 3 起只发一条，LeaderCommit 很高。
	// 确认过的前缀到 index 3；本节点原来的 4、5 被这次截断丢掉，绝不能被提交。
	reply, err := rf.AppendEntriesInRaft(context.Background(), &raftrpc.AppendEntriesInRaftRequest{
		Term: 2, LeaderId: 2, PrevLogIndex: 2, PrevLogTerm: 1, LeaderCommit: 99,
		Entries: []*raftrpc.LogEntry{{
			Term: 2,
			Command: &raftrpc.DetailCod{
				Index: 3, Term: 2, OpType: "Put", Key: "z", Value: "w3",
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reply.Success {
		t.Fatalf("被拒：ConflictIndex=%d", reply.ConflictIndex)
	}
	if rf.commitIndex != 3 {
		t.Fatalf("commitIndex = %d; want 3（PrevLogIndex 2 + 接受 1 条）", rf.commitIndex)
	}
	if got := logValues(rf); len(got) != 3 || got[2] != "w3" {
		t.Fatalf("日志 = %v; want [v1 v2 w3]", got)
	}
}
