package kvstore

import (
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// newApplyTestServer 造一个能走 apply 路径的最小 KVServer：真库、无 Raft
// （这条路径碰不到它）、不做 KV 分离，于是 value 直接进库、可以用 Get 读回来核对。
func newApplyTestServer(t *testing.T) *KVServer {
	t.Helper()
	kvs := &KVServer{persister: new(raft.Persister),
		reqMap: map[int]*OpContext{}, seqMap: map[int64]int64{}}
	if _, err := kvs.persister.Init(filepath.Join(t.TempDir(), "db"), true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kvs.persister.Close)
	return kvs
}

func termLogMsg(index int) raft.ApplyMsg {
	return raft.ApplyMsg{CommandValid: true, CommandIndex: index, CommandTerm: 1,
		Command: &raftrpc.DetailCod{Index: int32(index), Term: 1, OpType: "TermLog"}}
}

// **批量与逐条必须等价。** 这是整个改动的正确性前提：applyCommand 现在就是
// applyBatch 的单条特例，而 applyLoop 走批量路径。两条路径若有分歧，表现是
// "高并发下偶尔读到旧值或少一条"，而没有任何报错——正是这个仓库反复踩的形态。
//
// 用例里刻意放了三种会让批量出错的东西：
//   - 同一个 key 在一批里被写两次（顺序必须保住，后者胜）
//   - 中间夹一条 TermLog 空指令（它不写行，只推进 applied）
//   - 批的最后一条是普通 Put（applied 必须停在它上面）
func TestApplyBatchEqualsOneByOne(t *testing.T) {
	msgs := []raft.ApplyMsg{
		putMsg(1, "a", "a1"),
		putMsg(2, "b", "b1"),
		putMsg(3, "a", "a2"), // 覆盖同批里更早的那条
		termLogMsg(4),
		putMsg(5, "c", "c1"),
	}

	one := newApplyTestServer(t)
	one.mu.Lock()
	for i := range msgs {
		one.applyCommand(msgs[i]) // 单条路径（lsmReplayHeld 走的就是它）
	}
	one.mu.Unlock()

	batched := newApplyTestServer(t)
	batched.mu.Lock()
	batched.applyBatch(msgs) // 一整批
	batched.mu.Unlock()

	for _, k := range []string{"a", "b", "c"} {
		v1, e1 := one.persister.Get(k)
		v2, e2 := batched.persister.Get(k)
		if e1 != nil || e2 != nil {
			t.Fatalf("key %q 读失败: 逐条=%v 批量=%v", k, e1, e2)
		}
		if v1 != v2 {
			t.Errorf("key %q: 逐条得到 %q，批量得到 %q —— 两条路径不等价", k, v1, v2)
		}
	}
	if v, _ := batched.persister.Get("a"); v != "a2" {
		t.Errorf("批内覆盖失效：a = %q，want %q", v, "a2")
	}

	a1, _, _ := one.persister.GetApplied()
	a2, _, _ := batched.persister.GetApplied()
	if a1 != a2 || a2 != 5 {
		t.Errorf("applied: 逐条=%d 批量=%d，want 5", a1, a2)
	}
	if one.lastAppliedIndex != batched.lastAppliedIndex || batched.lastAppliedIndex != 5 {
		t.Errorf("lastAppliedIndex: 逐条=%d 批量=%d，want 5",
			one.lastAppliedIndex, batched.lastAppliedIndex)
	}
}

// 等在这一批上的客户端**必须在落库之后**才被唤醒。
//
// 客户端拿到 OK 之后的读走 leader 本地的库（租约读），先唤醒再落库会让它
// 读不到自己刚写的值。这里的判据是：通道关掉的时候，库里已经有值。
// 由于 applyBatch 是同步的，这条用例实际验的是"落库和唤醒都发生了、且数据对"；
// 两者的**先后**由自审第 34 节按行序钉住（运行时测先后会是不确定的）。
func TestApplyBatchWakesEveryWaiterWithDataInStore(t *testing.T) {
	kvs := newApplyTestServer(t)
	msgs := []raft.ApplyMsg{putMsg(1, "a", "a1"), putMsg(2, "b", "b1"), putMsg(3, "c", "c1")}
	ctxs := make([]*OpContext, len(msgs))
	for i, m := range msgs {
		op := m.Command.(*raftrpc.DetailCod)
		ctxs[i] = newOpContext(op)
		kvs.reqMap[m.CommandIndex] = ctxs[i]
	}

	kvs.mu.Lock()
	kvs.applyBatch(msgs)
	kvs.mu.Unlock()

	for i, c := range ctxs {
		select {
		case <-c.committed:
		default:
			t.Fatalf("第 %d 条的等待者没被唤醒——它的客户端会一直挂到超时", i+1)
		}
		if c.wrongLeader {
			t.Errorf("第 %d 条被判成 wrongLeader，而 term 是一致的", i+1)
		}
	}
	for _, kv := range [][2]string{{"a", "a1"}, {"b", "b1"}, {"c", "c1"}} {
		if v, err := kvs.persister.Get(kv[0]); err != nil || v != kv[1] {
			t.Errorf("唤醒了但库里没有：%s = %q (err=%v)，want %q", kv[0], v, err, kv[1])
		}
	}
}

// 一批只落一次库。批量的收益全在这里，所以要钉住它真的发生了——
// 攒行的逻辑写错（比如每条都 flush 一次）不会有任何报错，只是收益消失。
func TestApplyBatchWritesStoreOnce(t *testing.T) {
	kvs := newApplyTestServer(t)
	msgs := make([]raft.ApplyMsg, 0, 32)
	for i := 1; i <= 32; i++ {
		msgs = append(msgs, putMsg(i, string(rune('a'+i%26))+"k", "v"))
	}
	before := storeWriteCalls()
	kvs.mu.Lock()
	kvs.applyBatch(msgs)
	kvs.mu.Unlock()
	if got := storeWriteCalls() - before; got != 1 {
		t.Errorf("32 条应用落了 %d 次库，want 1 —— 攒行没生效，批量的收益就没了", got)
	}
}
