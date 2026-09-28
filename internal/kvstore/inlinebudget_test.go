package kvstore

import (
	"path/filepath"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 内联额度走完整的 apply 路径验：额度内的小值内联，用完之后改存偏移（与 nezha 相同），
// 换一代库之后额度清零、又能内联。
//
// 三件事都会**静默地**错：额度不生效，表现只是库又溢出成 SST、读变慢；
// 额度用完之后没落到偏移分支，那条写就丢了（读的时候哪里都找不到）；
// 换库不清零，额度用完一次之后 AVP 永久退化成 nezha，而日志里什么都没有。
func TestInlineBudgetSpillsToOffsetAndResetsOnNewStore(t *testing.T) {
	kvs := newApplyTestServer(t)
	kvs.kvSeparation, kvs.inlinePlacement, kvs.inlineThreshold = true, true, 512
	kvs.inlineBudgetBytes = 10 // 每条 key+value = 2+3 = 5 字节，恰好放得下两条

	seq := int64(0)
	put := func(idx int, key, val string) raft.ApplyMsg {
		seq++
		return raft.ApplyMsg{CommandValid: true, CommandIndex: idx, CommandTerm: 1,
			FileVersion: 0, Offset: int64(idx * 100),
			Command: &raftrpc.DetailCod{Index: int32(idx), Term: 1, OpType: OP_TYPE_PUT,
				Key: key, Value: val, ClientId: 1, SeqId: seq}}
	}
	kind := func(key string) raft.RecordKind {
		t.Helper()
		k, _, _, err := kvs.persister.GetRecord(key)
		if err != nil {
			t.Fatalf("取 %q 的记录失败: %v", key, err)
		}
		return k
	}

	spills := avpStats.budgetSpills.Load()
	kvs.mu.Lock()
	kvs.applyBatch([]raft.ApplyMsg{put(1, "k1", "v01"), put(2, "k2", "v02"), put(3, "k3", "v03")})
	kvs.mu.Unlock()

	if kind("k1") != raft.RecordInline || kind("k2") != raft.RecordInline {
		t.Fatalf("额度之内的两条应当内联：k1=%v k2=%v", kind("k1"), kind("k2"))
	}
	// 额度用完之后必须落到偏移分支——不是被丢掉。丢掉的写不会报错，只在读的时候查不到。
	if got := kind("k3"); got != raft.RecordOffset {
		t.Fatalf("额度用完之后 k3 应当存偏移，得到 %v", got)
	}
	if d := avpStats.budgetSpills.Load() - spills; d != 1 {
		t.Errorf("inline_budget_spills 增加了 %d，want 1——这个计数是额度在起作用的唯一证据", d)
	}

	// 换一代库（GC 切换 / 装快照 / 重启恢复都是换 kvs.persister）：额度必须清零。
	next := new(raft.Persister)
	if _, err := next.Init(filepath.Join(t.TempDir(), "db2"), true); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(next.Close)
	kvs.mu.Lock()
	kvs.persister = next
	kvs.applyBatch([]raft.ApplyMsg{put(4, "k4", "v04")})
	kvs.mu.Unlock()
	if got := kind("k4"); got != raft.RecordInline {
		t.Fatalf("换库之后额度没清零：k4 = %v，want 内联——额度用完一次之后 AVP 就永久退化成 nezha", got)
	}
}

// 额度为 0 是历史行为：不限。负数在构造时已钳到 0（server.go），这里只钉 0。
func TestInlineBudgetZeroMeansUnlimited(t *testing.T) {
	kvs := &KVServer{persister: new(raft.Persister)}
	for i := 0; i < 1000; i++ {
		if !kvs.takeInlineBudget(1 << 20) {
			t.Fatalf("额度为 0 时第 %d 次被拒——0 应当表示不限", i+1)
		}
	}
}
