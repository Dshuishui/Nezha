package kvstore

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// key 的契约走到服务端 API 这一层还成立吗。
//
// persister_test.go 钉的是存储层，client_test.go 钉的是客户端编码，这里钉的是**两者之间
// 那一段**：请求进了 KVServer 之后没有人再改写 key。此前 StartGet 一路上会对每个待比较
// 的 key 调一次 PadKey（read.go 里十几处），所以"存储层不补齐"这件事必须在这一层一起
// 成立，否则写进去的是原样、查的是补齐后的，全部读不到。
//
// 用 kvSeparation=false（baseline 路径）：value 就在存储引擎里，不需要 Raft、不需要
// valuelog，这个用例要验的只是 key 怎么流过服务端。
func newKeyTestKV(t *testing.T) *KVServer {
	t.Helper()
	kvs := &KVServer{persister: new(raft.Persister), reqMap: map[int]*OpContext{}, seqMap: map[int64]int64{}}
	if _, err := kvs.persister.Init(filepath.Join(t.TempDir(), "db"), true); err != nil {
		t.Skipf("RocksDB 不可用，跳过: %v", err)
	}
	t.Cleanup(kvs.persister.Close)
	return kvs
}

func TestServerKeepsAwkwardKeysDistinct(t *testing.T) {
	kvs := newKeyTestKV(t)

	// 三类此前会被静默改写的 key：
	//   前导零   —— PadKey 后与 "7" 同一个串，后写的覆盖先写的
	//   超长     —— 被截断到 KeyLength，前缀相同的互相覆盖
	//   非数字   —— 补零本来就只对整数成立
	cases := map[string]string{
		"7":          "seven",
		"007":        "zero-zero-seven",
		"0000000007": "ten-wide",
		fmt.Sprintf("user%d", uint64(math.MaxUint64)): "longest-ycsb",
		"user" + strings.Repeat("9", 30) + "_a":       "long-a",
		"user" + strings.Repeat("9", 30) + "_b":       "long-b",
	}
	for k, v := range cases {
		kvs.persister.Put(k, v)
	}
	for k, want := range cases {
		reply := kvs.StartGet(&kvrpc.GetInRaftRequest{Key: k})
		if reply.Err != raft.OK {
			t.Errorf("key %q: Err=%s，期望读得到", k, reply.Err)
			continue
		}
		if reply.Value != want {
			t.Errorf("key %q = %q，期望 %q —— 服务端某处又在改写 key 了", k, reply.Value, want)
		}
	}
}

// NUL 开头的 key 必须在进入 Raft **之前**被拒绝，而且是明确报错。
// 存储层用首字节 0x00 区分恢复元数据，让这种 key 写进去会覆盖 applied index。
func TestPutRejectsReservedKey(t *testing.T) {
	kvs := newKeyTestKV(t)
	reply, err := kvs.PutInRaft(context.Background(), &kvrpc.PutInRaftRequest{Key: "\x00applied_index", Value: "x"})
	if err != nil {
		t.Fatalf("PutInRaft 返回了传输层错误: %v", err)
	}
	if reply.Err != raft.ErrInvalidKey {
		t.Fatalf("Err=%q，期望 %q——保留前缀必须被显式拒绝，不能静默写下去", reply.Err, raft.ErrInvalidKey)
	}
}

func TestPutAcceptsOrdinaryKeys(t *testing.T) {
	for _, k := range []string{"0", "007", "user1", strings.Repeat("x", 300)} {
		if err := raft.ValidateKey(k); err != nil {
			t.Errorf("普通 key %q 被拒绝: %v", k, err)
		}
	}
}

// op 类型必须在进 Raft 之前就被校验，理由与校验 key 相同：一个应用不了的条目不该先被
// 复制到多数派、再在 apply 时出问题。而这里的 op 是客户端**直接给**的
// （StartPut 里 `OpType: args.Op`），此前一个字都没检查。
//
// 后果分两档，都不报错：
//
//	轻——applyOne 只有 Put 和 TermLog 两个分支，别的 op 掉进 else，value 一个字节都不会
//	    写进任何副本，而客户端拿到 NOKEY，看起来像"这个 key 不存在"。
//	重——那个 else 分支里调 Get_opt，它对 TagInline 记录返回 ErrInlineValue 并且 panic。
//	    条目已复制给所有副本，于是每个节点在 apply 同一条时一起崩：一个客户端请求打掉整个集群。
func TestPutRejectsUnknownOpType(t *testing.T) {
	kvs := newKeyTestKV(t)
	ctx := context.Background()

	for _, op := range []string{"", "Get", "get", "PUT", "Delete", "TermLog", "随便"} {
		reply, err := kvs.PutInRaft(ctx, &kvrpc.PutInRaftRequest{
			Key: "k", Value: "v", Op: op, ClientId: 1, SeqId: 1,
		})
		if err != nil {
			t.Fatalf("op=%q: %v", op, err)
		}
		if reply.GetErr() == raft.OK {
			t.Errorf("op=%q 被接受了——它会被复制出去，然后在 apply 时掉进 else 分支", op)
		}
	}

	// 两个方向都要测。合法路径不能走 PutInRaft——它会一直走到 raft.Start，
	// 而这个夹具没有 Raft 层（nil 指针）。所以直接测判据本身：
	// 只测"非法的被拒"是灵敏而不特异，那样一个恒为假的判据也能通过。
	if !validPutOp(OP_TYPE_PUT) {
		t.Errorf("validPutOp(%q) = false——校验把唯一实现的那种也挡住了", OP_TYPE_PUT)
	}
	for _, op := range []string{"", "Get", "PUT", "Delete", "TermLog"} {
		if validPutOp(op) {
			t.Errorf("validPutOp(%q) = true", op)
		}
	}
}
