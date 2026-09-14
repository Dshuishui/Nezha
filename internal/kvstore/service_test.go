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
