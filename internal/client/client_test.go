package client

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
)

// fakeNode answers like a node: only the leader accepts, the others redirect to it.
type fakeNode struct {
	kvrpc.UnimplementedKVServer
	id, leader int32
	mu         sync.Mutex
	rows       map[string]string
	puts       int
}

func (n *fakeNode) PutInRaft(_ context.Context, r *kvrpc.PutInRaftRequest) (*kvrpc.PutInRaftResponse, error) {
	if n.id != n.leader {
		return &kvrpc.PutInRaftResponse{Err: kvrpc.ErrWrongLeader, LeaderId: n.leader}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rows[r.Key] = r.Value
	n.puts++
	return &kvrpc.PutInRaftResponse{Err: kvrpc.OK}, nil
}

func (n *fakeNode) GetInRaft(_ context.Context, r *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	if n.id != n.leader {
		return &kvrpc.GetInRaftResponse{Err: kvrpc.ErrWrongLeader, LeaderId: n.leader}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if v, ok := n.rows[r.Key]; ok {
		return &kvrpc.GetInRaftResponse{Err: kvrpc.OK, Value: v}, nil
	}
	return &kvrpc.GetInRaftResponse{Err: kvrpc.ErrNoKey, Value: kvrpc.NoKey}, nil
}

func (n *fakeNode) ScanRangeInRaft(_ context.Context, r *kvrpc.ScanRangeRequest) (*kvrpc.ScanRangeResponse, error) {
	if n.id != n.leader {
		return &kvrpc.ScanRangeResponse{Err: kvrpc.ErrWrongLeader, LeaderId: n.leader}, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	out := map[string]string{}
	for k, v := range n.rows {
		if k >= r.StartKey && k <= r.EndKey {
			out[k] = v
		}
	}
	return &kvrpc.ScanRangeResponse{Err: kvrpc.OK, KeyValuePairs: out}, nil
}

// cluster starts n fake nodes on loopback with node leader as the leader.
func cluster(t *testing.T, n int, leader int32) ([]string, []*fakeNode) {
	t.Helper()
	var addrs []string
	var nodes []*fakeNode
	for i := 0; i < n; i++ {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		node := &fakeNode{id: int32(i), leader: leader, rows: map[string]string{}}
		srv := grpc.NewServer()
		kvrpc.RegisterKVServer(srv, node)
		go func() { _ = srv.Serve(lis) }()
		t.Cleanup(srv.Stop)
		addrs = append(addrs, lis.Addr().String())
		nodes = append(nodes, node)
	}
	return addrs, nodes
}

func TestFollowsRedirectOnce(t *testing.T) {
	addrs, nodes := cluster(t, 3, 2)
	c := MustNew(addrs, Options{PoolMaxIdle: 2, PoolMaxActive: 4, PoolMaxConcurrentStreams: 8})
	defer c.Close()
	if reply, err := c.Put("k", "v"); err != nil || reply.Err != kvrpc.OK {
		t.Fatalf("Put: %v %v", reply, err)
	}
	if c.Leader() != 2 {
		t.Fatalf("leader after redirect = %d, want 2", c.Leader())
	}
	if _, err := c.Put("k2", "v2"); err != nil {
		t.Fatal(err)
	}
	if nodes[2].puts != 2 || nodes[0].puts != 0 {
		t.Fatalf("puts landed on %d/%d, want all on the leader", nodes[0].puts, nodes[2].puts)
	}
	v, found, err := c.Get("k")
	if err != nil || !found || v != "v" {
		t.Fatalf("Get k = %q found=%v err=%v", v, found, err)
	}
	if _, found, err := c.Get("missing"); err != nil || found {
		t.Fatalf("Get missing: found=%v err=%v", found, err)
	}
	reply, err := c.Scan("k", "k2")
	if err != nil || reply.Err != kvrpc.OK || len(reply.KeyValuePairs) != 2 {
		t.Fatalf("Scan: %v %v", reply, err)
	}
}

func TestGetFromDoesNotRedirect(t *testing.T) {
	addrs, _ := cluster(t, 2, 1)
	c := MustNew(addrs, Options{PoolMaxIdle: 2, PoolMaxActive: 4, PoolMaxConcurrentStreams: 8})
	defer c.Close()
	reply, err := c.GetFrom(0, "k")
	if err != nil || reply.Err != kvrpc.ErrWrongLeader || reply.LeaderId != 1 {
		t.Fatalf("GetFrom follower: %v %v", reply, err)
	}
	if c.Leader() != 0 {
		t.Fatalf("GetFrom must not move the leader, got %d", c.Leader())
	}
}

func TestSequenceIdsAreUnique(t *testing.T) {
	c := &Client{}
	seen := map[int64]bool{}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				s := c.NextSeq()
				mu.Lock()
				if seen[s] {
					t.Errorf("sequence id %d handed out twice", s)
				}
				seen[s] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

// key 的定长十进制编码。存储层现在原样存 key、按字节序排，所以"整数 key 的范围查询
// 要有数值语义"这件事由客户端负责。这一组钉住它是**单射**的——那正是把同一个编码放在
// 存储层时不成立的性质（存储层面对的是任意字符串，"7" 与 "007" 会撞成一个）。
func TestEncodeKeyPadsToWidth(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	for in, want := range map[string]string{
		"0":          "0000000000",
		"7":          "0000000007",
		"42":         "0000000042",
		"1234567890": "1234567890", // 恰好等宽，原样
	} {
		got, err := c.encodeKey(in)
		if err != nil {
			t.Errorf("encodeKey(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("encodeKey(%q) = %q，期望 %q", in, got, want)
		}
		if back := c.decodeKey(got); back != in {
			t.Errorf("往返失败 %q -> %q -> %q", in, got, back)
		}
	}
}

// 绝不截断。超长 key 原样发出去，由存储层完整保存——截断是被修掉的那个缺陷。
func TestEncodeKeyNeverTruncates(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	long := "user18446744073709551615" // 最长的一种 YCSB key，24 字符
	got, err := c.encodeKey(long)
	if err != nil {
		t.Fatalf("encodeKey: %v", err)
	}
	if got != long {
		t.Fatalf("encodeKey(%q) = %q，超长 key 必须原样通过", long, got)
	}
	if back := c.decodeKey(got); back != long {
		t.Fatalf("decodeKey 改动了没被补齐的 key: %q -> %q", got, back)
	}
}

// 带前导零的 key 无法与另一个 key 区分开，必须**报错**而不是悄悄撞上去。
func TestEncodeKeyRejectsLeadingZero(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	for _, k := range []string{"007", "01", "0000000007"} {
		if _, err := c.encodeKey(k); !errors.Is(err, ErrLeadingZero) {
			t.Errorf("encodeKey(%q) 应当报 ErrLeadingZero，实际 %v", k, err)
		}
	}
	// "0" 是零的规范写法，不是前导零
	if _, err := c.encodeKey("0"); err != nil {
		t.Errorf("encodeKey(\"0\") 被拒绝了: %v", err)
	}
}

// KeyPadNone：原样收发，任何 key 都不动。YCSB 这类本身就定长的 key 用这个。
func TestKeyPadNonePassesEverythingThrough(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: KeyPadNone}}
	for _, k := range []string{"0", "007", "7", "user18446744073709551615", ""} {
		got, err := c.encodeKey(k)
		if err != nil {
			t.Errorf("encodeKey(%q): %v", k, err)
			continue
		}
		if got != k || c.decodeKey(got) != k {
			t.Errorf("KeyPadNone 下 %q 被改动成了 %q/%q", k, got, c.decodeKey(got))
		}
	}
}

// 默认宽度必须与 benchmark 的键空间一致：key 是 strconv.Itoa(i)，十位刚好覆盖到 10^10。
func TestDefaultsSelectKeyPadWidth(t *testing.T) {
	var o Options
	o.defaults()
	if o.KeyPadWidth != DefaultKeyPadWidth {
		t.Fatalf("默认 KeyPadWidth = %d，期望 %d（现有 benchmark 工具一行都没改，全靠这个默认值）",
			o.KeyPadWidth, DefaultKeyPadWidth)
	}
}

// 非数字的短 key 必须原样发出去，不能补零。
//
// 左补零会打乱它们的字节序："user1" 与 "user12" 补到十位是 "00000user1" 与
// "0000user12"，第 5 个字节上 '0' < 'u'，于是 "user12" 排到了 "user1" 前面。
// 存储层按字节序排、分区按字节区间路由、SCAN 建立在这个序上——补零把一个
// 本来正确的序改成了错的。
func TestEncodeKeyDoesNotPadNonDecimalKeys(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	for _, k := range []string{"user1", "user12", "a", "k-1", "1a", "a1", "1.5", "-7"} {
		got, err := c.encodeKey(k)
		if err != nil {
			t.Errorf("encodeKey(%q): %v", k, err)
			continue
		}
		if got != k {
			t.Errorf("encodeKey(%q) = %q，非数字 key 必须原样通过（补零会打乱字节序）", k, got)
		}
		if back := c.decodeKey(got); back != k {
			t.Errorf("往返失败 %q -> %q -> %q", k, got, back)
		}
	}

	// 补零之后的序必须与补零之前一致——这正是"只对十进制补零"要保住的性质。
	a, _ := c.encodeKey("user1")
	b, _ := c.encodeKey("user12")
	if !(a < b) {
		t.Errorf("encodeKey 之后 %q 不再小于 %q——序被补零改掉了", a, b)
	}
}

// 数字 key 在**同一位宽内**，补零之后的字节序必须等于数值序。
// 这是 SCAN 区间判定赖以成立的性质。
func TestEncodeKeyPreservesNumericOrderWithinWidth(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	prev := ""
	for _, n := range []string{"0", "1", "2", "9", "10", "99", "100", "999999999", "1234567890"} {
		got, err := c.encodeKey(n)
		if err != nil {
			t.Fatalf("encodeKey(%q): %v", n, err)
		}
		if prev != "" && !(prev < got) {
			t.Errorf("%q 编码为 %q，不大于前一个 %q——字节序与数值序不一致", n, got, prev)
		}
		prev = got
	}
}

// **已知限制**，钉在这里而不是只写在注释里：超过位宽的十进制 key 原样通过，
// 它的字节序不再等于数值序。不截断是对的（截断会让不同的 key 撞成一个），
// 但要换来正确的序就得换一种编码。
// 这个用例存在的意义是：哪天把键空间推过 10^10，它会**立刻**提醒你 SCAN
// 的区间判定不能再假定"字节序 == 数值序"。改掉编码时连它一起改。
func TestKeysWiderThanPadWidthLoseNumericOrder(t *testing.T) {
	c := &Client{opts: Options{KeyPadWidth: 10}}
	small, _ := c.encodeKey("9999999999")  // 10 位
	large, _ := c.encodeKey("10000000000") // 11 位，数值更大
	if large < small {
		return // 这就是当前的已知行为
	}
	t.Fatalf("超宽 key 的字节序变了：%q 不再小于 %q——若是有意改的编码，"+
		"请同时更新 encodeKey 的「已知限制」说明与依赖字节序的 SCAN 判定", large, small)
}
