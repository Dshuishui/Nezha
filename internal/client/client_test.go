package client

import (
	"context"
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
