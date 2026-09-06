// Package client is the Go client of a Nezha cluster used by every benchmark and
// verification tool: one connection pool per server, a leader that follows
// ErrWrongLeader redirects, and per-client monotonically increasing sequence ids.
package client

import (
	"context"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/pool"
)

// Options tune a Client. Zero values mean the defaults noted on each field.
type Options struct {
	Leader int // index in servers to contact first (default 0)

	// Connection pool per server; the defaults match the benchmark tools' historical
	// settings (150 idle, 300 active, 800 streams per connection).
	PoolMaxIdle              int
	PoolMaxActive            int
	PoolMaxConcurrentStreams int

	PutTimeout  time.Duration // default 120 s: a Put waits for the apply callback
	GetTimeout  time.Duration // default 60 s
	ScanTimeout time.Duration // default 600 s: full-range scans can be large
}

func (o *Options) defaults() {
	if o.PoolMaxIdle == 0 {
		o.PoolMaxIdle = 150
	}
	if o.PoolMaxActive == 0 {
		o.PoolMaxActive = 300
	}
	if o.PoolMaxConcurrentStreams == 0 {
		o.PoolMaxConcurrentStreams = 800
	}
	if o.PutTimeout == 0 {
		o.PutTimeout = 120 * time.Second
	}
	if o.GetTimeout == 0 {
		o.GetTimeout = 60 * time.Second
	}
	if o.ScanTimeout == 0 {
		o.ScanTimeout = 600 * time.Second
	}
}

// Client is safe for concurrent use.
type Client struct {
	servers  []string
	pools    []pool.Pool
	leader   atomic.Int32
	clientID int64
	seq      atomic.Int64
	opts     Options
}

// New builds the pools for servers. Peers need not be up yet (see pool.Dial).
func New(servers []string, opts Options) (*Client, error) {
	if len(servers) == 0 {
		return nil, errors.New("client: no servers")
	}
	opts.defaults()
	if opts.Leader < 0 || opts.Leader >= len(servers) {
		return nil, fmt.Errorf("client: leader index %d out of range for %d servers", opts.Leader, len(servers))
	}
	c := &Client{servers: servers, opts: opts, clientID: randomID()}
	c.leader.Store(int32(opts.Leader))
	po := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              opts.PoolMaxIdle,
		MaxActive:            opts.PoolMaxActive,
		MaxConcurrentStreams: opts.PoolMaxConcurrentStreams,
		Reuse:                true,
	}
	for _, s := range servers {
		p, err := pool.New([]string{s}, po)
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("client: pool for %s: %w", s, err)
		}
		c.pools = append(c.pools, p)
	}
	return c, nil
}

// MustNew is New for tools that cannot continue without a cluster.
func MustNew(servers []string, opts Options) *Client {
	c, err := New(servers, opts)
	if err != nil {
		panic(err)
	}
	return c
}

// Close releases the connection pools.
func (c *Client) Close() {
	for _, p := range c.pools {
		if p != nil {
			p.Close()
		}
	}
}

// Servers returns the addresses the client was built with.
func (c *Client) Servers() []string { return c.servers }

// Leader returns the index of the server currently believed to be the leader.
func (c *Client) Leader() int { return int(c.leader.Load()) }

// ClientID identifies this client to the cluster; requests carry it with a sequence id.
func (c *Client) ClientID() int64 { return c.clientID }

// NextSeq hands out the next request sequence id.
func (c *Client) NextSeq() int64 { return c.seq.Add(1) }

// redirect follows an ErrWrongLeader answer. A server that does not know the leader
// answers with an index out of range or its own; either way wait briefly and try again.
func (c *Client) redirect(from int, hint int32) int {
	if int(hint) >= 0 && int(hint) < len(c.servers) && int(hint) != from {
		c.leader.Store(hint)
		return int(hint)
	}
	time.Sleep(10 * time.Millisecond)
	return c.Leader()
}

func (c *Client) call(server int, timeout time.Duration, fn func(ctx context.Context, kv kvrpc.KVClient) error) error {
	conn, err := c.pools[server].Get()
	if err != nil {
		return fmt.Errorf("client: conn to %s: %w", c.servers[server], err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return fn(ctx, kvrpc.NewKVClient(conn.Value()))
}

// Put writes key through the leader, following redirects. The response is returned as
// the leader sent it (Err is OK, or a server-side failure string such as "defeat");
// transport errors and timeouts come back as err.
func (c *Client) Put(key, value string) (*kvrpc.PutInRaftResponse, error) {
	req := &kvrpc.PutInRaftRequest{Key: key, Value: value, Op: "Put", ClientId: c.clientID, SeqId: c.NextSeq()}
	target := c.Leader()
	for {
		var reply *kvrpc.PutInRaftResponse
		err := c.call(target, c.opts.PutTimeout, func(ctx context.Context, kv kvrpc.KVClient) (e error) {
			reply, e = kv.PutInRaft(ctx, req)
			return
		})
		if err != nil {
			return nil, err
		}
		if reply.Err == kvrpc.ErrWrongLeader {
			target = c.redirect(target, reply.LeaderId)
			continue
		}
		return reply, nil
	}
}

// Get reads key through the leader, following redirects. found is false for ErrNoKey.
func (c *Client) Get(key string) (value string, found bool, err error) {
	target := c.Leader()
	for {
		reply, err := c.GetFrom(target, key)
		if err != nil {
			return "", false, err
		}
		switch reply.Err {
		case kvrpc.OK:
			return reply.Value, true, nil
		case kvrpc.ErrNoKey:
			return reply.Value, false, nil
		case kvrpc.ErrWrongLeader:
			target = c.redirect(target, reply.LeaderId)
		default:
			return reply.Value, false, fmt.Errorf("client: get %q: %s", key, reply.Err)
		}
	}
}

// GetFrom reads key from one specific server without following redirects. Verification
// tools use it to inspect a follower's local state.
func (c *Client) GetFrom(server int, key string) (*kvrpc.GetInRaftResponse, error) {
	req := &kvrpc.GetInRaftRequest{Key: key, ClientId: c.clientID, SeqId: c.NextSeq()}
	var reply *kvrpc.GetInRaftResponse
	err := c.call(server, c.opts.GetTimeout, func(ctx context.Context, kv kvrpc.KVClient) (e error) {
		reply, e = kv.GetInRaft(ctx, req)
		return
	})
	return reply, err
}

// Scan returns the rows in [start, end] through the leader, following redirects.
func (c *Client) Scan(start, end string) (*kvrpc.ScanRangeResponse, error) {
	target := c.Leader()
	for {
		reply, err := c.ScanFrom(target, start, end)
		if err != nil {
			return nil, err
		}
		if reply.Err == kvrpc.ErrWrongLeader {
			target = c.redirect(target, reply.LeaderId)
			continue
		}
		return reply, nil
	}
}

// ScanFrom scans one specific server without following redirects.
func (c *Client) ScanFrom(server int, start, end string) (*kvrpc.ScanRangeResponse, error) {
	req := &kvrpc.ScanRangeRequest{StartKey: start, EndKey: end}
	var reply *kvrpc.ScanRangeResponse
	err := c.call(server, c.opts.ScanTimeout, func(ctx context.Context, kv kvrpc.KVClient) (e error) {
		reply, e = kv.ScanRangeInRaft(ctx, req)
		return
	})
	return reply, err
}

func randomID() int64 {
	n, err := crand.Int(crand.Reader, big.NewInt(1<<62))
	if err != nil {
		return time.Now().UnixNano()
	}
	return n.Int64()
}
