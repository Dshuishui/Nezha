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
	"strings"
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

	// KeyPadWidth left-pads decimal keys with '0' to this width before sending them.
	//
	// The store keeps keys verbatim and orders them bytewise, which is the contract every
	// ordered KV store offers (RocksDB, LevelDB, TiKV). Bytewise order is not numeric
	// order — "10" sorts before "9" — so a workload whose keys are integers and whose
	// scans mean numeric ranges has to encode them itself. That encoding lives here, in
	// one place, rather than in the store: it is only injective on integers, and the
	// store must not assume its keys are integers. Applying it there is what silently
	// merged "7" with "007" and truncated anything past KeyLength.
	//
	// 0 selects DefaultKeyPadWidth, which keeps every existing benchmark tool working
	// unchanged. Set KeyPadNone for keys that are already order-preserving or not
	// numeric at all — YCSB's "user"+hash keys, for instance.
	KeyPadWidth int
}

const (
	// DefaultKeyPadWidth covers the benchmark key space: keys are strconv.Itoa(i) for
	// i < 10^10, so ten digits order correctly and match the width the store used to
	// pad to internally.
	DefaultKeyPadWidth = 10
	// KeyPadNone sends keys exactly as given.
	KeyPadNone = -1
)

// ErrLeadingZero rejects a key the padding could not tell apart from another one.
//
// Padding is injective only on canonical decimal integers: "7" and "007" pad to the same
// ten-character string, and nothing downstream can separate them again. Refusing the
// non-canonical form is what makes the encoding lossless, instead of merely documented as
// lossy — that documentation is exactly what the old storage-layer padding had.
var ErrLeadingZero = errors.New("client: key has a leading zero, which the fixed-width encoding cannot represent distinctly (use KeyPadNone to send keys verbatim)")

// isDecimal reports whether the key is made only of ASCII digits. Padding is meaningful
// only for those; see encodeKey.
func isDecimal(key string) bool {
	if key == "" {
		return false
	}
	for i := 0; i < len(key); i++ {
		if key[i] < '0' || key[i] > '9' {
			return false
		}
	}
	return true
}

// encodeKey applies KeyPadWidth. It never truncates: a key at least as long as the width
// goes out untouched, so a wider key is stored in full rather than silently cut short.
//
// 补零**只对十进制整数做**。非数字的 key 原样发出去，因为左补零会打乱它们的字节序：
// "user1" 与 "user12" 补到十位是 "00000user1" 与 "0000user12"，第 5 个字节上
// '0' < 'u'，于是 "user12" 排在 "user1" **前面**。存储层按字节序排、分区按字节区间
// 路由、SCAN 建立在这个序上，所以补零把一个正确的序改成了错的。
// 这与上面 ErrLeadingZero 是同一个原则：把"只对规范十进制成立"从注释里的说明
// 变成代码里的约束。
//
// **已知限制**：超过 w 位的十进制 key 原样通过，而它的字节序不再等于数值序——
// w=10 时 "10000000000"（11 位）按字节排在 "9999999999" 之前。
// 不截断是对的（截断会让不同的 key 撞成一个），但要换来正确的序就得换一种编码。
// 现有 benchmark 的键空间上限约 10^8，够用；把键空间推到 10^10 以上之前，
// SCAN 的区间判定不能再假定"字节序 == 数值序"。client_test.go 里钉住了这一条。
func (c *Client) encodeKey(key string) (string, error) {
	w := c.opts.KeyPadWidth
	if w <= 0 {
		return key, nil
	}
	if len(key) > 1 && key[0] == '0' {
		return "", ErrLeadingZero
	}
	if len(key) >= w || !isDecimal(key) {
		return key, nil
	}
	return strings.Repeat("0", w-len(key)) + key, nil
}

// decodeKey undoes encodeKey. Only an all-digit key of exactly the padded width can have
// been padded by us, so anything else is returned as it came back.
func (c *Client) decodeKey(key string) string {
	w := c.opts.KeyPadWidth
	if w <= 0 || len(key) != w || !isDecimal(key) {
		return key
	}
	trimmed := strings.TrimLeft(key, "0")
	if trimmed == "" {
		return "0" // an all-zero string can only have come from "0"
	}
	return trimmed
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
	if o.KeyPadWidth == 0 {
		o.KeyPadWidth = DefaultKeyPadWidth
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
	key, err := c.encodeKey(key)
	if err != nil {
		return nil, err
	}
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
	key, err := c.encodeKey(key)
	if err != nil {
		return nil, err
	}
	req := &kvrpc.GetInRaftRequest{Key: key, ClientId: c.clientID, SeqId: c.NextSeq()}
	var reply *kvrpc.GetInRaftResponse
	err = c.call(server, c.opts.GetTimeout, func(ctx context.Context, kv kvrpc.KVClient) (e error) {
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
//
// Both bounds go through encodeKey and the returned keys come back through decodeKey, so a
// caller working in unpadded integers sees unpadded integers. Encoding the bounds is what
// makes the range mean what the caller meant: the store compares bytes, and "9" > "10"
// bytewise.
func (c *Client) ScanFrom(server int, start, end string) (*kvrpc.ScanRangeResponse, error) {
	start, err := c.encodeKey(start)
	if err != nil {
		return nil, err
	}
	end, err = c.encodeKey(end)
	if err != nil {
		return nil, err
	}
	req := &kvrpc.ScanRangeRequest{StartKey: start, EndKey: end}
	var reply *kvrpc.ScanRangeResponse
	err = c.call(server, c.opts.ScanTimeout, func(ctx context.Context, kv kvrpc.KVClient) (e error) {
		reply, e = kv.ScanRangeInRaft(ctx, req)
		return
	})
	if reply != nil && len(reply.KeyValuePairs) > 0 && c.opts.KeyPadWidth > 0 {
		decoded := make(map[string]string, len(reply.KeyValuePairs))
		for k, v := range reply.KeyValuePairs {
			decoded[c.decodeKey(k)] = v
		}
		reply.KeyValuePairs = decoded
	}
	return reply, err
}

func randomID() int64 {
	n, err := crand.Int(crand.Reader, big.NewInt(1<<62))
	if err != nil {
		return time.Now().UnixNano()
	}
	return n.Int64()
}
