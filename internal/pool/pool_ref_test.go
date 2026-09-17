package pool

import (
	"errors"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
)

// Get 出错时**不能**留下引用。
//
// 引用是 conn.Close() 释放的，而全部调用方都写成
// `conn, err := p.Get(); if err != nil { return }`——出错时不会有人去 Close。
// 漏一份，decrRef 里 `newRef == 0` 这个收缩判据就永不成立，池再也不会缩回 MaxIdle。
func TestGetDoesNotLeakRefOnError(t *testing.T) {
	// 已关闭的池：current == 0，走 ErrClosed 那条路
	p := &pool{
		opt:     DefaultOptions,
		conns:   make([]*conn, DefaultOptions.MaxActive),
		address: []string{"127.0.0.1:1"},
	}
	atomic.StoreInt32(&p.current, 0)

	for i := 0; i < 5; i++ {
		if _, err := p.Get(); !errors.Is(err, ErrClosed) {
			t.Fatalf("第 %d 次 Get 应回 ErrClosed，得到 %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&p.ref); got != 0 {
		t.Fatalf("5 次失败的 Get 之后 ref = %d, want 0——漏掉的引用会让池永不收缩", got)
	}
}

// 一次性连接那条路 dial 失败时，既不能漏引用，也不能把一个包着 nil ClientConn
// 的 conn 交出去（谁真去调 conn.Value() 就是空指针）。
func TestGetOneTimeDialFailureLeavesNoRef(t *testing.T) {
	dialErr := errors.New("dial refused")
	opt := DefaultOptions
	opt.MaxIdle = 1
	opt.MaxActive = 1
	opt.MaxConcurrentStreams = 1
	opt.Reuse = false
	opt.Dial = func(string) (*grpc.ClientConn, error) { return nil, dialErr }

	p := &pool{opt: opt, conns: make([]*conn, opt.MaxActive), address: []string{"127.0.0.1:1"}}
	atomic.StoreInt32(&p.current, 1)
	// 先把 ref 顶到上限，这样下一次 Get 会落到"一次性连接"那一支
	atomic.StoreInt32(&p.ref, int32(opt.MaxConcurrentStreams))

	before := atomic.LoadInt32(&p.ref)
	c, err := p.Get()
	if !errors.Is(err, dialErr) {
		t.Fatalf("Get 应回 dial 错误，得到 %v", err)
	}
	if c != nil {
		t.Errorf("出错时还返回了 conn=%v——它包着 nil ClientConn", c)
	}
	if got := atomic.LoadInt32(&p.ref); got != before {
		t.Errorf("失败的 Get 让 ref 从 %d 变成 %d，应当不变", before, got)
	}
}
