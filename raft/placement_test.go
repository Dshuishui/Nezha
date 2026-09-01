package raft

import (
	"os"
	"testing"
)

// 存储引擎里现在存着两种东西：8 字节偏移，或内联的 value 本身。
// 早先靠长度区分（Get_opt 检查 len != 8），一旦开始内联就会撞车——
// 一个 7 字节的 value 加上标记正好也是 8 字节。这几个用例钉住标记字节的语义。

func newTestPersister(t *testing.T) *Persister {
	t.Helper()
	dir, err := os.MkdirTemp("", "placement")
	if err != nil {
		t.Fatal(err)
	}
	p := &Persister{}
	p, err = p.Init(dir, true)
	if err != nil {
		t.Skipf("RocksDB 不可用，跳过: %v", err)
	}
	t.Cleanup(func() { p.Close(); os.RemoveAll(dir) })
	return p
}

func TestOffsetAndInlineDoNotCollide(t *testing.T) {
	p := newTestPersister(t)

	p.Put_opt("offkey", 123456789)
	got, err := p.Get_opt("offkey")
	if err != nil {
		t.Fatalf("Get_opt: %v", err)
	}
	if got != 123456789 {
		t.Fatalf("偏移读回 %d, want 123456789", got)
	}
	if _, ok := p.GetInline("offkey"); ok {
		t.Fatal("偏移记录被误判为内联 value")
	}

	// 7 字节 value：加上标记后总长 8，正是旧的长度判别会撞车的那个尺寸
	p.PutInline("inkey", "1234567")
	v, ok := p.GetInline("inkey")
	if !ok {
		t.Fatal("内联 value 读不回来")
	}
	if v != "1234567" {
		t.Fatalf("内联 value = %q, want %q", v, "1234567")
	}
	if _, err := p.Get_opt("inkey"); err == nil {
		t.Fatal("内联记录被当成了偏移解析")
	}
}

func TestInlineRoundTripAcrossSizes(t *testing.T) {
	p := newTestPersister(t)
	cases := []string{"", "a", "1234567", "12345678", "123456789", string(make([]byte, 500))}
	for i, want := range cases {
		key := "k" + string(rune('A'+i))
		p.PutInline(key, want)
		got, ok := p.GetInline(key)
		if !ok {
			t.Fatalf("长度 %d 的 value 读不回来", len(want))
		}
		if got != want {
			t.Fatalf("长度 %d：读回 %d 字节, want %d 字节", len(want), len(got), len(want))
		}
	}
}

func TestGetInlineMissingKey(t *testing.T) {
	p := newTestPersister(t)
	if _, ok := p.GetInline("nope"); ok {
		t.Fatal("不存在的 key 报告为内联命中")
	}
}
