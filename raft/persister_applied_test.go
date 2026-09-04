package raft

import (
	"path/filepath"
	"testing"
)

func TestAppliedIndexTravelsWithData(t *testing.T) {
	p := &Persister{}
	if _, err := p.Init(filepath.Join(t.TempDir(), "db"), true); err != nil {
		t.Skipf("RocksDB not available: %v", err)
	}
	defer p.Close()

	if _, ok, err := p.GetApplied(); ok || err != nil {
		t.Fatalf("fresh db: ok=%v err=%v, want (false, nil)", ok, err)
	}
	p.PutOffsetApplied("42", 1234, 7)
	if a, ok, _ := p.GetApplied(); !ok || a != 7 {
		t.Fatalf("after PutOffsetApplied: %d/%v, want 7/true", a, ok)
	}
	if off, err := p.Get_opt("42"); err != nil || off != 1234 {
		t.Fatalf("data row: off=%d err=%v", off, err)
	}
	p.PutInlineApplied("43", "small", 8)
	p.PutValueApplied("44", "plain", 9)
	p.SetApplied(10)
	if a, _, _ := p.GetApplied(); a != 10 {
		t.Fatalf("applied = %d, want 10", a)
	}
	if v, ok := p.GetInline("43"); !ok || v != "small" {
		t.Fatalf("inline row: %q/%v", v, ok)
	}
	// 元数据键不能混进用户数据的扫描
	if !IsMetaKey([]byte(appliedIndexKey)) || IsMetaKey([]byte(p.PadKey("1"))) {
		t.Fatal("IsMetaKey misclassifies keys")
	}
}
