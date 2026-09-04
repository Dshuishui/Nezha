package raft

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// 攒批必须保持 index 升序，且每条记录的内容各自独立。
// 原实现用包级的 entry_global 取地址，逐条写入时无碍；攒批后同一批里的指针
// 会全部指向它，写出去的是最后一条重复 N 次——这个用例专门钉住这一点。
func TestGroupCommitPreservesOrderAndContent(t *testing.T) {
	dir := t.TempDir()
	rf := newTestRaft(t, dir)
	rf.EnableGroupCommit(2 * time.Millisecond)

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			e := entryOf(uint32(k+1), fmt.Sprintf("k%03d", k), fmt.Sprintf("v%03d", k))
			b, first := func() (*flushBatch, bool) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				return rf.enqueueForFlush(e)
			}()
			if first {
				select {
				case rf.flushSignal <- struct{}{}:
				default:
				}
			}
			<-b.done
		}(i)
	}
	wg.Wait()

	raw, err := os.ReadFile(rf.currentLog)
	if err != nil {
		t.Fatal(err)
	}
	// 顺序解析整个文件，校验每条记录的 key 与 value 配对正确、且互不相同
	seen := map[string]string{}
	var off int64
	for off < int64(len(raw)) {
		keySize := binary.LittleEndian.Uint32(raw[off+12 : off+16])
		valSize := binary.LittleEndian.Uint32(raw[off+16 : off+20])
		key := string(raw[off+20 : off+20+int64(keySize)])
		val := string(raw[off+20+int64(keySize) : off+20+int64(keySize)+int64(valSize)])
		seen[strings.TrimLeft(key, "0")] = val
		off += 20 + int64(keySize) + int64(valSize)
	}
	if len(seen) != n {
		t.Fatalf("文件中有 %d 条不同的 key，want %d —— 攒批可能写重了同一条", len(seen), n)
	}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("k%03d", i)
		want := fmt.Sprintf("v%03d", i)
		if seen[k] != want {
			t.Fatalf("key %s 的值为 %q, want %q —— entry 之间串了内容", k, seen[k], want)
		}
	}
	// Offsets 必须与写入顺序一一对应
	if len(rf.Offsets) != n {
		t.Fatalf("Offsets 有 %d 项, want %d", len(rf.Offsets), n)
	}
	for i := 1; i < len(rf.Offsets); i++ {
		if rf.Offsets[i] <= rf.Offsets[i-1] {
			t.Fatalf("Offsets 非递增：第 %d 项 %d <= 第 %d 项 %d",
				i, rf.Offsets[i], i-1, rf.Offsets[i-1])
		}
	}
}
