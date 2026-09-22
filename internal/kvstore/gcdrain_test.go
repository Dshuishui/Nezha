package kvstore

import (
	"os"
	"path/filepath"
	"testing"
)

// drainFixture 搭一个只够跑排空判定的 KVServer：数据目录、GC 开关、当前日志。
func drainFixture(t *testing.T, tailBytes int) *KVServer {
	t.Helper()
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	log := filepath.Join(dir, "data", "RaftState.log")
	if err := os.WriteFile(log, make([]byte, tailBytes), 0o644); err != nil {
		t.Fatal(err)
	}
	return &KVServer{dataDir: dir, gcEnabled: true, kvSeparation: true, currentLog: log}
}

func touchDrain(t *testing.T, kvs *KVServer) {
	t.Helper()
	f, err := os.Create(kvs.gcDrainPath())
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
}

// 没有握手文件就不该进排空模式——否则 GC 会无视阈值一直吸收，
// 那等于把「按比例触发」这个摊销机制整个关掉。
func TestGCDrainOffWithoutHandshakeFile(t *testing.T) {
	kvs := drainFixture(t, 4<<20)
	if kvs.gcDrainRequested() {
		t.Fatal("没有握手文件却进了排空模式")
	}
}

func TestGCDrainOnWithHandshakeFile(t *testing.T) {
	kvs := drainFixture(t, 4<<20)
	touchDrain(t, kvs)
	if !kvs.gcDrainRequested() {
		t.Fatal("有握手文件却没进排空模式")
	}
	// 重复调用不该重置轮数——gcLoop 每 5 秒问一次。
	kvs.noteGCDrainRound()
	kvs.noteGCDrainRound()
	if !kvs.gcDrainRequested() {
		t.Fatal("第二次询问就退出了排空模式")
	}
	if got := kvs.gcDrain.rounds.Load(); got != 2 {
		t.Fatalf("轮数 = %d，想要 2（再次询问把它清零了）", got)
	}
}

// **完成的信号是节点删掉握手文件。** 删不掉或者忘了删，驱动会一直等一个
// 永远不会成立的条件——所以这条要钉死。
func TestGCDrainFinishRemovesHandshakeFile(t *testing.T) {
	kvs := drainFixture(t, 4<<20)
	touchDrain(t, kvs)
	kvs.gcDrainRequested()
	kvs.noteGCDrainRound()

	kvs.finishGCDrain(512)

	if _, err := os.Stat(kvs.gcDrainPath()); !os.IsNotExist(err) {
		t.Fatalf("排空完成后握手文件还在（err=%v）——驱动会一直等下去", err)
	}
	if kvs.gcDrain.requested.Load() {
		t.Fatal("排空完成后 requested 还是 true")
	}
	// 完成之后再问，不该又进排空模式。
	if kvs.gcDrainRequested() {
		t.Fatal("完成之后又进了排空模式")
	}
}

// **不跑 GC 的系统必须早退。** 否则 baseline 会带着一个永远等不到删除的握手文件，
// 驱动在那里等到超时，而日志上一个字都没有——那正是最难查的一类失败。
func TestGCDrainIgnoredWhenSystemHasNoGC(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		gcEnabled, kvSeparation bool
	}{
		{"baseline", false, false},
		{"nezha-nogc", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs := drainFixture(t, 4<<20)
			kvs.gcEnabled, kvs.kvSeparation = tc.gcEnabled, tc.kvSeparation
			touchDrain(t, kvs)
			if kvs.gcDrainRequested() {
				t.Fatalf("%s 不跑 GC，却进了排空模式", tc.name)
			}
		})
	}
}

// 见底线不能取 0：valuelog 永远有一条 NoOp 打头，吸收完一轮之后新文件里也马上有
// 元数据行。取 0 会让排空永不结束，而那是**静默**的——驱动只会看到超时。
func TestGCDrainFloorLeavesRoomForMetadata(t *testing.T) {
	if gcDrainFloorBytes <= 0 {
		t.Fatalf("见底线 = %d，取 0 或负数会让排空永不结束", gcDrainFloorBytes)
	}
	// 比任何元数据都大得多
	if gcDrainFloorBytes < 64<<10 {
		t.Fatalf("见底线 %d 字节太小，元数据行就能顶住它", gcDrainFloorBytes)
	}
	// 又要远小于最小的 GC 阈值（0.3 GiB），否则「见底」会在还有大量用户数据时就成立
	const smallestThreshold = 3 * (1 << 30) / 10
	if gcDrainFloorBytes > smallestThreshold/100 {
		t.Fatalf("见底线 %d 字节相对最小 GC 阈值 %d 太大，会在还有用户数据时就判见底",
			gcDrainFloorBytes, smallestThreshold)
	}
}
