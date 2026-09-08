package kvstore

import (
	"path/filepath"
	"strconv"
	"testing"
)

// 归并轮的路径必须只跟轮号有关，跟"跑过几轮"无关。
//
// 原先是每轮就地累加全局变量（`p = fmt.Sprintf("%s_%d", p, round)`），于是第 2 轮得到
// newRaftState_1_2、第 3 轮 newRaftState_1_2_3、第 4 轮 _1_2_3_4 —— 名字随轮数越滚越长。
// 两轮上限一直盖着它，放开轮数上限就会立刻踩到，而且崩溃恢复按固定规则去猜路径，对不上就
// 找不到文件。这条用例钉住"连着算 5 轮，第 N 轮永远是 <基名>_N"。
func TestMergeRoundPathsDoNotAccumulate(t *testing.T) {
	dir := t.TempDir()
	InitAnotherGCPaths(dir)

	vlog := filepath.Join(dir, "data", "valuelog")
	dbdir := filepath.Join(dir, "data", "dbfile")

	for round := 2; round <= 6; round++ {
		gotLog, gotDB := mergeRoundPaths(round)
		wantLog := filepath.Join(vlog, "newRaftState_"+strconv.Itoa(round))
		wantDB := filepath.Join(dbdir, "newKeyIndex_"+strconv.Itoa(round))
		if gotLog != wantLog {
			t.Errorf("第 %d 轮日志路径 = %q; want %q", round, gotLog, wantLog)
		}
		if gotDB != wantDB {
			t.Errorf("第 %d 轮存储引擎路径 = %q; want %q", round, gotDB, wantDB)
		}
	}

	// 再算一遍同一轮，必须完全一致——派生是纯函数，不能有副作用
	l1, d1 := mergeRoundPaths(3)
	l2, d2 := mergeRoundPaths(3)
	if l1 != l2 || d1 != d2 {
		t.Errorf("重复调用同一轮结果不同: (%q,%q) vs (%q,%q)", l1, d1, l2, d2)
	}
}

// 第 1 轮走的是 gc_first 的路径，与归并轮的命名不能撞车，否则第 2 轮会打开第 1 轮还开着的库。
func TestFirstAndMergeRoundPathsDoNotCollide(t *testing.T) {
	dir := t.TempDir()
	InitGCPaths(dir)
	InitAnotherGCPaths(dir)

	round2Log, round2DB := mergeRoundPaths(2)
	if round2Log == firstNewRaftStateLogPath {
		t.Errorf("第 2 轮的日志路径与第 1 轮相同: %q", round2Log)
	}
	if round2DB == firstNewPersisterPath {
		t.Errorf("第 2 轮的存储引擎路径与第 1 轮相同: %q", round2DB)
	}
}
