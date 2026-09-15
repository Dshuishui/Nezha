package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

// 给落后太多的副本发快照
//
// 一个 follower 一旦落后到 leader 已经压缩掉的位置，就再也追不上：doAppendEntries 算出的
// 起点落进已压缩区间，每一轮都只能跳过它。今天代价被挪到了 leader 的内存上——compactLog
// 把压缩上界夹到 min(matchIndex)，于是 rf.log 随最慢 follower 的落后程度线性增长
// （实测一个 follower 停 40 秒，leader 的内存日志从 5000 条涨到 150000 条）。
// 快照是唯一的出路：压缩点可以越过落后者，再用状态快照把它整体补齐。
//
// **我们比通用状态机便宜。** 通用实现做快照最麻烦的一件事是"打快照时状态机还在被写"，
// 于是要么停写、要么 copy-on-write（dragonboat 的 concurrent snapshot、TiKV 借 RocksDB
// 的 snapshot 句柄）。我们不需要：GC 之后状态机的主体就是一组**封口即不可变**的分区文件。
// Raft 博士论文第 5 章讲 LSM 式日志清理时的原话是 "runs are immutable, so there is no
// concern of the runs being modified during the transfer"——我们的分区文件正是那个 run。
// 也就是说 GC 的产物本身就是快照格式，这是分区化设计顺带给出的一个能力。
//
// 一份快照由四样东西构成，合起来就是"截至某个 index 的完整状态"：
//
//	snapshot.json  清单：位点、分区列表、当前日志要取多长、有没有存储引擎导出
//	分区文件 + 旁挂索引  已经 GC 过的那部分数据，不可变，直接从原文件流出去
//	store.sst      存储引擎（key → 当前日志里的偏移）的导出，可 ingest
//	当前日志的前缀  上一轮 GC 之后的增量。追加写，所以前缀是一致切点
//
// 位点取**当前日志文件的基址**（rf.fileBaseIndex）：比它更早的数据已经被搬进分区文件，
// 而它之后的记录就在随快照一起发出去的那段日志里。接收端装完之后日志一直到切点处的
// 最后一条，leader 于是从那条的下一个 index 继续正常复制——不需要额外的追赶机制。
//
// **存储引擎的导出是有界的**，这一点值得说清楚：每轮 GC 切换都新建一个库，被取代的那个
// 随后删掉，所以当前库里只有上一轮 GC 之后写入的行。它的大小受 GC 阈值约束，不随总数据量
// 增长。落一份本地副本（SstFileWriter 是唯一能产出可 ingest 文件的途径，RocksDB 拒绝
// ingest 自己 flush/compaction 的产物）因此只是尾部那么大，而不是整个数据集那么大。
// 分区文件与日志前缀则一份副本都不做，直接从原文件流出去。
//
// 例行快照不在这里：状态机的数据本来就在盘上，applied index 与数据行写在同一个
// WriteBatch 里，所以"盘上已有的东西构成一个 index 位点"这件事是**零成本**的，
// 不需要制作任何东西（dragonboat 对 on-disk 状态机走的 saveDummy 就是这个语义：
// 周期性快照只是一个持久化的位点标记，用来授权日志压缩）。只有要发给落后副本时才物化，
// 也就是本文件做的事。

const (
	// snapshotWorkDir 是 leader 侧放导出产物的目录（只有 store.sst 与 snapshot.json）。
	// 进程启动时清空：传输不可续传，重启后半成品一律作废（三家业界实现都选幂等可重来
	// 而不是可续传，见 notes/REF-raft-snapshot.md）。
	snapshotWorkDir      = "snapshot"
	snapshotManifestName = "snapshot.json"
	snapshotStoreSSTName = "store.sst"
)

// errSnapshotBusy 表示现在做不了快照，稍后重试即可，不是错误。GC 正在搬运时就是这样：
// 此刻有两个日志文件和两个库，等它这一轮结束，状态会回到单文件单库的形态。
// etcd 在同一处用的是 ErrSnapshotTemporarilyUnavailable，raft 层收到后稍后重发。
var errSnapshotBusy = errors.New("snapshot: state machine is mid-GC, retry later")

// snapshotManifest 是快照的自描述清单，作为 snapshot.json 随快照一起传输。
//
// 里面的路径一律只存**文件名**。存绝对路径的后果在分区清单上已经踩过一次：数据目录一搬动，
// 节点就去打开原目录里的文件，而且只要原路径还在就不报错（实测 /proc/<pid>/fd 全部指向
// 原目录）。快照是跨节点搬运，这个坑在这里是必然会踩到而不是偶然。
type snapshotManifest struct {
	// LastIncludedIndex / Term 是快照覆盖到的位点：它之前的数据都在分区文件里。
	LastIncludedIndex int   `json:"last_included_index"`
	LastIncludedTerm  int32 `json:"last_included_term"`
	// AppliedIndex 是存储引擎导出所对应的 applied index，取自同一个读视图。
	AppliedIndex int `json:"applied_index"`
	// LastIndex / Term 是随快照发出的那段日志的最后一条记录。装完之后接收端的日志到这里。
	LastIndex int   `json:"last_index"`
	LastTerm  int32 `json:"last_term"`

	NumGC      int             `json:"num_gc"`
	SortedFile string          `json:"sorted_file"` // 分区组基名，文件名
	Partitions []partitionMeta `json:"partitions"`

	CurrentLog      string `json:"current_log"`       // 当前日志的文件名
	CurrentLogBytes int64  `json:"current_log_bytes"` // 只取前这么多字节
	// StoreSST 为空表示库里一行都没有（全新节点，或者上一轮 GC 之后没写过）。
	StoreSST  string `json:"store_sst"`
	StoreRows int    `json:"store_rows"`
}

// snapshot 是一份做好的快照。release 必须在传输结束后调用（成功或失败都要）：
// 它放开分区文件的引用并删掉 leader 侧的导出产物。
type snapshot struct {
	manifest snapshotManifest
	files    []raft.SSTableFile
	release  func()
}

// totalBytes 是这份快照要传的字节数，用于日志与限速。
func (s *snapshot) totalBytes() int64 {
	var n int64
	for _, f := range s.files {
		if f.Limit > 0 {
			n += f.Limit
			continue
		}
		if st, err := os.Stat(f.Path); err == nil {
			n += st.Size()
		}
	}
	return n
}

func (kvs *KVServer) snapshotWorkPath() string {
	return filepath.Join(kvs.dataDir, "data", snapshotWorkDir)
}

// clearSnapshotWork 清空 leader 侧的导出目录。进程启动时调用一次：上次留下的 store.sst
// 属于一次没走完的传输，而传输是幂等重来的，半成品没有任何用处，留着只占盘。
func (kvs *KVServer) clearSnapshotWork() {
	dir := kvs.snapshotWorkPath()
	if err := os.RemoveAll(dir); err != nil {
		fmt.Printf("[SNAPSHOT] 清理上次的导出目录 %s 失败: %v\n", dir, err)
	}
}

// createSnapshot 物化一份当前状态的快照。
//
// 三件事的**先后顺序是有约束的**，颠倒了会做出一份自相矛盾的快照：
//
//  1. 先在一个临界区里钉住分区组、读下轮次与当前日志名。分区文件封口即不可变，钉住之后
//     GC 就不能删它们了——传输要几分钟，期间 leader 会正常跑 GC。
//  2. 再导出存储引擎。导出在一个 RocksDB snapshot 句柄上做，数据行与 applied 标记取自
//     同一个读视图，所以位点与数据天然一致（ExportStoreSST 因此把 applied 作为返回值
//     而不是入参）。
//  3. 最后切日志。**必须在导出之后**：切点会刷净并 fsync，所以切点覆盖的记录一定包含
//     导出时已经 applied 的全部记录，于是 applied <= 切点的 LastIndex。反过来先切后导，
//     导出的 applied 可能超过切点，接收端就会看到"applied 比日志里最后一条记录还大"，
//     恢复直接放弃。
//
// 收尾还要复核轮次没变：CutLog 分两段取锁（logMu 取长度、rf.mu 取基址），中间夹得进一次
// GC 切换，而切换会换掉库与日志文件，此前导出的那份就对不上了。复核不通过就整份作废重来——
// 快照是幂等可重来的，作废一次的代价只是白导一次尾部。
func (kvs *KVServer) createSnapshot() (*snapshot, error) {
	t0 := time.Now()
	kvs.mu.Lock()
	if kvs.gcInProgress {
		kvs.mu.Unlock()
		return nil, errSnapshotBusy
	}
	numGC, currentLog, sortedFile := kvs.numGC, kvs.currentLog, kvs.sortedFilePath
	ps, releasePins := kvs.pinPartitionsLocked()
	manifest := kvs.lastPartitions.manifest()
	kvs.mu.Unlock()

	dir := kvs.snapshotWorkPath()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		releasePins()
		return nil, fmt.Errorf("snapshot work dir: %w", err)
	}
	sstPath := filepath.Join(dir, snapshotStoreSSTName)
	manifestPath := filepath.Join(dir, snapshotManifestName)
	release := func() {
		releasePins()
		os.Remove(sstPath)
		os.Remove(manifestPath)
	}

	rows, applied, err := kvs.persister.ExportStoreSST(sstPath)
	if err != nil {
		release()
		return nil, fmt.Errorf("export store: %w", err)
	}
	cut, err := kvs.raft.CutLog()
	if err != nil {
		release()
		return nil, fmt.Errorf("cut log: %w", err)
	}

	kvs.mu.Lock()
	changed := kvs.gcInProgress || kvs.numGC != numGC || kvs.currentLog != currentLog
	kvs.mu.Unlock()
	if changed {
		release()
		return nil, errSnapshotBusy
	}
	if cut.Path != currentLog {
		// 与上面那条复核同因（GC 切换），只是从 raft 侧看到的。分开报是为了在日志里能
		// 区分到底是哪一边先变的。
		release()
		return nil, errSnapshotBusy
	}
	if applied > cut.LastIndex {
		// 导出在切点之前完成，所以这不该发生。真发生了说明先后顺序被改坏了，
		// 宁可不发也不要发一份接收端起不来的快照。
		release()
		return nil, fmt.Errorf("snapshot: store applied=%d is ahead of the log cut at %d", applied, cut.LastIndex)
	}

	sm := snapshotManifest{
		LastIncludedIndex: cut.BaseIndex,
		LastIncludedTerm:  cut.BaseTerm,
		AppliedIndex:      applied,
		LastIndex:         cut.LastIndex,
		LastTerm:          cut.LastTerm,
		NumGC:             numGC,
		SortedFile:        baseNameOrEmpty(sortedFile),
		Partitions:        manifest,
		CurrentLog:        filepath.Base(cut.Path),
		CurrentLogBytes:   cut.Bytes,
		StoreRows:         rows,
	}
	if rows > 0 {
		sm.StoreSST = snapshotStoreSSTName
	}
	data, err := json.MarshalIndent(sm, "", "  ")
	if err != nil {
		release()
		return nil, fmt.Errorf("marshal snapshot manifest: %w", err)
	}
	if err := raft.WriteFileAtomic(manifestPath, data); err != nil {
		release()
		return nil, fmt.Errorf("write snapshot manifest: %w", err)
	}

	// 清单排在最前面：接收端按文件名查找，不依赖顺序，但一个人读传输日志时先看到清单
	// 比先看到一堆分区文件有用。
	files := []raft.SSTableFile{{Path: manifestPath}}
	if rows > 0 {
		files = append(files, raft.SSTableFile{Path: sstPath})
	}
	// 当前日志只发前 cut.Bytes 字节——它正在被追加，超出切点的字节不属于这份快照。
	files = append(files, raft.SSTableFile{Path: cut.Path, Limit: cut.Bytes})
	for _, p := range ps.Paths() {
		files = append(files, raft.SSTableFile{Path: p})
		if idx := sparseIndexPath(p); fileExists(idx) {
			// 旁挂索引可有可无（接收端读不到就扫描重建），但带上它能把安装时间从
			// O(数据量) 变成 O(索引量)——实测 34 个分区 / 278MB，读索引 3.5ms，
			// 扫描重建 266ms。
			files = append(files, raft.SSTableFile{Path: idx})
		}
	}

	snap := &snapshot{manifest: sm, files: files, release: release}
	fmt.Printf("[SNAPSHOT] 做好一份：位点=(%d,%d) applied=%d 日志到=%d 分区=%d 库行数=%d "+
		"文件=%d 共%dB 耗时=%v\n",
		sm.LastIncludedIndex, sm.LastIncludedTerm, sm.AppliedIndex, sm.LastIndex,
		len(sm.Partitions), rows, len(files), snap.totalBytes(), time.Since(t0))
	return snap, nil
}

func baseNameOrEmpty(p string) string {
	if p == "" {
		return ""
	}
	return filepath.Base(p)
}

func fileExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && !st.IsDir()
}
