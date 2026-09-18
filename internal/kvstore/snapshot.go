package kvstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/raftrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
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

// ---- 接收端：安装 ----
//
// 安装要么整体生效要么整体不生效，判据是 `kv_state.json`：它是最后一步写的，与 GC 完成
// 的提交顺序一致（先落数据，最后落状态文件）。崩在它之前，节点重启后仍是**原来**的状态，
// 这份快照只是没装上，leader 会重发；崩在它之后，新状态已经完整。
//
// 为此，收到的文件一律落在**快照专属的名字**下，绝不覆盖本节点自己的文件：
// 分区是 `<sorted>_snap<位点>.pN`、日志是 `RaftState_snap<位点>.log`、
// 库是 `dbfile/keyIndex_snap<位点>`。如果直接用 leader 那边的名字，两边名字撞上时
// （轮次相同就会撞）就会在旧状态文件还指着旧文件的时候把旧文件毁掉——那是不可恢复的。
// 顺带也让"崩在中途"留下的东西一眼能认出来是快照产物。
//
// 不做断点续传：接收端的半成品一律丢弃重来（TiKV 接收端崩了从头重放、CockroachDB 是一次
// 原子 ingest-and-excise，三家都选幂等可重来）。所以这里不需要记录"收到哪了"。

// snapshotInstallNames 给一份位点为 idx 的快照产出本节点上的落位名字。
type snapshotInstallNames struct {
	valuelog   string // 分区与日志所在目录
	indexDir   string // 旁挂索引所在目录
	logPath    string // 日志文件的落位
	sortedBase string // 分区组的基名（不含 .pN）
	storePath  string // 新库的目录
}

func (kvs *KVServer) snapshotInstallNames(idx int) snapshotInstallNames {
	vlog := filepath.Join(kvs.dataDir, "data", "valuelog")
	return snapshotInstallNames{
		valuelog:   vlog,
		indexDir:   filepath.Join(vlog, sparseIndexDir),
		logPath:    filepath.Join(vlog, fmt.Sprintf("RaftState_snap%d.log", idx)),
		sortedBase: filepath.Join(vlog, fmt.Sprintf("RaftState_sorted_snap%d", idx)),
		storePath:  filepath.Join(kvs.dataDir, "data", "dbfile", fmt.Sprintf("keyIndex_snap%d", idx)),
	}
}

// installSnapshot 是接收端的 SSTableInstaller 对 SNAPSHOT 载荷的处理。
// 返回值是安装之后本节点日志覆盖到的 index，leader 据此从下一个 index 继续正常复制。
func (kvs *KVServer) installSnapshot(span raft.SSTableSpan) (int, raftrpc.InstallSSTableStatus) {
	t0 := time.Now()
	byName := map[string]string{}
	for _, f := range span.Files {
		byName[filepath.Base(f.Path)] = f.Path
	}
	sm, err := readSnapshotManifest(byName[snapshotManifestName])
	if err != nil {
		util.EPrintf("[SNAPSHOT] 收到的快照没有可用的清单: %v", err)
		return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_FAILED
	}
	// 清单说有的文件必须都收到了。少一个就装出一个**静默残缺**的状态机：读路径把
	// "这一处没有"当作常态（数据分散在几处，一次读并发查），所以缺一个分区只会在某次
	// GET 上变成一个 NOKEY，不报错。
	if err := checkSnapshotFiles(sm, byName); err != nil {
		util.EPrintf("[SNAPSHOT] %v", err)
		return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_FAILED
	}

	// 整体替换期间 apply 不能在跑，也不能有 GC 在换文件。
	//
	// **这把写锁现在只等 apply 与几次字段捕获，都是纳秒到微秒级。** 读路径原先是整段读
	// 都持着 stateMu 的读锁，而 Go 的 RWMutex 在有写者等待时不再放新读者进来，
	// applyCommand 又同样按读锁持有它——于是这里一等，apply 就停一次扫描的时长
	// （4GB 规模下 33 秒）。现在读路径只在 captureState 那一小段持读锁，
	// 生命周期交给 storeRetireMu（见 read.go 的 stateSnapshot）。
	//
	// 为什么 apply 这一侧的互斥必须留着：上面那条 lastAppliedIndex 的检查在很早就做完了，
	// 而下面的落位/ingest/装分区组要花几秒。这段窗口里若 apply 还能推进，
	// 第 5 步把 lastAppliedIndex 重设成 sm.AppliedIndex 就是**往回退**，
	// 而那几条已应用的数据在旧库里、第 7 步会被删掉。
	waitStart := time.Now()
	kvs.stateMu.Lock()
	if waited := time.Since(waitStart); waited > time.Second {
		// 现在它不该再等很久了。真等久了说明有别的东西在长时间持 stateMu 的读锁，
		// 那是新问题，要报出来而不是静默忍受。
		fmt.Printf("[SNAPSHOT] 等状态机写锁等了 %v 才拿到——期间 apply 一并被挡住。"+
			"读路径已经不整段持这把锁了，所以这说明有别的长时间读者，值得查\n",
			waited.Round(time.Millisecond))
	}
	defer kvs.stateMu.Unlock()

	kvs.mu.Lock()
	if kvs.gcActive || kvs.gcInProgress {
		// 让 leader 稍后重试而不是勉强安装：GC 正在搬运时盘上有两个日志文件和两个库，
		// 等它这一轮结束，状态会回到单文件单库的形态。leader 侧的重发退避负责节奏。
		kvs.mu.Unlock()
		util.DPrintf("[SNAPSHOT] GC 正在跑，本次安装退回，等 leader 重发")
		return kvs.lastAppliedIndex, raftrpc.InstallSSTableStatus_FAILED
	}
	if kvs.lastAppliedIndex >= sm.LastIndex {
		// 比本节点还旧的快照直接忽略。
		la := kvs.lastAppliedIndex
		kvs.mu.Unlock()
		util.DPrintf("[SNAPSHOT] 快照到 %d，本节点已 applied 到 %d，跳过", sm.LastIndex, la)
		return la, raftrpc.InstallSSTableStatus_SKIPPED
	}
	kvs.installing = true
	prevPartitions, prevLog, prevStore := kvs.lastPartitions, kvs.currentLog, kvs.currentDBPath
	kvs.mu.Unlock()

	// Raft 侧再判一次"会不会后退"。上面那条用的是 KV 层的 applied，而真正不能后退的是
	// **commitIndex**：applied 落后于 commitIndex 的那一段是已提交未应用，装快照会把
	// commitIndex 重设为快照里的 applied，于是那一段已提交的条目被丢掉。
	// 必须在换任何指针之前问：InstallSnapshotState 走到一半失败没法回滚，
	// 调用方只能 log.Fatalf。
	if regress, why := kvs.raft.SnapshotRegresses(sm.LastIncludedIndex, sm.LastIndex); regress {
		util.DPrintf("[SNAPSHOT] 拒绝会让本节点后退的快照：%s", why)
		return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_SKIPPED
	}
	defer func() {
		kvs.mu.Lock()
		kvs.installing = false
		kvs.mu.Unlock()
	}()

	names := kvs.snapshotInstallNames(sm.LastIncludedIndex)
	fail := func(format string, a ...any) (int, raftrpc.InstallSSTableStatus) {
		util.EPrintf("[SNAPSHOT] 安装失败（本节点仍是原来的状态）: "+format, a...)
		// 落位一半的文件留在盘上也不影响正确性——状态文件还没写，没人引用它们。
		// 下次启动清空收件目录时会带走没落位的那些，落位了的则成为孤儿文件。
		return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_FAILED
	}
	if err := os.MkdirAll(names.indexDir, 0o755); err != nil {
		return fail("建索引目录: %v", err)
	}

	// 1) 分区与旁挂索引落位，同时把清单里的文件名改成落位后的名字。
	//    用 rename：收件目录就在 dataDir 下，同一个文件系统，原子且不拷字节。
	metas := make([]partitionMeta, 0, len(sm.Partitions))
	for i, m := range sm.Partitions {
		dst := partitionPath(names.sortedBase, i)
		if err := os.Rename(byName[m.Path], dst); err != nil {
			return fail("分区 %s 落位: %v", m.Path, err)
		}
		if src, ok := byName[filepath.Base(sparseIndexPath(m.Path))]; ok {
			if err := os.Rename(src, sparseIndexPath(dst)); err != nil {
				// 索引可有可无：装载时读不到就扫描重建，只是慢。
				util.EPrintf("[SNAPSHOT] 分区 %s 的旁挂索引落位失败（启动时会扫描重建）: %v", dst, err)
			}
		}
		m.Path = filepath.Base(dst)
		metas = append(metas, m)
	}

	// 2) 日志落位。
	if err := os.Rename(byName[sm.CurrentLog], names.logPath); err != nil {
		return fail("日志落位: %v", err)
	}

	// 3) 新建一个库并 ingest 导出。库必须是新的：ingest 是"加进去当最新的数据"，
	//    并进本节点原来那个库会让旧历史里的 key 残留下来——它们的偏移指向一个已经
	//    不是当前日志的文件，读出来是别的记录的 value，而且不报错。
	// 先删掉这个路径上可能残留的库。名字里带位点，所以同一份快照重试时会落到同一个路径上，
	// 而**上一次没走完的安装**已经往那里 ingest 过一部分行了。直接 Init 会打开那个旧库，
	// 于是上一次的残留行留了下来——它们的偏移指向一个已经不是当前日志的文件，读出来是
	// 别的记录的 value，而且不报错。场景 F（安装到一半崩）走的正是这条路。
	if err := os.RemoveAll(names.storePath); err != nil {
		return fail("清理上一次未完成安装留下的库 %s: %v", names.storePath, err)
	}
	store := &raft.Persister{}
	if _, err := store.Init(names.storePath, true); err != nil {
		return fail("建新库 %s: %v", names.storePath, err)
	}
	if sm.StoreSST != "" {
		if err := store.IngestSSTables([]string{byName[sm.StoreSST]}); err != nil {
			store.Close()
			return fail("ingest 存储引擎导出: %v", err)
		}
	} else {
		// leader 那边库里一行都没有（上一轮 GC 之后没写过）。applied 仍要落一笔，
		// 否则重启时 applied=0 与日志基址矛盾，恢复直接放弃。
		store.SetApplied(sm.AppliedIndex)
	}
	if got, ok, err := store.GetApplied(); err != nil || !ok || got != sm.AppliedIndex {
		store.Close()
		return fail("ingest 之后库里的 applied=%d（ok=%v err=%v），清单说的是 %d",
			got, ok, err, sm.AppliedIndex)
	}

	// 4) 重建分区组。放在写状态文件之前：装不起来就整份作废，本节点仍是原来的状态。
	newParts, err := kvs.loadPartitionSet(names.sortedBase, metas)
	if err != nil {
		store.Close()
		return fail("装载快照的分区组: %v", err)
	}

	// 5) 换掉内存里的状态机指针，再让 Raft 接上新日志。
	kvs.mu.Lock()
	kvs.persister = store
	kvs.currentDBPath = names.storePath
	kvs.currentLog = names.logPath
	kvs.numGC = sm.NumGC
	kvs.lastAppliedIndex = sm.AppliedIndex
	if len(metas) > 0 {
		kvs.sortedFilePath = names.sortedBase
		kvs.retirePartitions(newParts)
		// 与 recoverOrInit 同一套读路径开关：有分区就意味着 GC 至少完成过一轮。
		switch {
		case sm.NumGC >= 2:
			kvs.anotherSortedFilePath = names.sortedBase
			kvs.anotherPartitions = newParts
		default:
			kvs.firstSortedFilePath = names.sortedBase
			kvs.firstPartitions = newParts
		}
		kvs.FirstGC = false
		kvs.startGC = true
		kvs.lastGCFinish = true
	}
	kvs.raft.SetCurrentPersister(store)
	kvs.mu.Unlock()

	last, err := kvs.raft.InstallSnapshotState(
		sm.LastIncludedIndex, sm.LastIncludedTerm,
		raft.LogFile{Path: names.logPath, Version: int32(sm.NumGC)}, sm.AppliedIndex)
	if err != nil {
		// 这里已经把指针换过去了，回滚不回来（旧库的句柄还在，但 Raft 的日志状态是半的）。
		// 与其带着一个半装好的状态继续服务，不如停下：下一次启动会从状态文件恢复，
		// 而状态文件还没写，所以恢复出来的是**原来**的状态，leader 会重发。
		log.Fatalf("[SNAPSHOT] 接上快照的日志失败，状态已半换、无法回滚: %v", err)
	}

	// 6) 最后写状态文件——这一步之前崩，等于这份快照没装过。
	snapshotInstallPauseWindow()
	kvs.mu.Lock()
	kvs.saveKVState()
	kvs.mu.Unlock()

	// 7) 旧的东西：分区组已经进退役队列（引用归零才删文件），库与日志要等在途的读者。
	//
	// **读者不再被 stateMu 挡在外面了**（那个"没人再持有它们"的旧说法随之失效）：
	// 一次读在 captureState 里取下 persister 与 currentLog 之后就放掉了 stateMu，
	// 之后整段读只持 storeRetireMu 的读锁。所以这里要取它的写锁——
	// 而 apply **一概不碰 storeRetireMu**，于是等读者的是这条回收路径，不是写入路径。
	//
	// 不设上限地等：一次范围扫描的时长与结果大小成正比（4GB 下 33 秒），
	// 而这几行只是删文件，晚几十秒毫无影响——状态文件已经写完，本节点在功能上
	// 已经装好了这份快照。**不能用 TryLock 退回**：连续扫描下会把回收永久饿死，
	// 盘上于此积压一份被取代的库，而那比晚删几十秒糟得多。
	// 等超过一次扫描的量级就说一声，好让"盘上为什么多一份库"有线索。
	kvs.reapPartitions()
	retireStart := time.Now()
	kvs.storeRetireMu.Lock()
	if waited := time.Since(retireStart); waited > 5*time.Second {
		fmt.Printf("[SNAPSHOT] 等在途的读者放开旧库等了 %v（很可能有一次长范围扫描）\n",
			waited.Round(time.Millisecond))
	}
	if prevStore != "" && prevStore != names.storePath {
		if err := os.RemoveAll(prevStore); err != nil {
			util.EPrintf("[SNAPSHOT] 删除被替换的库 %s 失败: %v", prevStore, err)
		}
	}
	if prevLog != "" && prevLog != names.logPath {
		if err := os.Remove(prevLog); err != nil && !os.IsNotExist(err) {
			util.EPrintf("[SNAPSHOT] 删除被替换的日志 %s 失败: %v", prevLog, err)
		}
	}
	kvs.storeRetireMu.Unlock()
	fmt.Printf("[SNAPSHOT] 装好一份：位点=(%d,%d) applied=%d 日志到=%d 分区=%d(%dB) "+
		"库行数=%d 耗时=%v（原状态：分区=%d 日志=%s）\n",
		sm.LastIncludedIndex, sm.LastIncludedTerm, sm.AppliedIndex, last,
		newParts.Len(), newParts.TotalSize(), sm.StoreRows, time.Since(t0),
		prevPartitions.Len(), filepath.Base(prevLog))
	return last, raftrpc.InstallSSTableStatus_INGESTED
}

func (kvs *KVServer) appliedIndexNow() int {
	kvs.mu.Lock()
	defer kvs.mu.Unlock()
	return kvs.lastAppliedIndex
}

func readSnapshotManifest(path string) (snapshotManifest, error) {
	var sm snapshotManifest
	if path == "" {
		return sm, fmt.Errorf("快照里没有 %s", snapshotManifestName)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return sm, err
	}
	if err := json.Unmarshal(data, &sm); err != nil {
		return sm, fmt.Errorf("parse %s: %v", snapshotManifestName, err)
	}
	if sm.CurrentLog == "" {
		return sm, fmt.Errorf("快照清单没有指明日志文件")
	}
	if sm.AppliedIndex > sm.LastIndex {
		return sm, fmt.Errorf("快照清单自相矛盾：applied=%d 超过日志末尾 %d", sm.AppliedIndex, sm.LastIndex)
	}
	if sm.LastIncludedIndex > sm.AppliedIndex {
		return sm, fmt.Errorf("快照清单自相矛盾：位点 %d 超过 applied %d", sm.LastIncludedIndex, sm.AppliedIndex)
	}
	return sm, nil
}

func checkSnapshotFiles(sm snapshotManifest, byName map[string]string) error {
	if byName[sm.CurrentLog] == "" {
		return fmt.Errorf("快照缺日志文件 %s", sm.CurrentLog)
	}
	if sm.StoreSST != "" && byName[sm.StoreSST] == "" {
		return fmt.Errorf("快照缺存储引擎导出 %s", sm.StoreSST)
	}
	for _, m := range sm.Partitions {
		p := byName[m.Path]
		if p == "" {
			return fmt.Errorf("快照缺分区 %s", m.Path)
		}
		st, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("快照的分区 %s: %v", m.Path, err)
		}
		// 长度对不上意味着没收全或者收错了。让它在这里失败，而不是等 loadPartitionSet
		// 去撞一个说不出哪里不对的 "unexpected EOF"。
		if st.Size() != m.Size {
			return fmt.Errorf("快照的分区 %s 清单说 %d 字节，收到 %d 字节", m.Path, m.Size, st.Size())
		}
	}
	return nil
}

// installPayload 是本节点唯一的 SSTableInstaller，按载荷类型分派。
//
// 一个入口而不是两个：传输只有一条，SetSSTableInstaller 也只记一个回调。分派放在这里
// 而不是让传输层去认类型，是因为"收到之后做什么"本来就是状态机的决定——传输层只搬字节、
// 只管任期。
func (kvs *KVServer) installPayload(span raft.SSTableSpan) (int, raftrpc.InstallSSTableStatus) {
	switch span.Kind {
	case raftrpc.InstallSSTableKind_SNAPSHOT:
		return kvs.installSnapshot(span)
	case raftrpc.InstallSSTableKind_SPAN:
		if kvs.lsm == nil {
			// 只有 LSM-Raft 基线会发 span。不是基线却收到一个，说明对端跑的是另一个
			// 系统配置——按失败上报，不要把它当快照装上。
			util.EPrintf("[SNAPSHOT] 收到 span 载荷，但本节点没有启用 LSM-Raft 基线")
			return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_FAILED
		}
		return kvs.lsmInstall(span)
	default:
		util.EPrintf("[SNAPSHOT] 未知的载荷类型 %v", span.Kind)
		return kvs.appliedIndexNow(), raftrpc.InstallSSTableStatus_FAILED
	}
}

// snapshotForRaft 是 Raft 侧的 SnapshotSource：把 createSnapshot 的产物翻译成
// Raft 认识的形状，并把"现在做不了"翻译成 raft.ErrSnapshotUnavailable。
//
// 为什么要翻译这一层而不是让 Raft 直接认识 kvstore 的类型：Raft 不该知道什么是分区、
// 什么是存储引擎导出。它只需要"位点 + 一串文件 + 一个释放函数"。
func (kvs *KVServer) snapshotForRaft() (raft.SnapshotPayload, error) {
	snap, err := kvs.createSnapshot()
	if errors.Is(err, errSnapshotBusy) {
		return raft.SnapshotPayload{}, raft.ErrSnapshotUnavailable
	}
	if err != nil {
		return raft.SnapshotPayload{}, err
	}
	return raft.SnapshotPayload{
		LastIncludedIndex: snap.manifest.LastIncludedIndex,
		LastIncludedTerm:  snap.manifest.LastIncludedTerm,
		LastIndex:         snap.manifest.LastIndex,
		Files:             snap.files,
		Release:           snap.release,
	}, nil
}

// snapshotInstallPauseWindow 是崩溃恢复的测试钩子：设了 NEZHA_SNAP_INSTALL_PAUSE_MS 时，
// 安装在"文件都已落位、状态文件尚未写"处停住，给外部脚本 kill -9 的机会。生产上不设。
//
// 这个窗口必须靠钩子才进得去：一次安装只有几十毫秒（小规模实测 30ms），从外面轮询
// 根本抓不到。传输那个窗口不需要钩子——把 -snapshotRateMB 调到 1 就能把它拉长到十几秒，
// 那是生产旋钮而不是测试开关，能少一个钩子就少一个。
func snapshotInstallPauseWindow() {
	pauseFor("NEZHA_SNAP_INSTALL_PAUSE_MS", "snapshot files in place; state file not written")
}

// clearOrphanSnapshotArtifacts 删掉不被当前状态引用的快照产物。
//
// 安装留下的文件名里带位点（RaftState_snap<idx>.log 等），刻意不覆盖本节点自己的文件——
// 这正是"状态文件最后写"能成为原子开关的前提。代价是一次没走完的安装会留下孤儿文件：
// 状态文件还指着旧状态，那批文件谁都不引用，却一直占着盘。
//
// 判据是"当前状态文件引用了它吗"，而不是"名字里有没有 snap"：安装**成功**之后，
// 当前状态引用的恰恰就是这批带 snap 的文件，按名字清理会把正在用的数据删掉。
func (kvs *KVServer) clearOrphanSnapshotArtifacts(st kvState) {
	keep := map[string]bool{
		filepath.Base(st.CurrentLog): true,
		filepath.Base(st.OldLog):     true,
		filepath.Base(st.CurrentDB):  true,
		filepath.Base(st.OldDB):      true,
	}
	for _, m := range st.Partitions {
		keep[filepath.Base(m.Path)] = true
		keep[filepath.Base(sparseIndexPath(m.Path))] = true
	}
	var removed int
	globs := []string{
		filepath.Join(kvs.dataDir, "data", "valuelog", "*snap*"),
		filepath.Join(kvs.dataDir, "data", "valuelog", sparseIndexDir, "*snap*"),
		filepath.Join(kvs.dataDir, "data", "dbfile", "*snap*"),
	}
	for _, g := range globs {
		matches, err := filepath.Glob(g)
		if err != nil {
			continue
		}
		for _, p := range matches {
			if keep[filepath.Base(p)] {
				continue
			}
			if err := os.RemoveAll(p); err != nil {
				fmt.Printf("[SNAPSHOT] 删除孤儿产物 %s 失败: %v\n", p, err)
				continue
			}
			removed++
		}
	}
	if removed > 0 {
		fmt.Printf("[SNAPSHOT] 清理上次未完成安装留下的孤儿产物 %d 个\n", removed)
	}
}
