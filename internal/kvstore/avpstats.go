package kvstore

import (
	"fmt"
	"sync/atomic"
	"time"
)

// AVP 的机理指标。
//
// 端到端延迟不足以证明 AVP 在起作用：实验机器内存远大于数据集时，sortedFile
// 整个落在 OS page cache 里，"关闭 AVP" 那一组读文件也不产生真实磁盘 I/O，
// 两组的延迟差会被抹平到只剩块内解析的 CPU 开销。
//
// 这些计数器直接记录 AVP 究竟省掉了什么——命中多少次、每次未命中要顺序解析
// 多少条 entry——不受 page cache 影响，是审稿人真正要看的证据。
// 热路径上只有 atomic 加法，开销可忽略。
var avpStats struct {
	inlineHits     atomic.Uint64 // 内联缓存命中：直接返回 value，零文件 I/O
	inlineMisses   atomic.Uint64 // 未命中：需要走 sortedFile
	notFound       atomic.Uint64 // 哪里都没找到这个 key（在 GetInRaft 那个唯一汇合点计）
	servedByLog    atomic.Uint64 // 由 valuelog 答复的读：内联缓存服务不了它，不进 hit/miss
	blockScans     atomic.Uint64 // 块扫描次数（每次未命中一次）
	entriesScanned atomic.Uint64 // 块内顺序解析的 entry 总条数
	bytesRead      atomic.Uint64 // 从 sortedFile 实际读取的字节数
}

// avpRecordPartitionRead 记一次**由分区文件答复**的读：内联缓存有机会服务它。
// hit 表示这一次是缓存接住的（省掉一次文件 seek）。
//
// 必须在汇合点调用，不能在 getFromSortedFile 里面：GET 是多路并发查找，那次查找的结果
// 可能压根没被采用（当前 valuelog 优先），而没被采用的查找不该算进命中率。
func avpRecordPartitionRead(hit bool) {
	if hit {
		avpStats.inlineHits.Add(1)
		return
	}
	avpStats.inlineMisses.Add(1)
}

// avpRecordServedByLog 记一次**由 valuelog 答复**的读（刚写过、还没被 GC 搬走的 key）。
// 它不进 hit/miss：内联缓存只装已经搬进分区的小值，对这种读本来就无能为力，
// 把它算成未命中等于用负载的新鲜度去压低 AVP 的收益。
func avpRecordServedByLog() { avpStats.servedByLog.Add(1) }

// avpRecordNotFound 标记一次读最终没有找到这个 key。
//
// 必须在 GetInRaft 这个唯一的汇合点调用，不能在各条查找分支里调用：
// GC 后数据分散在多个 sortedFile 与新旧 valuelog 中，一次读并发查这几处，
// 单条路径查不到是常态。早先埋在分支里时，这个数虚高到 37%——测的是分片
// 未命中，不是键缺失。
// 不把它从未命中里剥出来，命中率就会被负载配置系统性压低：读的键空间大于
// 实际写入量时，多出来的请求注定 miss，而压低多少取决于两者的比例——
// 换个数据规模，命中率就不可比了。
func avpRecordNotFound() { avpStats.notFound.Add(1) }
func avpRecordScan(entries int, bytes int64) {
	avpStats.blockScans.Add(1)
	avpStats.entriesScanned.Add(uint64(entries))
	avpStats.bytesRead.Add(uint64(bytes))
}

// AVPStatsLine 汇总成一行，供从节点日志里抓取。
// 命中率和"平均每次未命中解析多少条 entry"是两个核心指标：
// 前者说明 AVP 覆盖了多少读，后者量化它每次省下的解析工作量。
//
// **hit_rate 的分母是"内联缓存有机会服务的读"，不是全部 GET。**
//
// GET 是多路并发查找：当前 valuelog 与分区文件同时查，结果按优先级取
// （read.go 的 anotherGCGet）。两个 goroutine 无论如何都会跑完，所以此前每次 GET 都会
// 产生一次 hit 或 miss——包括那些**答案来自 valuelog** 的读（刚写过的 key）。
// 而内联缓存只装已经搬进分区的小值，对那种读本来就无能为力，把它算成未命中等于
// 用负载的新鲜度去压低 AVP 的收益，覆盖写比例越高压得越多。
//
// 现在计数搬到了汇合点，按"是哪条路答的"分流：
//
//	hits + misses  由**分区文件**答复的读 —— 缓存有机会服务，这才是命中率的分母
//	served_by_log  由 valuelog 答复的读   —— 缓存服务不了，单独记
//	not_found      哪里都没找到           —— 注定 miss，本来就该剔除
//
// **2026-09-17 改的口径。此前用旧口径（分母 = 全部 GET）出过的 hit_rate 与现在不可比**，
// 需要重测；旧口径下的数字系统性偏低。
func AVPStatsLine() string {
	h := avpStats.inlineHits.Load()
	m := avpStats.inlineMisses.Load()
	scans := avpStats.blockScans.Load()
	ents := avpStats.entriesScanned.Load()
	bytes := avpStats.bytesRead.Load()
	nf := avpStats.notFound.Load()
	sbl := avpStats.servedByLog.Load()

	served := h + m // 由分区答复的读
	var hitRate float64
	if served > 0 {
		hitRate = float64(h) / float64(served) * 100
	}
	var entsPerScan float64
	if scans > 0 {
		entsPerScan = float64(ents) / float64(scans)
	}
	return fmt.Sprintf(
		"[AVP-STATS] partition_reads=%d hits=%d misses=%d hit_rate=%.2f%% "+
			"served_by_log=%d not_found=%d block_scans=%d entries_scanned=%d "+
			"entries_per_scan=%.1f bytes_read=%d",
		served, h, m, hitRate, sbl, nf, scans, ents, entsPerScan, bytes)
}

// StartAVPStatsReporter 周期性把指标打进节点日志。
// 放在后台而不是每次查询后输出：热路径上多一次格式化就足以污染延迟测量。
func StartAVPStatsReporter(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		for range time.Tick(interval) {
			// 判据要含 servedByLog：一轮实验里若所有读都由 valuelog 答复
			// （GC 还没跑过），hits+misses 恒为 0，这一行就永远不打——
			// 而那恰好是最需要知道"缓存一次都没被用上"的时候。
			if avpStats.inlineHits.Load()+avpStats.inlineMisses.Load()+
				avpStats.servedByLog.Load() > 0 {
				fmt.Println(AVPStatsLine())
			}
		}
	}()
}
