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
	notFound       atomic.Uint64 // 其中 key 根本不存在的部分
	blockScans     atomic.Uint64 // 块扫描次数（每次未命中一次）
	entriesScanned atomic.Uint64 // 块内顺序解析的 entry 总条数
	bytesRead      atomic.Uint64 // 从 sortedFile 实际读取的字节数
}

func avpRecordHit()  { avpStats.inlineHits.Add(1) }
func avpRecordMiss() { avpStats.inlineMisses.Add(1) }

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
// reads 到底数的是什么，用之前要清楚，否则命中率会被读成比实际更低的数。
//
// GET 是**多路并发**查找：当前 valuelog 与分区文件同时查，结果按优先级取
// （read.go 的 anotherGCGet）。两个 goroutine 无论如何都会跑完，所以每次 GET
// 恰好产生一次分区查找、也就是恰好一次 hit 或 miss——分母是对的，等于 GET 次数。
//
// 但分子不是。一次"答案来自当前 valuelog"的 GET（刚写过的 key）同样会记一次
// **miss**，而内联缓存对这种读本来就无能为力：它只缓存已经搬进分区的小值。
// 于是 hit_rate 把"缓存没能服务的读"和"缓存本可服务却没命中的读"算在了一起，
// 系统性地压低 AVP 的收益。这与 not_found 被单独剥出来是同一类问题
// （见 avpRecordNotFound 的说明），只是还没有剥。
//
// 要剥需要在汇合点知道"这一次是哪条路答的"，而那会改变一个已经用来出过数的指标，
// 所以没有顺手改。**写进论文前必须先决定**：要么剥出来重测，要么明确说明
// hit_rate 的分母是全部 GET、而非"内联缓存有机会服务的那些 GET"。
// 写满覆盖（overwrite）比例越高，这个差距越大。
func AVPStatsLine() string {
	h := avpStats.inlineHits.Load()
	m := avpStats.inlineMisses.Load()
	scans := avpStats.blockScans.Load()
	ents := avpStats.entriesScanned.Load()
	bytes := avpStats.bytesRead.Load()

	nf := avpStats.notFound.Load()

	total := h + m
	var hitRate float64
	if total > 0 {
		hitRate = float64(h) / float64(total) * 100
	}
	// 有效命中率：只在"确实存在的 key"上算。这才是 AVP 的真实度量，
	// 原始命中率会随键空间与写入量的比例漂移。
	effective := total - nf
	var effRate float64
	if effective > 0 {
		effRate = float64(h) / float64(effective) * 100
	}
	var entsPerScan float64
	if scans > 0 {
		entsPerScan = float64(ents) / float64(scans)
	}
	return fmt.Sprintf(
		"[AVP-STATS] reads=%d hits=%d misses=%d not_found=%d hit_rate=%.2f%% eff_hit_rate=%.2f%% block_scans=%d entries_scanned=%d entries_per_scan=%.1f bytes_read=%d",
		total, h, m, nf, hitRate, effRate, scans, ents, entsPerScan, bytes)
}

// StartAVPStatsReporter 周期性把指标打进节点日志。
// 放在后台而不是每次查询后输出：热路径上多一次格式化就足以污染延迟测量。
func StartAVPStatsReporter(interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		for range time.Tick(interval) {
			if avpStats.inlineHits.Load()+avpStats.inlineMisses.Load() > 0 {
				fmt.Println(AVPStatsLine())
			}
		}
	}()
}
