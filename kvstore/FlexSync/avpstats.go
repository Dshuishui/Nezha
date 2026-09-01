package main

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

// avpRecordNotFound 标记一次未命中其实是"这个 key 从没写入过"。
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
