package kvstore

// AVP placement 的可视化。
//
// 可插拔：默认不启动，只有显式给出 -vizAddr 才监听端口。热路径上新增的开销
// 仅一次 atomic 加法加一次桶下标计算（约 1ns，相对 PUT 的十几毫秒可忽略），
// 且只在 inlinePlacement 开启时才发生——其余被测系统根本不走这条分支。
//
// 为什么值得单独做这个页面：吞吐和延迟曲线用 Prometheus + Grafana 就够了，
// 但"哪些 value 进了存储引擎、哪些留在 valuelog、阈值定在哪里合适"是 AVP
// 特有的机制，通用监控画不出来，而它恰好是调 inlineThreshold 时最需要看的东西。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
)

// placementBuckets 按 value 大小分桶。边界取 2 的幂，覆盖从小值到论文用的
// 16KB 量级；inlineThreshold 落在哪一格，页面上会标出来。
// 数组而非切片：下面的计数器需要以它的长度做数组尺寸，那必须是编译期常量。
var placementBuckets = [...]int{64, 128, 256, 512, 1024, 4096, 16384}

// numPlacementBuckets 比边界数多一格，用于装"大于等于最后一个边界"的那些。
const numPlacementBuckets = len(placementBuckets) + 1

func bucketLabel(i int) string {
	if i == 0 {
		return fmt.Sprintf("<%dB", placementBuckets[0])
	}
	if i == len(placementBuckets) {
		return fmt.Sprintf(">=%dB", placementBuckets[len(placementBuckets)-1])
	}
	return fmt.Sprintf("%d-%dB", placementBuckets[i-1], placementBuckets[i])
}

func bucketIndex(size int) int {
	for i, b := range placementBuckets {
		if size < b {
			return i
		}
	}
	return len(placementBuckets)
}

var placementStats struct {
	inlined  [numPlacementBuckets]atomic.Uint64 // 落进存储引擎的
	external [numPlacementBuckets]atomic.Uint64 // 留在 valuelog 的
}

// recordPlacement 记一次放置决策。调用点在 applyLoop 的写路径分流处。
func recordPlacement(size int, inlined bool) {
	i := bucketIndex(size)
	if inlined {
		placementStats.inlined[i].Add(1)
	} else {
		placementStats.external[i].Add(1)
	}
}

type vizBucket struct {
	Label    string `json:"label"`
	Inlined  uint64 `json:"inlined"`
	External uint64 `json:"external"`
}

type vizPayload struct {
	System          string      `json:"system"`
	InlineThreshold int         `json:"inlineThreshold"`
	Buckets         []vizBucket `json:"buckets"`
	Reads           uint64      `json:"reads"`
	Hits            uint64      `json:"hits"`
	Misses          uint64      `json:"misses"`
	NotFound        uint64      `json:"notFound"`
	HitRate         float64     `json:"hitRate"`
	EffHitRate      float64     `json:"effHitRate"`
	EntriesPerScan  float64     `json:"entriesPerScan"`
	BytesRead       uint64      `json:"bytesRead"`
}

func collectViz(system string, threshold int) vizPayload {
	p := vizPayload{System: system, InlineThreshold: threshold}
	for i := range placementStats.inlined {
		p.Buckets = append(p.Buckets, vizBucket{
			Label:    bucketLabel(i),
			Inlined:  placementStats.inlined[i].Load(),
			External: placementStats.external[i].Load(),
		})
	}
	p.Hits = avpStats.inlineHits.Load()
	p.Misses = avpStats.inlineMisses.Load()
	p.NotFound = avpStats.notFound.Load()
	p.Reads = p.Hits + p.Misses
	if p.Reads > 0 {
		p.HitRate = float64(p.Hits) / float64(p.Reads) * 100
	}
	// 有效命中率剔除掉本就不存在的 key：读的键空间大于实际写入量时，
	// 那部分请求注定未命中，会把命中率系统性压低。
	if eff := p.Reads - p.NotFound; eff > 0 {
		p.EffHitRate = float64(p.Hits) / float64(eff) * 100
	}
	if scans := avpStats.blockScans.Load(); scans > 0 {
		p.EntriesPerScan = float64(avpStats.entriesScanned.Load()) / float64(scans)
	}
	p.BytesRead = avpStats.bytesRead.Load()
	return p
}

// StartAVPViz 启动可视化服务。addr 为空则什么也不做。
// 页面自包含，不引用任何外部资源——实验机器往往没有出站网络。
func StartAVPViz(addr, system string, threshold int) {
	if addr == "" {
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(collectViz(system, threshold))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(vizPage))
	})
	go func() {
		fmt.Printf("[AVP-VIZ] 可视化已启动: http://%s\n", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			// 不要中断实验：可视化失败只是少了一个观测手段。
			fmt.Printf("[AVP-VIZ] 启动失败（不影响实验）: %v\n", err)
		}
	}()
}
