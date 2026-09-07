// Package bench holds what the benchmark tools report, so PUT, GET and SCAN describe a
// run the same way and one harness can parse all three.
//
// 论文口径报延迟分布，不报平均值加吞吐：平均值掩盖尾部（几个超时请求摊进二十万个
// 请求里几乎不动分毫），而吞吐由最慢的那个 goroutine 决定，测的是本轮撞上几次超时、
// 不是系统快慢——PUT 吞吐曾因此出现 60% 的轮间方差，而 p50/p99 从头到尾没动过。
package bench

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Latencies collects one sample per request. Safe for concurrent use; workers that keep a
// private slice should hand it over with Append once, rather than locking per request.
type Latencies struct {
	mu sync.Mutex
	d  []time.Duration
}

// Add records one sample.
func (l *Latencies) Add(d time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, d)
	l.mu.Unlock()
}

// Append records a worker's private batch of samples.
func (l *Latencies) Append(ds []time.Duration) {
	l.mu.Lock()
	l.d = append(l.d, ds...)
	l.mu.Unlock()
}

// Len is the number of samples collected.
func (l *Latencies) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.d)
}

// Stats is the distribution of one operation's latency.
type Stats struct {
	N                              int
	Mean, P50, P90, P95, P99, P999 time.Duration
	Min, Max                       time.Duration
}

// Stats sorts the samples and summarises them. Zero samples give a zero Stats.
func (l *Latencies) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.d) == 0 {
		return Stats{}
	}
	sort.Slice(l.d, func(i, j int) bool { return l.d[i] < l.d[j] })
	var sum time.Duration
	for _, d := range l.d {
		sum += d
	}
	return Stats{
		N:    len(l.d),
		Mean: sum / time.Duration(len(l.d)),
		P50:  percentile(l.d, 0.50),
		P90:  percentile(l.d, 0.90),
		P95:  percentile(l.d, 0.95),
		P99:  percentile(l.d, 0.99),
		P999: percentile(l.d, 0.999),
		Min:  l.d[0],
		Max:  l.d[len(l.d)-1],
	}
}

// percentile returns the nearest-rank percentile of an already sorted slice: the smallest
// sample at or above p of the distribution. Nearest rank always returns a sample that was
// actually measured, which matters at p999 on small runs where interpolation would invent
// a value between the last two samples.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(float64(len(sorted))*p + 0.9999999) // ceil, without importing math
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// Line reports the distribution on one machine-parsable line. op names the operation
// (PUT/GET/SCAN) so a harness can tell three runs of one node apart.
func (s Stats) Line(op string) string {
	return fmt.Sprintf("[LATENCY] op=%s n=%d mean=%.3fms p50=%.3fms p90=%.3fms p95=%.3fms p99=%.3fms p999=%.3fms min=%.3fms max=%.3fms",
		op, s.N, ms(s.Mean), ms(s.P50), ms(s.P90), ms(s.P95), ms(s.P99), ms(s.P999), ms(s.Min), ms(s.Max))
}

// ThroughputLine reports rate on its own line, in both units the tools have historically
// used: operations for the request rate, megabytes for the data rate.
//
// ops is the number of successful operations, bytes the payload they moved, and elapsed
// the wall time of the measured phase.
func ThroughputLine(op string, ops int, bytes int64, elapsed time.Duration) string {
	secs := elapsed.Seconds()
	if secs <= 0 {
		secs = 1e-9
	}
	return fmt.Sprintf("[THROUGHPUT] op=%s ops=%d bytes=%d elapsed=%.3fs ops_per_s=%.1f mb_per_s=%.4f",
		op, ops, bytes, secs, float64(ops)/secs, float64(bytes)/1e6/secs)
}
