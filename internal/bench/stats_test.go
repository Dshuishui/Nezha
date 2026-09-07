package bench

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// 分位数用最近秩（nearest rank）：返回的必定是真实测到过的样本。
// 用插值的话，小样本下 p999 会落在最后两个样本之间，报出一个从未发生过的延迟。
func TestPercentileIsNearestRank(t *testing.T) {
	var l Latencies
	for i := 1; i <= 100; i++ { // 1ms..100ms
		l.Add(time.Duration(i) * time.Millisecond)
	}
	s := l.Stats()
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"p50", s.P50, 50 * time.Millisecond},
		{"p90", s.P90, 90 * time.Millisecond},
		{"p95", s.P95, 95 * time.Millisecond},
		{"p99", s.P99, 99 * time.Millisecond},
		{"p999", s.P999, 100 * time.Millisecond}, // 向上取整落到最后一个样本
		{"min", s.Min, 1 * time.Millisecond},
		{"max", s.Max, 100 * time.Millisecond},
		{"mean", s.Mean, 50500 * time.Microsecond}, // (1+..+100)/100 = 50.5ms
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if s.N != 100 {
		t.Errorf("N = %d, want 100", s.N)
	}
}

// 尾部是这套指标存在的理由：几个慢请求必须体现在 p99 上，而平均值几乎不动。
func TestTailShowsUpInPercentilesNotInMean(t *testing.T) {
	// 985/15 而不是 990/10：后者的慢样本正好占满 1%，p99 会落在最后一个快样本上
	// （最近秩下这是正确结果，只是让断言变得没有意义）。
	var l Latencies
	for i := 0; i < 985; i++ {
		l.Add(time.Millisecond)
	}
	for i := 0; i < 15; i++ {
		l.Add(60 * time.Second) // 撞上超时的那几个
	}
	s := l.Stats()
	if s.Mean > 2*time.Second {
		t.Errorf("mean = %v，平均值本该被 98.5%% 的快请求摊平——这正是它掩盖尾部的方式", s.Mean)
	}
	if s.P50 != time.Millisecond {
		t.Errorf("p50 = %v，尾部不该影响中位数", s.P50)
	}
	if s.P99 < time.Second {
		t.Errorf("p99 = %v，1%% 的超时必须体现在 p99 上", s.P99)
	}
	if s.Max != 60*time.Second {
		t.Errorf("max = %v, want 60s", s.Max)
	}
}

func TestEmptyStatsAreZero(t *testing.T) {
	var l Latencies
	s := l.Stats()
	if s.N != 0 || s.P50 != 0 || s.Max != 0 {
		t.Fatalf("空样本应当全零，得到 %+v", s)
	}
	if !strings.Contains(s.Line("GET"), "n=0") {
		t.Fatalf("空样本的输出行: %s", s.Line("GET"))
	}
}

func TestConcurrentCollection(t *testing.T) {
	var l Latencies
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			batch := make([]time.Duration, 0, 100)
			for i := 0; i < 100; i++ {
				batch = append(batch, time.Millisecond)
			}
			l.Append(batch)
		}()
	}
	wg.Wait()
	if got := l.Len(); got != 800 {
		t.Fatalf("collected %d samples, want 800", got)
	}
}

// 输出行是给采集脚本解析的，字段名和顺序变了会静默地把 CSV 弄空。
func TestLinesCarryEveryField(t *testing.T) {
	var l Latencies
	l.Add(2 * time.Millisecond)
	line := l.Stats().Line("SCAN")
	for _, f := range []string{"op=SCAN", "n=1", "mean=", "p50=", "p90=", "p95=", "p99=", "p999=", "min=", "max="} {
		if !strings.Contains(line, f) {
			t.Errorf("缺字段 %q: %s", f, line)
		}
	}
	tp := ThroughputLine("SCAN", 100, 1_000_000, 2*time.Second)
	for _, f := range []string{"op=SCAN", "ops=100", "bytes=1000000", "elapsed=2.000s", "ops_per_s=50.0", "mb_per_s=0.5000"} {
		if !strings.Contains(tp, f) {
			t.Errorf("缺字段 %q: %s", f, tp)
		}
	}
}
