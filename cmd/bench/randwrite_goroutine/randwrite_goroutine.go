package main

import (
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/client"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
)

var (
	ser = flag.String("servers", "", "the Server, Client Connects to")
	// mode     = flag.String("mode", "RequestRatio", "Read or Put and so on")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	// getratio = flag.Int("getratio", 1, "Get Times per Put Times")
	vsize = flag.Int("vsize", 64, "value size in type")
)

type KVClient struct {
	Kvservers []string
	c         *client.Client

	goodPut int // 有效吞吐量
	// totalLatency time.Duration // 添加总延迟字段
}

type putResult struct {
	goodPut       int
	avgLatency    time.Duration
	totalLatency  time.Duration
	localDataSize float64 // MB
	// 逐请求延迟。平均值会把尾部藏起来：实测一轮里几个 60 秒超时能把总耗时
	// 拉长 300 多秒（吞吐从 0.119 掉到 0.074），而平均延迟只动了 11%。
	// 吞吐由最慢的 goroutine 决定，因此它反映的是撞上几次超时，而非系统快慢。
	// 要看清这件事，必须保留分布。
	latencies []time.Duration
}

// func (kvc *KVClient) batchRawPut(value string) {
//     wg := sync.WaitGroup{}
//     base := *dnums / *cnums
//     wg.Add(*cnums)

//     // Create a channel to collect results from goroutines
//     resultChan := make(chan int, *cnums)

//     for i := 0; i < *cnums; i++ {
//         go func(i int) {
//             defer wg.Done()
//             localGoodPut := 0
//             rand.Seed(time.Now().UnixNano())
//             for j := 0; j < base; j++ {
//                 key := util.GenerateFixedSizeKey(5)
//                 reply, err := kvc.c.Put(key, value)
//                 if err == nil && reply != nil && reply.Err != "defeat" {
//                     localGoodPut++
//                 }
//             }
//             // Send the local result to the channel
//             resultChan <- localGoodPut
//         }(i)
//     }

//     // Close the result channel when all goroutines are done
//     go func() {
//         wg.Wait()
//         close(resultChan)
//     }()

//     // Collect and sum up the results
//     totalGoodPut := 0
//     for localGoodPut := range resultChan {
//         totalGoodPut += localGoodPut
//     }

//     kvc.goodPut = totalGoodPut

//     for _, pool := range kvc.pools {
//         pool.Close()
//         util.DPrintf("The raft pool has been closed")
//     }
// }

// batchRawPut blinds put bench.

func (kvc *KVClient) batchRawPut(value string) (float64, time.Duration) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0

	allKeys := generateUniqueRandomInts(0, *dnums-1)
	results := make(chan putResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			localResult := putResult{}

			start := i * base
			end := (i + 1) * base
			if i == *cnums-1 {
				end = *dnums
			}
			keys := allKeys[start:end]

			startTime := time.Now()
			for j := 0; j < len(keys); j++ {
				key := strconv.Itoa(keys[j])
				reqStart := time.Now()
				reply, err := kvc.c.Put(key, value)
				// 失败的请求也要记：它们正是尾部
				localResult.latencies = append(localResult.latencies, time.Since(reqStart))
				if err == nil && reply != nil && reply.Err != "defeat" {
					localResult.goodPut++
				}
			}
			localResult.totalLatency = time.Since(startTime)

			if localResult.goodPut > 0 {
				localResult.avgLatency = localResult.totalLatency / time.Duration(localResult.goodPut)
				localResult.localDataSize = float64(localResult.goodPut*len(value)) / 1000000 // MB
			}

			results <- localResult
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var allLatencies []time.Duration
	var totalGoodPut int
	var totalDataSize float64
	var totalAvgLatency time.Duration
	var maxTotalLatency time.Duration
	var avgLatency time.Duration

	goroutineCount := 0

	for result := range results {
		totalGoodPut += result.goodPut
		totalDataSize += result.localDataSize
		allLatencies = append(allLatencies, result.latencies...)
		totalAvgLatency += result.avgLatency
		if result.totalLatency > maxTotalLatency {
			maxTotalLatency = result.totalLatency
		}
		if result.goodPut > 0 {
			goroutineCount++
		}
	}

	kvc.goodPut = totalGoodPut
	avgThroughput := totalDataSize / maxTotalLatency.Seconds()
	if goroutineCount != 0 {
		avgLatency = totalAvgLatency / time.Duration(goroutineCount)
	}
	// avgLatency := totalAvgLatency / time.Duration(goroutineCount)

	kvc.c.Close()

	// 百分位。平均值掩盖尾部：几个 60 秒超时能把总耗时拉长数百秒、吞吐腰斩，
	// 而平均延迟几乎不动——它们被摊进二十万个请求里。吞吐由最慢的 goroutine
	// 决定，所以它测的是本轮撞上几次超时，不是系统快慢。
	if len(allLatencies) > 0 {
		sort.Slice(allLatencies, func(i, j int) bool { return allLatencies[i] < allLatencies[j] })
		pct := func(p float64) time.Duration {
			return allLatencies[int(float64(len(allLatencies)-1)*p)]
		}
		ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
		fmt.Printf("latency percentiles: p50=%.3fms p90=%.3fms p99=%.3fms p999=%.3fms max=%.3fms samples=%d\n",
			ms(pct(0.50)), ms(pct(0.90)), ms(pct(0.99)), ms(pct(0.999)),
			ms(allLatencies[len(allLatencies)-1]), len(allLatencies))
	}
	return avgThroughput, avgLatency
}

func generateUniqueRandomInts(min, max int) []int {
	nums := make([]int, max-min+1)
	for i := range nums {
		nums[i] = min + i
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return nums
}

// InitPool builds the shared cluster client (one connection pool per server).
func (kvc *KVClient) InitPool() {
	kvc.c = client.MustNew(kvc.Kvservers, client.Options{})
}

func main() {
	flag.Parse()
	valueSize := *vsize
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers

	value := util.GenerateLargeValue(valueSize)
	kvc.InitPool()

	startTime := time.Now()
	avgThroughput, avgLatency := kvc.batchRawPut(value)
	elapsedTime := time.Since(startTime)

	sum_Size_MB := float64(kvc.goodPut*valueSize) / 1000000

	fmt.Printf("\nelapse:%v, throughput:%.4fMB/S, avg latency:%v, total %v, goodPut %v, value %v, client %v, Size %.2fMB\n",
		elapsedTime, avgThroughput, avgLatency, *dnums, kvc.goodPut, *vsize, *cnums, sum_Size_MB)
}
