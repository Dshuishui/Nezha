package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/bench"
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
	// keyspace>0 把本次跑从"装载"变成"覆盖写"：键从 [0, keyspace) 有放回地抽，
	// 而不是 0..dnums-1 的一个排列。装载阶段每个键只写一次、不产生任何垃圾，
	// 而写放大与空间放大在文献里量的是回收垃圾的代价（Scavenger+/HashKV/Titan
	// 都是"装载→update 制造垃圾→触发 GC→再测"），没有覆盖写就测不到那件事。
	keyspace = flag.Int("keyspace", 0, "overwrite mode: draw keys from [0,keyspace) with repetition (0 = unique load)")
	dist     = flag.String("dist", "zipf", "overwrite key distribution: zipf|uniform（仅 keyspace>0 时有效）")
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

	// 装载模式下键是 0..dnums-1 的排列（每键一次，零垃圾）；覆盖写模式下每个
	// goroutine 自己按分布抽键，见下方 keysFor。
	var allKeys []int
	if *keyspace <= 0 {
		allKeys = generateUniqueRandomInts(0, *dnums-1)
	}
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
			keys := keysFor(allKeys, start, end, i)

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
	//
	// 统计改用 internal/bench，与 GET/SCAN 同一套字段和同一种分位数定义，
	// 采集脚本才能用一个解析器读三种负载。
	var lat bench.Latencies
	lat.Append(allLatencies)
	fmt.Println(lat.Stats().Line("PUT"))
	fmt.Println(bench.ThroughputLine("PUT", totalGoodPut, int64(totalDataSize*1e6), maxTotalLatency))
	return avgThroughput, avgLatency
}

// keysFor 给第 i 个 goroutine 挑出它要写的键。
// 装载模式直接切排列；覆盖写模式按 -dist 抽样，Zipf 参数与 zipf_read 保持一致，
// 这样"写热点"与"读热点"是同一批键，垃圾的分布才与真实负载相符。
func keysFor(allKeys []int, start, end, worker int) []int {
	if *keyspace <= 0 {
		return allKeys[start:end]
	}
	n := end - start
	out := make([]int, n)
	rnd := rand.New(rand.NewSource(time.Now().UnixNano() + int64(worker)))
	if *dist == "uniform" {
		for j := range out {
			out[j] = rnd.Intn(*keyspace)
		}
		return out
	}
	z := rand.NewZipf(rnd, 1.01, 1, uint64(*keyspace-1))
	for j := range out {
		out[j] = int(z.Uint64())
	}
	return out
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
