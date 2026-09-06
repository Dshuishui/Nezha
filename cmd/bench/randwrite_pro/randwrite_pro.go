package main

import (
	"flag"
	"fmt"
	"math/rand"
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
	goodPut    int
	avgLatency time.Duration
	throughput float64
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

	allKeys := generateUniqueRandomInts(0, 39062)
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
				reply, err := kvc.c.Put(key, value)
				if err == nil && reply != nil && reply.Err != "defeat" {
					localResult.goodPut++
				}
			}
			totalLatency := time.Since(startTime)

			if localResult.goodPut > 0 {
				localResult.avgLatency = totalLatency / time.Duration(localResult.goodPut)
				localDataSize := float64(localResult.goodPut*len(value)) / 1000000 // MB
				localResult.throughput = localDataSize / totalLatency.Seconds()
			}

			results <- localResult
		}(i)
	}

	// go func() {
	wg.Wait()
	close(results)
	// }()

	var totalGoodPut int
	var totalThroughput float64
	var totalAvgLatency time.Duration
	goroutineCount := 0

	for result := range results {
		totalGoodPut += result.goodPut
		if result.goodPut > 0 {
			totalThroughput += result.throughput
			totalAvgLatency += result.avgLatency
			goroutineCount++
		}
	}

	kvc.goodPut = totalGoodPut
	avgThroughput := totalThroughput / float64(goroutineCount)
	avgLatency := totalAvgLatency / time.Duration(goroutineCount)

	kvc.c.Close()

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
