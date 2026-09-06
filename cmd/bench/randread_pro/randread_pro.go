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
)

// 保留原有的标志定义
var (
	ser   = flag.String("servers", "", "the Server, Client Connects to")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	key   = flag.Int("key", 6, "target key")
)

type KVClient struct {
	c         *client.Client
	Kvservers []string

	goodPut   int // 有效吞吐量
	valuesize int
	// totalLatency time.Duration // 添加总延迟字段
}
type getResult struct {
	count         int
	avgLatency    time.Duration
	totalDataSize float64 // MB/s
	valueSize     int
	duration      time.Duration
}

func (kvc *KVClient) randRead() (float64, time.Duration) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0

	resultChan := make(chan getResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			localResult := getResult{}
			rand.Seed(time.Now().UnixNano())
			startTime := time.Now()
			for j := 0; j < base; j++ {
				key := rand.Intn(125000)
				targetkey := strconv.Itoa(key)
				value, keyExist, err := kvc.c.Get(targetkey)
				if err == nil && keyExist && value != "ErrNoKey" {
					localResult.count++
					localResult.valueSize = len([]byte(value))
				}
			}
			localResult.duration = time.Since(startTime)
			if localResult.count > 0 {
				localResult.avgLatency = localResult.duration / time.Duration(localResult.count)
				localResult.totalDataSize = float64(localResult.count*localResult.valueSize) / 1000000 // MB
				// localResult.throughput = totalDataSize / duration.Seconds()
			}
			resultChan <- localResult
		}(i)
	}
	// go func() {
	wg.Wait()
	close(resultChan)
	// }()

	var maxDuration time.Duration
	var totalData float64
	var totalAvgLatency time.Duration
	var totalCount int
	goroutineCount := 0

	for result := range resultChan {
		if result.count > 0 {
			if result.duration > maxDuration {
				maxDuration = result.duration
			}
			totalData += result.totalDataSize
			totalAvgLatency += result.avgLatency
			kvc.valuesize = result.valueSize
			goroutineCount++
		}
		totalCount += result.count
	}
	// fmt.Printf("此时，maxduration为%v,totalData:%v\n",maxDuration,totalData)

	kvc.goodPut = totalCount

	throughput := totalData / maxDuration.Seconds()
	avgLatency := totalAvgLatency / time.Duration(goroutineCount)

	kvc.c.Close()

	return throughput, avgLatency
}

func runTest() (float64, time.Duration) {
	flag.Parse()
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers

	kvc.InitPool()
	startTime := time.Now()
	throughput, averageLatency := kvc.randRead()

	elapsedTime := time.Since(startTime)
	sum_Size_MB := float64(kvc.goodPut*kvc.valuesize) / 1000000

	fmt.Printf("Elapse: %v, Throughput: %.4f MB/S, Total: %v, GoodPut: %v, Value: %v, Client: %v, Size: %.2f MB, Average Latency: %v\n",
		elapsedTime, throughput, *dnums, kvc.goodPut, kvc.valuesize, *cnums, sum_Size_MB, averageLatency)

	return throughput, averageLatency
}

// InitPool builds the shared cluster client (one connection pool per server).
func (kvc *KVClient) InitPool() {
	kvc.c = client.MustNew(kvc.Kvservers, client.Options{})
}

func main() {
	numTests := 3
	var totalThroughput float64
	var totalAverageLatency time.Duration

	for i := 0; i < numTests; i++ {
		fmt.Printf("\n运行测试 %d / %d\n", i+1, numTests)
		throughput, averageLatency := runTest()
		totalThroughput += throughput
		totalAverageLatency += averageLatency

		if i < numTests-1 {
			fmt.Println("等待5秒后进行下一次测试...")
			time.Sleep(5 * time.Second)
		}
	}

	averageThroughput := totalThroughput / float64(numTests)
	overallAverageLatency := totalAverageLatency / time.Duration(numTests)
	fmt.Printf("\n%d 次测试的平均吞吐量: %.4f MB/S\n", numTests, averageThroughput)
	fmt.Printf("%d 次测试的总平均延迟: %v\n", numTests, overallAverageLatency)
}
