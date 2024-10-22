package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/pool"
	"gitee.com/dong-shuishui/FlexSync/raft"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

	crand "crypto/rand"
	"math/big"
)

var (
	ser   = flag.String("servers", "", "the Server, Client Connects to")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	k1    = flag.Int("startkey", 0, "first key")
	k2    = flag.Int("endkey", 20, "last key")
)

type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64
	seqId     int64
	leaderId  int

	pools        []pool.Pool
	goodPut      int
	valuesize    int
	totalLatency time.Duration // 添加总延迟字段
	goodscan     int
}

type scanResult struct {
	totalCount int // 总读取数量
	scanCount  int // 执行的scan次数
	avgLatency time.Duration
	throughput float64
	valueSize  int
	totalDataSize float64
	totalLatency time.Duration
}

func (kvc *KVClient) scan(gapkey int) (float64, time.Duration, float64) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0
	kvc.goodscan = 0

	results := make(chan scanResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			localResult := scanResult{}
			rand.Seed(time.Now().UnixNano() + int64(i))

			var totalAvgLatency time.Duration
			// var totalDataSize float64
			// var totalActualLatency time.Duration

			for j := 0; j < base; j++ {
				k1 := rand.Intn(625000)
				k2 := k1 + gapkey
				startKey := strconv.Itoa(k1)
				endKey := strconv.Itoa(k2)
				if startKey > endKey {
					startKey, endKey = endKey, startKey
				}

				start := time.Now()
				reply, err := kvc.rangeGet(startKey, endKey)
				duration := time.Since(start)
				if err != nil {
					fmt.Printf("有问题：%v\n", err)
				}
				// fmt.Printf("有问题：%v\n",err)
				if err == nil && reply != nil && len(reply.KeyValuePairs) != 0 {
					count := len(reply.KeyValuePairs)
					localResult.totalCount += count
					localResult.scanCount++

					if localResult.totalCount > 0 && count != 0 { // 该次scan读取到了数据
						for _, value := range reply.KeyValuePairs {
							localResult.valueSize = len([]byte(value))
							break // 只迭代一次后就跳出循环
						}

						// 计算单个scan的平均时延和吞吐量
						avgItemLatency := duration / time.Duration(count)
						// scanLatency := avgItemLatency * time.Duration(gapkey)
						scanDataSize := float64(count*localResult.valueSize) / 1000000 // MB
						// scanThroughput := scanDataSize / duration.Seconds()

						totalAvgLatency += avgItemLatency
						localResult.totalDataSize += scanDataSize
						localResult.totalLatency += duration
					}
				}
			}

			if localResult.scanCount > 0 {
				localResult.avgLatency = totalAvgLatency / time.Duration(localResult.scanCount)
				// localResult.throughput = localResult.totalDataSize / totalActualLatency.Seconds() // 计算吞吐量还是得用实际读取这些数据所花费的时间
			}

			results <- localResult
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalAvgLatency, maxDuration time.Duration
	var totalData float64
	var totalCount, totalScanCount, valueSize int
	// goroutineCount := 0

	for result := range results {
		if result.scanCount > 0 {
			if result.totalLatency > maxDuration{
				maxDuration = result.totalLatency
			}
			totalAvgLatency += result.avgLatency
			totalData += result.totalDataSize
			valueSize = result.valueSize
			// goroutineCount++
		}
		totalCount += result.totalCount
		totalScanCount += result.scanCount
	}

	kvc.goodPut = totalCount
	kvc.goodscan = totalScanCount
	kvc.valuesize = valueSize

	avgLatency := totalAvgLatency / time.Duration(*cnums)
	throughput := totalData / maxDuration.Seconds()

	sum_Size_MB := float64(totalCount*valueSize) / 1000000

	// for _, pool := range kvc.pools {
	// 	pool.Close()
	// 	util.DPrintf("The raft pool has been closed")
	// }

	return throughput, avgLatency, sum_Size_MB
}

func (kvc *KVClient) rangeGet(key1 string, key2 string) (*kvrpc.ScanRangeResponse, error) {
	args := &kvrpc.ScanRangeRequest{
		StartKey: key1,
		EndKey:   key2,
	}
	for {
		p := kvc.pools[kvc.leaderId]
		conn, err := p.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
		}
		defer conn.Close()
		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2000)
		defer cancel()
		reply, err := client.ScanRangeInRaft(ctx, args)
		if err != nil {
			// util.EPrintf("err in ScanRangeInRaft: %v", err)
			return nil, err
		}
		if reply.Err == raft.ErrWrongLeader {
			kvc.changeToLeader(int(reply.LeaderId))
			continue
		}
		if reply.Err == raft.OK {
			return reply, nil
		}
	}
}

func (kvc *KVClient) InitPool() {
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              150,
		MaxActive:            300,
		MaxConcurrentStreams: 800,
		Reuse:                true,
	}
	fmt.Printf("servers:%v\n", kvc.Kvservers)
	for i := 0; i < len(kvc.Kvservers); i++ {
		peers_single := []string{kvc.Kvservers[i]}
		p, err := pool.New(peers_single, DesignOptions)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		kvc.pools = append(kvc.pools, p)
	}
}

func (kvc *KVClient) changeToLeader(Id int) (leaderId int) {
	kvc.mu.Lock()
	defer kvc.mu.Unlock()
	kvc.leaderId = Id
	return kvc.leaderId
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func main() {
	flag.Parse()
	gapkey := 62500
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool()

	var totalThroughput float64
	var totalAvgLatency time.Duration
	numTests := 3

	for i := 0; i < numTests; i++ {
		startTime := time.Now()
		throughput, avgLatency, sum_Size_MB := kvc.scan(gapkey)
		elapsedTime := time.Since(startTime)
		// throughput := float64(sum_Size_MB) / elapsedTime.Seconds()
		// throughput := float64(sum_Size_MB) / avgLatency.Seconds()	// 用这个时延
		totalThroughput += throughput
		totalAvgLatency += avgLatency

		fmt.Printf("Test %d: elapse:%v, throught:%.4fMB/S, avg latency:%v, total %v, goodPut %v, client %v, valuesize %vB, Size %.2fMB\n",
			i+1, elapsedTime, throughput, avgLatency, *dnums, kvc.goodPut, *cnums, kvc.valuesize, sum_Size_MB)

		if i < numTests-1 {
			time.Sleep(5 * time.Second)
		}
	}

	avgThroughput := totalThroughput / float64(numTests)
	avgLatency := totalAvgLatency / time.Duration(numTests)
	fmt.Printf("\nAverage throughput over %d tests: %.4fMB/S\n", numTests, avgThroughput)
	fmt.Printf("Average latency over %d tests: %v\n", numTests, avgLatency)

	for _, pool := range kvc.pools {
		pool.Close()
		util.DPrintf("The raft pool has been closed")
	}
}
