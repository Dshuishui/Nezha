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
	goodscan int
}

func (kvc *KVClient) scan(gapkey int) (float64, time.Duration) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0
	kvc.goodscan = 0
	kvc.totalLatency = 0 // 重置总延迟

	type scanResult struct {
		count     int
		count_scan     int
		valueSize int
		latency   time.Duration // 添加延迟字段
	}

	results := make(chan scanResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			tag := 0
			localResult := scanResult{}
			rand.Seed(time.Now().UnixNano() + int64(i))
			for j := 0; j < base; j++ {
				k1 := rand.Intn(3000000)
				k2 := k1 + gapkey
				startKey := strconv.Itoa(k1)
				endKey := strconv.Itoa(k2)
				if startKey > endKey {
					startKey, endKey = endKey, startKey
				}
				start := time.Now() // 开始计时
				reply, err := kvc.rangeGet(startKey, endKey)
				duration := time.Since(start) // 计算持续时间
				if err == nil {
					localResult.count += len(reply.KeyValuePairs)
					localResult.count_scan ++
					localResult.latency += duration // 累加延迟
				}
				if tag == 0 && reply != nil && len(reply.KeyValuePairs) != 0 {
					for _, value := range reply.KeyValuePairs {
						// fmt.Printf("这个value为多少：%v\n",value)
						localResult.valueSize = len([]byte(value))
						// fmt.Printf("这个valuesize为多少：%v\n",localResult.valueSize)
						break // 只迭代一次后就跳出循环
					}
					tag = 1
				}
			}
			results <- localResult
		}(i)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	totalGoodPut := 0
	totalScan := 0
	tag2 := 0
	for result := range results {
		// 保证拿去一个valuesize即可，以免全部读取重复的valuesize引起不必要的开销
		if result.valueSize != 0 && tag2 == 0 {
			kvc.valuesize = result.valueSize
			tag2 = 1

		}
		totalGoodPut += result.count
		totalScan +=result.count_scan
		kvc.totalLatency += result.latency // 累加总延迟
	}

	kvc.goodPut = totalGoodPut
	kvc.goodscan = totalScan
	// fmt.Printf("读出来的valuesize为：%v\n", kvc.valuesize)
	sum_Size_MB := float64(kvc.goodPut*kvc.valuesize) / 1000000
	avgLatency := kvc.totalLatency / time.Duration(kvc.goodscan) // 计算平均延迟
	return sum_Size_MB,avgLatency
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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
	gapkey := 250000
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
		sum_Size_MB, avgLatency := kvc.scan(gapkey)
		elapsedTime := time.Since(startTime)
		throughput := float64(sum_Size_MB) / elapsedTime.Seconds()
		totalThroughput += throughput
		totalAvgLatency += avgLatency

		fmt.Printf("Test %d: elapse:%v, throught:%.4fMB/S, avg latency:%v, total %v, goodPut %v, client %v, valuesize %vB, Size %.2fMB\n",
			i+1, elapsedTime, throughput, avgLatency, *dnums, kvc.goodPut, *cnums, kvc.valuesize, sum_Size_MB)

		if i < numTests-1 {
			time.Sleep(8 * time.Second)
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
