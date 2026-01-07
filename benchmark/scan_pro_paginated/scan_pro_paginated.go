package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
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
	ser        = flag.String("servers", "", "the Server, Client Connects to")
	cnums      = flag.Int("cnums", 1, "Client Threads Number")
	dnums      = flag.Int("dnums", 1000000, "data num")
	k1         = flag.Int("startkey", 0, "first key")
	k2         = flag.Int("endkey", 20, "last key")
	outputFile = flag.String("output", "scan_benchmark_results.txt", "输出结果文件名")
	// 新增：分页参数，用于控制每次scan的最大key数量
	// 计算方式：假设value为16KB，每次scan最多返回1.5GB数据
	// 1.5GB / 16KB ≈ 100,000 个key
	batchSize = flag.Int("batchsize", 100000, "每次scan请求的最大key数量（用于分页）")
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
	totalLatency time.Duration
	goodscan     int
}

type scanResult struct {
	totalCount    int
	scanCount     int
	avgLatency    time.Duration
	throughput    float64
	valueSize     int
	totalDataSize float64
	totalLatency  time.Duration
}

type TestResult struct {
	testNumber    int
	elapsedTime   time.Duration
	throughput    float64
	avgLatency    time.Duration
	totalRequests int
	goodPut       int
	clientThreads int
	valueSize     int
	dataSizeMB    float64
	scanCount     int
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

			for j := 0; j < base; j++ {
				k1 := rand.Intn(5250000)
				k2 := k1 + gapkey - 1
				startKey := strconv.Itoa(k1)
				endKey := strconv.Itoa(k2)

				start := time.Now()
				// 【关键修改】使用分页的rangeGet
				reply, err := kvc.rangeGetWithPagination(startKey, endKey, gapkey)
				duration := time.Since(start)
				
				if err != nil {
					fmt.Printf("有问题：%v\n", err)
				}
				
				if err == nil && reply != nil && len(reply.KeyValuePairs) != 0 {
					count := len(reply.KeyValuePairs)
					localResult.totalCount += count

					if localResult.totalCount > 0 && count != 0 {
						localResult.scanCount++

						for _, value := range reply.KeyValuePairs {
							localResult.valueSize = len([]byte(value))
							break
						}

						avgItemLatency := duration
						scanDataSize := float64(count*localResult.valueSize) / 1000000 // MB

						totalAvgLatency += avgItemLatency
						localResult.totalDataSize += scanDataSize
						localResult.totalLatency += duration
					}
				}
			}

			if localResult.scanCount > 0 {
				localResult.avgLatency = totalAvgLatency / time.Duration(localResult.scanCount)
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

	for result := range results {
		if result.scanCount > 0 {
			if result.totalLatency > maxDuration {
				maxDuration = result.totalLatency
			}
			totalAvgLatency += result.avgLatency
			totalData += result.totalDataSize
			valueSize = result.valueSize
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

	return throughput, avgLatency, sum_Size_MB
}

// 【新增函数】分页的rangeGet - 将大范围scan拆分成多个小范围scan
func (kvc *KVClient) rangeGetWithPagination(startKey string, endKey string, totalRange int) (*kvrpc.ScanRangeResponse, error) {
	startKeyInt, err := strconv.Atoi(startKey)
	if err != nil {
		return nil, err
	}
	endKeyInt, err := strconv.Atoi(endKey)
	if err != nil {
		return nil, err
	}

	// 如果范围小于等于batchSize，直接执行单次scan
	if totalRange <= *batchSize {
		return kvc.rangeGet(startKey, endKey)
	}

	// 否则进行分页scan
	allResults := make(map[string]string)
	currentStart := startKeyInt

	for currentStart <= endKeyInt {
		currentEnd := currentStart + *batchSize - 1
		if currentEnd > endKeyInt {
			currentEnd = endKeyInt
		}

		// 执行单次scan
		reply, err := kvc.rangeGet(strconv.Itoa(currentStart), strconv.Itoa(currentEnd))
		if err != nil {
			return nil, err
		}

		// 合并结果
		if reply != nil && reply.KeyValuePairs != nil {
			for k, v := range reply.KeyValuePairs {
				allResults[k] = v
			}
		}

		// 移动到下一个批次
		currentStart = currentEnd + 1
	}

	// 返回合并后的结果
	return &kvrpc.ScanRangeResponse{
		Err:           raft.OK,
		KeyValuePairs: allResults,
	}, nil
}

// 原有的rangeGet函数保持不变，但现在只处理单个批次
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
		defer cancel()
		reply, err := client.ScanRangeInRaft(ctx, args)
		if err != nil {
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

// 保存测试结果到文件
func saveResultToFile(result TestResult, filePath string, isFirstTest bool) error {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %v", err)
	}

	flag := os.O_WRONLY | os.O_CREATE
	if isFirstTest {
		flag |= os.O_TRUNC
	} else {
		flag |= os.O_APPEND
	}

	file, err := os.OpenFile(filePath, flag, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	if isFirstTest {
		header := "测试编号,耗时(秒),吞吐量(MB/S),平均延迟(ms),总请求数,成功请求数,Scan次数,客户端线程数,值大小(字节),数据总量(MB)\n"
		if _, err := file.WriteString(header); err != nil {
			return fmt.Errorf("写入标题失败: %v", err)
		}
	}

	resultLine := fmt.Sprintf("%d,%.2f,%.4f,%.2f,%d,%d,%d,%d,%d,%.2f\n",
		result.testNumber,
		result.elapsedTime.Seconds(),
		result.throughput,
		float64(result.avgLatency.Microseconds())/1000,
		result.totalRequests,
		result.goodPut,
		result.scanCount,
		result.clientThreads,
		result.valueSize,
		result.dataSizeMB,
	)

	if _, err := file.WriteString(resultLine); err != nil {
		return fmt.Errorf("写入结果失败: %v", err)
	}

	return nil
}

// 保存总结果到文件
func saveSummaryToFile(filePath string, numTests int, avgThroughput float64, avgLatency time.Duration, gapkey int) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return fmt.Errorf("打开文件失败: %v", err)
	}
	defer file.Close()

	summary := fmt.Sprintf("\n汇总信息\n")
	summary += fmt.Sprintf("%d 次测试的平均吞吐量: %.4f MB/S\n", numTests, avgThroughput)
	summary += fmt.Sprintf("%d 次测试的总平均延迟: %.2f ms\n", numTests, float64(avgLatency.Microseconds())/1000)
	summary += fmt.Sprintf("测试完成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	summary += fmt.Sprintf("测试参数: 客户端线程数=%d, 请求总数=%d, 范围大小=%d, 分页批次大小=%d\n", *cnums, *dnums, gapkey, *batchSize)

	if _, err := file.WriteString(summary); err != nil {
		return fmt.Errorf("写入汇总信息失败: %v", err)
	}

	return nil
}

func main() {
	flag.Parse()
	
	// 【重要】这里的gapkey就是你的scan范围大小
	// 如果要测试1,000,000个key的scan，将gapkey设为1000000
	gapkey := 1000000  // 修改为你需要的scan范围大小
	
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool()

	// 打印配置信息
	fmt.Printf("=== Scan测试配置 ===\n")
	fmt.Printf("Scan范围大小(gapkey): %d\n", gapkey)
	fmt.Printf("分页批次大小(batchsize): %d\n", *batchSize)
	fmt.Printf("预计分页次数: %d\n", (gapkey+*batchSize-1) / *batchSize)
	fmt.Printf("====================\n\n")

	_, filename, _, ok := runtime.Caller(0)
	var sourceDir string
	if ok {
		sourceDir = filepath.Dir(filename)
	} else {
		sourceDir = "."
		fmt.Println("警告：无法获取源文件目录，将使用当前目录")
	}

	resultFilePath := filepath.Join(sourceDir, *outputFile)

	fmt.Printf("测试结果将保存到: %s\n", resultFilePath)
	fmt.Printf("开始运行测试，范围大小: %d...\n\n", gapkey)

	var totalThroughput float64
	var totalAvgLatency time.Duration
	numTests := 10

	for i := 0; i < numTests; i++ {
		startTime := time.Now()
		throughput, avgLatency, sum_Size_MB := kvc.scan(gapkey)
		elapsedTime := time.Since(startTime)
		totalThroughput += throughput
		totalAvgLatency += avgLatency

		result := TestResult{
			testNumber:    i + 1,
			elapsedTime:   elapsedTime,
			throughput:    throughput,
			avgLatency:    avgLatency,
			totalRequests: *dnums,
			goodPut:       kvc.goodPut,
			clientThreads: *cnums,
			valueSize:     kvc.valuesize,
			dataSizeMB:    sum_Size_MB,
			scanCount:     kvc.goodscan,
		}

		isFirstTest := i == 0
		if err := saveResultToFile(result, resultFilePath, isFirstTest); err != nil {
			fmt.Printf("保存测试结果失败: %v\n", err)
		} else {
			fmt.Printf("测试 %d 的结果已保存到 %s\n", i+1, resultFilePath)
		}

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

	if err := saveSummaryToFile(resultFilePath, numTests, avgThroughput, avgLatency, gapkey); err != nil {
		fmt.Printf("保存汇总信息失败: %v\n", err)
	} else {
		fmt.Printf("汇总信息已保存到 %s\n", resultFilePath)
	}

	for _, pool := range kvc.pools {
		pool.Close()
		util.DPrintf("The raft pool has been closed")
	}
}