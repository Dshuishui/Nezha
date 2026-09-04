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

	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	crand "crypto/rand"
	"math/big"
)

var (
	ser   = flag.String("servers", "", "the Server, Client Connects to")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	k1    = flag.Int("startkey", 0, "first key")
	k2    = flag.Int("endkey", 20, "last key")
	// 扫描长度。原先写死为 4000000——那比任何一次实验的数据量都大，于是每轮
	// SCAN 都在扫全库，测的是"顺序读整个数据集有多快"，而不是 range query。
	// 稀疏索引的价值（二分定位 + 块内顺序扫）在全库扫描下被完全稀释。
	// 论文 Fig.7 用的是 10/100/1000/10000 四档，这里做成参数以便对齐。
	gapkeyArg = flag.Int("gapkey", 1000, "scan length: number of keys per range query")
	// 扫描起点的采样上界，应设为实际写入的记录数。
	// 原先写死为 370000 且与实际写入量无关，两头都出错：写入多于该值时高位数据
	// 永远扫不到；写入少于该值时起点会落在没有数据的区间，range query 返回空，
	// 于是 goodPut 归零、吞吐算出 NaN。这与 zipf_read 早先 keyspace 默认 1 亿
	// 是同一类错误——读的键空间必须等于实际写入量。
	keyspaceArg = flag.Int("keyspace", 370000, "扫描起点的采样上界；应设为实际写入的记录数")
	outputFile  = flag.String("output", "scan_benchmark_results.txt", "输出结果文件名")
	// 每轮都要扫遍整个数据集，耗时随数据量线性增长：50 万 key 时一轮约 30s，
	// 300 万 key 时一轮 3 分钟以上，跑满 100 轮就是 5 小时以上。
	// 默认值保持 100 不变，已有实验数据的口径不受影响；
	// 大数据集下用这个 flag 把轮数降到能在合理时间内跑完。
	numTestsFlag = flag.Int("tests", 100, "测试轮数（结果取各轮平均）")
	// 轮间静置。原先固定 5 秒，100 轮即 500 秒纯等待，而每轮扫描往往只要几毫秒。
	restSecFlag = flag.Int("rest", 5, "轮间静置秒数，让系统状态回稳")
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
	totalCount    int // 总读取数量
	scanCount     int // 执行的scan次数
	avgLatency    time.Duration
	throughput    float64
	valueSize     int
	totalDataSize float64
	totalLatency  time.Duration
}

// 定义测试结果结构体
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
			// var totalDataSize float64
			// var totalActualLatency time.Duration

			for j := 0; j < base; j++ {
				// 起点上界要留出一个 gapkey，否则区间尾部越过数据末端，
				// 扫回来的条数比 gapkey 少，吞吐被系统性低估。
				upper := *keyspaceArg - gapkey
				if upper < 1 {
					upper = 1
				}
				k1 := rand.Intn(upper)
				// k1 := (i*base + j) * gapkey
				k2 := k1 + gapkey - 1
				startKey := strconv.Itoa(k1)
				endKey := strconv.Itoa(k2)
				// if startKey > endKey {
				// 	startKey, endKey = endKey, startKey
				// }

				start := time.Now()
				reply, err := kvc.rangeGet(startKey, endKey)
				duration := time.Since(start)
				if err != nil {
					fmt.Printf("有问题：%v\n", err)
				}
				// fmt.Printf("有问题：%v\n",err)
				if err == nil && reply != nil && len(reply.KeyValuePairs) != 0 {
					count := len(reply.KeyValuePairs)
					// fmt.Printf("一次读出多少键值对：%v\n", count)
					// for key, _ := range reply.KeyValuePairs {
					// 	fmt.Printf("the map is: key-%v\n", key)
					// }
					localResult.totalCount += count
					// localResult.scanCount++

					if localResult.totalCount > 0 && count != 0 { // 该次scan读取到了数据
						localResult.scanCount++

						for _, value := range reply.KeyValuePairs {
							localResult.valueSize = len([]byte(value))
							break // 只迭代一次后就跳出循环
						}

						// 计算单个scan的平均时延和吞吐量
						// avgItemLatency := duration / time.Duration(count)
						avgItemLatency := duration
						// scanLatency := avgItemLatency * time.Duration(gapkey)
						scanDataSize := float64(count*localResult.valueSize) / 1000000 // MB
						// scanThroughput := scanDataSize / duration.Seconds()

						totalAvgLatency += avgItemLatency
						localResult.totalDataSize += scanDataSize
						localResult.totalLatency += duration
					}
				}
				if reply == nil {
					// fmt.Println("reply为空")
				} else {
					// fmt.Printf("err=%v\n reply=%v\n length=%v\n", err, reply.Err, len(reply.KeyValuePairs))
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
			if result.totalLatency > maxDuration {
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
	// maxDuration 为零意味着这一轮没有任何一次 range query 返回数据——除下去得到
	// NaN，再被后续的平均值传染，整份汇总就都成了 NaN。把这种轮次记为 0 吞吐，
	// 汇总里才看得出"有几轮扫空了"，而不是整列不可用。
	var throughput float64
	if maxDuration.Seconds() > 0 {
		throughput = totalData / maxDuration.Seconds()
	}

	// 计算ops
	// throughput = (float64(totalScanCount)*float64(valueSize) / 1000000) / maxDuration.Seconds()

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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*600)
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

// 保存测试结果到文件
func saveResultToFile(result TestResult, filePath string, isFirstTest bool) error {
	// 确保目录存在
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("创建目录失败: %v", err)
	}

	// 打开文件，如果是第一次测试则覆盖文件，否则追加
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

	// 如果是第一次测试，写入标题行
	if isFirstTest {
		header := "测试编号,耗时(秒),吞吐量(MB/S),平均延迟(ms),总请求数,成功请求数,Scan次数,客户端线程数,值大小(字节),数据总量(MB)\n"
		if _, err := file.WriteString(header); err != nil {
			return fmt.Errorf("写入标题失败: %v", err)
		}
	}

	// 写入测试结果
	resultLine := fmt.Sprintf("%d,%.2f,%.4f,%.2f,%d,%d,%d,%d,%d,%.2f\n",
		result.testNumber,
		result.elapsedTime.Seconds(),
		result.throughput,
		float64(result.avgLatency.Microseconds())/1000, // 转换为毫秒
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

	// 添加空行和汇总信息
	summary := fmt.Sprintf("\n汇总信息\n")
	summary += fmt.Sprintf("%d 次测试的平均吞吐量: %.4f MB/S\n", numTests, avgThroughput)
	summary += fmt.Sprintf("%d 次测试的总平均延迟: %.2f ms\n", numTests, float64(avgLatency.Microseconds())/1000)
	summary += fmt.Sprintf("测试完成时间: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	summary += fmt.Sprintf("测试参数: 客户端线程数=%d, 请求总数=%d, 范围大小=%d\n", *cnums, *dnums, gapkey)

	if _, err := file.WriteString(summary); err != nil {
		return fmt.Errorf("写入汇总信息失败: %v", err)
	}

	return nil
}

func main() {
	flag.Parse()
	gapkey := *gapkeyArg
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()
	fmt.Printf("Client MaxSendMsgSize: %d bytes (%d GB)\n", pool.MaxSendMsgSize, pool.MaxSendMsgSize>>30)
	fmt.Printf("Client MaxRecvMsgSize: %d bytes (%d GB)\n", pool.MaxRecvMsgSize, pool.MaxRecvMsgSize>>30)

	kvc.InitPool()

	// 获取源代码文件所在的目录
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
	numTests := *numTestsFlag

	for i := 0; i < numTests; i++ {
		startTime := time.Now()
		throughput, avgLatency, sum_Size_MB := kvc.scan(gapkey)
		elapsedTime := time.Since(startTime)
		// throughput := float64(sum_Size_MB) / elapsedTime.Seconds()
		// throughput := float64(sum_Size_MB) / avgLatency.Seconds()	// 用这个时延
		totalThroughput += throughput
		totalAvgLatency += avgLatency

		// 保存单次测试结果到文件
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

		// 如果是第一次测试，则会创建新文件
		isFirstTest := i == 0
		if err := saveResultToFile(result, resultFilePath, isFirstTest); err != nil {
			fmt.Printf("保存测试结果失败: %v\n", err)
		} else {
			fmt.Printf("测试 %d 的结果已保存到 %s\n", i+1, resultFilePath)
		}

		fmt.Printf("Test %d: elapse:%v, throught:%.4fMB/S, avg latency:%v, total %v, goodPut %v, client %v, valuesize %vB, Size %.2fMB\n",
			i+1, elapsedTime, throughput, avgLatency, *dnums, kvc.goodPut, *cnums, kvc.valuesize, sum_Size_MB)

		if i < numTests-1 {
			if *restSecFlag > 0 {
				time.Sleep(time.Duration(*restSecFlag) * time.Second)
			}
		}
	}

	avgThroughput := totalThroughput / float64(numTests)
	avgLatency := totalAvgLatency / time.Duration(numTests)

	// 打印汇总信息到控制台
	fmt.Printf("\nAverage throughput over %d tests: %.4fMB/S\n", numTests, avgThroughput)
	fmt.Printf("Average latency over %d tests: %v\n", numTests, avgLatency)

	// 保存汇总信息到文件
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
