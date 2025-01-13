package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/pool"
	"gitee.com/dong-shuishui/FlexSync/raft"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
)

var (
	ser        = flag.String("servers", "", "the Server, Client Connects to")
	cnums      = flag.Int("cnums", 1, "Client Threads Number")
	dnums      = flag.Int("dnums", 100000, "total operation number")
	vsize      = flag.Int("vsize", 64, "value size in bytes")
	writeRatio = flag.Float64("wratio", 0.5, "write ratio (0-1.0)")
	scanSize   = flag.Int("scansize", 100, "number of keys to scan")
)

type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64
	seqId     int64
	leaderId  int
	pools     []pool.Pool
}

type OperationResult struct {
	isWrite        bool
	latency        time.Duration
	success        bool
	valueSize      int
	itemsScanned   int           // scan操作读取的条目数
	perItemLatency time.Duration // 每个条目的平均时延
}

type WorkloadStats struct {
	totalWrites    int64
	totalScans     int64
	writeLatencies []time.Duration
	scanLatencies  []time.Duration // 存储每个条目的平均时延
	totalScanItems int64
	totalDataSize  int64 // 总数据量(写入+扫描)
	throughput     float64
	mu             sync.Mutex
	avgLatency     time.Duration
}

func (ws *WorkloadStats) addResult(result OperationResult) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if result.success {
		if result.isWrite {
			ws.totalWrites++
			ws.writeLatencies = append(ws.writeLatencies, result.latency)
			ws.totalDataSize += int64(result.valueSize)
		} else {
			ws.totalScans++
			if result.itemsScanned > 0 {
				ws.scanLatencies = append(ws.scanLatencies, result.perItemLatency)
				ws.totalScanItems += int64(result.itemsScanned)
				ws.totalDataSize += int64(result.valueSize * result.itemsScanned)
			}
		}
	}
}

type mixedWorkloadResult struct {
	totalCount   int
	totalLatency time.Duration
	valueSize    int
	dataSize     int64 // 这个goroutine处理的总数据量
	scanCount    int   // 执行的scan次数
	avgLatency   time.Duration
}

// 辅助函数：返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// 辅助函数：返回两个整数中的较大值
func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func (kvc *KVClient) mixedWorkload(writeRatio float64, value string) *WorkloadStats {
	stats := &WorkloadStats{
		writeLatencies: make([]time.Duration, 0),
		scanLatencies:  make([]time.Duration, 0),
	}

	wg := sync.WaitGroup{}
	opsPerThread := *dnums / *cnums
	wg.Add(*cnums)

	// 预生成唯一的key集合
	maxKey := 38062
	allKeys := generateUniqueRandomInts(0, maxKey)

	results := make(chan mixedWorkloadResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(threadID int) {
			defer wg.Done()

			localResult := mixedWorkloadResult{}
			start := threadID * opsPerThread
			end := (threadID + 1) * opsPerThread
			if threadID == *cnums-1 {
				end = *dnums
			}
			if len(allKeys) < *dnums {
				end = len(allKeys)
			}

			localKeys := allKeys[start:end]
			localOpsCount := end - start

			// 分配操作类型：写操作和扫描操作
			writeCount := int(float64(localOpsCount) * writeRatio)
			// scanCount := localOpsCount - writeCount

			// 创建操作序列
			operations := make([]bool, localOpsCount) // true for write, false for scan
			for i := 0; i < writeCount; i++ {
				operations[i] = true
			}

			// 随机打乱操作顺序
			rand.Shuffle(len(operations), func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			// 执行操作序列
			for idx, keyInt := range localKeys {
				key := strconv.Itoa(keyInt)
				isWrite := operations[idx]
				startTime := time.Now()
				var result OperationResult

				if isWrite {
					reply, err := kvc.PutInRaft(key, value)
					result = OperationResult{
						isWrite:   true,
						latency:   time.Since(startTime),
						success:   err == nil && reply != nil && reply.Err != "defeat",
						valueSize: len(value),
					}
					if result.success {
						localResult.dataSize += int64(len(value))
					}
				} else {
					// scan操作
					scanEnd := min(keyInt+*scanSize, maxKey)
					endKey := strconv.Itoa(scanEnd)
					reply, err := kvc.rangeGet(key, endKey)
					duration := time.Since(startTime)

					if err == nil && reply != nil {
						itemCount := len(reply.KeyValuePairs)
						if itemCount > 0 {
							localResult.totalCount++
							// avgLatency := duration / time.Duration(itemCount) // 计算每个条目的平均时延
							avgLatency := duration // 计算scan的平均时延
							result = OperationResult{
								isWrite:        false,
								latency:        duration,
								success:        true,
								valueSize:      len(value),
								itemsScanned:   itemCount,
								perItemLatency: avgLatency,
							}
							localResult.dataSize += int64(itemCount * len(value))
							localResult.totalLatency += result.latency
						} else {
							result = OperationResult{
								isWrite: false,
								success: true,
							}
						}
					} else {
						result = OperationResult{
							isWrite: false,
							success: false,
						}
					}
				}

				localResult.valueSize = len(value)
				// localResult.totalCount++
				stats.addResult(result)
			}
			if localResult.totalCount > 0 {
				localResult.avgLatency = localResult.totalLatency / time.Duration(localResult.totalCount)
				// localResult.throughput = localResult.totalDataSize / totalActualLatency.Seconds() // 计算吞吐量还是得用实际读取这些数据所花费的时间
			}
			results <- localResult
		}(i)
	}

	wg.Wait()
	close(results)

	// 找出最长执行时间的goroutine
	var maxDuration, totalAvgLatency time.Duration
	var totalDataSize int64
	for result := range results {
		if result.totalLatency > maxDuration {
			maxDuration = result.totalLatency
		}
		totalAvgLatency += result.avgLatency
		totalDataSize += result.dataSize
	}

	stats.avgLatency = totalAvgLatency / time.Duration(*cnums)
	// 计算总吞吐量
	stats.throughput = float64(totalDataSize) / 1000000 / maxDuration.Seconds() // MB/s

	return stats
}

func (kvc *KVClient) rangeGet(startKey string, endKey string) (*kvrpc.ScanRangeResponse, error) {
	args := &kvrpc.ScanRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}

	for {
		p := kvc.pools[kvc.leaderId]
		conn, err := p.Get()
		if err != nil {
			return nil, err
		}
		defer conn.Close()

		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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

func (kvc *KVClient) PutInRaft(key string, value string) (*kvrpc.PutInRaftResponse, error) {
	request := &kvrpc.PutInRaftRequest{
		Key:      key,
		Value:    value,
		Op:       "Put",
		ClientId: kvc.clientId,
		SeqId:    atomic.AddInt64(&kvc.seqId, 1),
	}

	for {
		p := kvc.pools[kvc.leaderId]
		conn, err := p.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
		}
		defer conn.Close()

		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()

		reply, err := client.PutInRaft(ctx, request)
		if err != nil {
			return nil, err
		}

		if reply.Err == raft.OK {
			return reply, nil
		} else if reply.Err == raft.ErrWrongLeader {
			kvc.changeToLeader(int(reply.LeaderId))
		} else if reply.Err == "defeat" {
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

	for i := 0; i < len(kvc.Kvservers); i++ {
		peers_single := []string{kvc.Kvservers[i]}
		p, err := pool.New(peers_single, DesignOptions)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		kvc.pools = append(kvc.pools, p)
	}
}

func (kvc *KVClient) changeToLeader(Id int) int {
	kvc.mu.Lock()
	defer kvc.mu.Unlock()
	kvc.leaderId = Id
	return kvc.leaderId
}

func generateUniqueRandomInts(min, max int) []int {
	nums := make([]int, max-min+1)
	for i := range nums {
		nums[i] = min + i
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return nums
}

func calculateAverage(durations []time.Duration) (averageLatency time.Duration, totalAvgLatency time.Duration) {
	if len(durations) == 0 {
		return 0, 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(len(durations)), sum
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	return bigx.Int64()
}
func main() {
	flag.Parse()

	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	value := util.GenerateLargeValue(*vsize)
	kvc.InitPool()

	fmt.Printf("Starting mixed scan-write workload test with %.1f%% writes\n", *writeRatio*100)
	fmt.Printf("Total operations: %d, Threads: %d, Value size: %d bytes\n",
		*dnums, *cnums, *vsize)

	startTime := time.Now()
	stats := kvc.mixedWorkload(*writeRatio, value)
	elapsedTime := time.Since(startTime)

	// 计算统计信息
	avgWriteLatency, _ := calculateAverage(stats.writeLatencies)
	// avgScanLatency, _ := calculateAverage(stats.scanLatencies)  // 这里已经是每个条目的平均时延了

	// 打印结果
	fmt.Printf("\nTest Results:\n")
	fmt.Printf("Total time: %v\n", elapsedTime)
	fmt.Printf("Write operations: %d (%.1f%%)\n",
		stats.totalWrites, float64(stats.totalWrites)*100/float64(*dnums))
	fmt.Printf("Scan operations: %d (%.1f%%)\n",
		stats.totalScans, float64(stats.totalScans)*100/float64(*dnums))
	fmt.Printf("Average write latency: %v\n", avgWriteLatency)
	fmt.Printf("Average scan latency per item: %v\n", stats.avgLatency)
	fmt.Printf("Total items scanned: %d\n", stats.totalScanItems)
	fmt.Printf("Total throughput: %.2f MB/s\n", stats.throughput)

	// 清理资源
	for _, pool := range kvc.pools {
		pool.Close()
	}
}
