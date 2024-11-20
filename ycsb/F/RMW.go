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
	isWrite   bool
	latency   time.Duration
	success   bool
	valueSize int
}

type WorkloadStats struct {
	totalReads     int64
	totalWrites    int64
	readLatencies  []time.Duration
	writeLatencies []time.Duration
	// readThroughput  float64
	// writeThroughput float64
	// throughput float64
	mu        sync.Mutex
	throughput float64
	avgReadLatency time.Duration
	avgWriteLatency time.Duration
}

func (ws *WorkloadStats) addResult(result OperationResult) {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if result.success {
		if result.isWrite {
			ws.totalWrites++
			ws.writeLatencies = append(ws.writeLatencies, result.latency)
		} else {
			ws.totalReads++
			ws.readLatencies = append(ws.readLatencies, result.latency)
		}
	}else{
		if result.isWrite {
			// ws.totalWrites++
			ws.writeLatencies = append(ws.writeLatencies, result.latency)
		} else {
			// ws.totalReads++
			ws.readLatencies = append(ws.readLatencies, result.latency)
		}
	}
}

func calculateAverage(durations []time.Duration, num int64) (averageLatency time.Duration, totalAvgLatency time.Duration) {
	if len(durations) == 0 {
		return 0, 0
	}
	var sum time.Duration
	for _, d := range durations {
		sum += d
	}
	return sum / time.Duration(num), sum
}

type mixedWorkloadResult struct {
	totalCount   int           // 总操作数
	totalLatency time.Duration //
	valueSize    int
	avgReadLatency time.Duration
	avgWriteLatency time.Duration
}

func (kvc *KVClient) mixedWorkload(writeRatio float64, value string) *WorkloadStats {
	stats := &WorkloadStats{
		readLatencies:  make([]time.Duration, 0),
		writeLatencies: make([]time.Duration, 0),
	}

	wg := sync.WaitGroup{}
	opsPerThread := *dnums / *cnums
	wg.Add(*cnums)
	var baga = 2500000

	// 预生成唯一的key集合
	allKeys := generateUniqueRandomInts(0, baga) // 针对1KB value，KV分离后
	// allKeys := generateUniqueRandomInts(0, 620000)	// 针对16KB value，KV分离后
	// allKeys := generateUniqueRandomInts(0, 1000000)	// 针对16KB value，KV分离前
	// allKeys := generateUniqueRandomInts(0, 12000000)	// 针对1KB value，KV分离前

	results := make(chan mixedWorkloadResult, *cnums)

	// 为每个线程预先生成操作序列
	for i := 0; i < *cnums; i++ {
		go func(threadID int) {
			defer wg.Done()

			localResult := mixedWorkloadResult{}
			start := threadID * opsPerThread
			end := (threadID + 1) * opsPerThread
			if threadID == *cnums-1 {
				end = *dnums
			}

			// 取余，保证索引下表在范围内
			start = start % baga
			end = end % baga

			// 确保待查询的key在范围内
			if len(allKeys) < *dnums {
				end = len(allKeys)
			}

			localKeys := allKeys[start:end]
			localOpsCount := end - start

			// 计算当前线程中写操作的精确数量
			writeCount := int(float64(localOpsCount) * writeRatio)
			// readCount := localOpsCount - writeCount

			// 创建操作序列，true表示写操作，false表示读操作
			operations := make([]bool, localOpsCount)
			for i := 0; i < writeCount; i++ {
				operations[i] = true
			}
			// 剩余的为读操作，默认为false

			// 随机打乱操作顺序
			rand.Shuffle(len(operations), func(i, j int) {
				operations[i], operations[j] = operations[j], operations[i]
			})

			startTimeBig := time.Now()
			// 执行操作序列
			for idx, keyInt := range localKeys {
				key := strconv.Itoa(keyInt)
				isWrite := operations[idx]

				startTime := time.Now()
				var result OperationResult

				if isWrite {
					
					// 下面是复合的RMW的写入操作，先读取在写入
					value, exists, err := kvc.Get(key)
					if err == nil && exists {
						reply, err := kvc.PutInRaft(key, value)
						result = OperationResult{
							isWrite:   true,
							latency:   time.Since(startTime),
							success:   err == nil && reply != nil && reply.Err != "defeat",
							valueSize: len(value),
						}
					}else{
						result = OperationResult{
							isWrite:   true,
							latency:   time.Since(startTime),
							success:   false,
							valueSize: len(value),
						}
					}
				} else { //	get的key用在一定范围内随机生成的key
					randKey := rand.Intn(baga)
					randStrKey := strconv.Itoa(randKey)
					value, exists, err := kvc.Get(randStrKey)
					if err != nil {
						fmt.Println("什么问题err:", err)
					}
					result = OperationResult{
						isWrite:   false,
						latency:   time.Since(startTime),
						success:   err == nil && exists && value!="ErrNoKey",
						valueSize: len([]byte(value)),
					}
				}
				if result.success { //	得是正常执行了才计算进去
					localResult.valueSize = result.valueSize
					localResult.totalCount++
				}
				// localResult.totalLatency += result.latency

				stats.addResult(result)
			}
			localResult.avgReadLatency, _ = calculateAverage(stats.readLatencies,stats.totalReads)
			localResult.avgWriteLatency, _ = calculateAverage(stats.writeLatencies,stats.totalWrites)
			localResult.totalLatency = time.Since(startTimeBig)
			results <- localResult
		}(i)
	}

	wg.Wait()
	close(results)

	var totalWRcount float64
	// var throughput float64
	// var totalLatency time.Duration
	var maxDuration time.Duration
	var totalReadLatency time.Duration
	var totalWriteLatency time.Duration
	var valueSize int

	for result := range results {
		totalWRcount += float64(result.totalCount)
		if result.totalLatency > maxDuration {
			maxDuration = result.totalLatency
		}
		valueSize = result.valueSize
		totalReadLatency += result.avgReadLatency
		totalWriteLatency += result.avgWriteLatency
	}
	fmt.Printf("读取多少数据：%v---%v---%v\n", totalWRcount, maxDuration, valueSize)
	stats.throughput = (totalWRcount * float64(valueSize) / 1000000) / maxDuration.Seconds()
	stats.avgReadLatency = totalReadLatency / time.Duration(*cnums) 
	stats.avgWriteLatency = totalWriteLatency / time.Duration(*cnums) 

	return stats
}

// Get方法实现
func (kvc *KVClient) Get(key string) (string, bool, error) {
	args := &kvrpc.GetInRaftRequest{
		Key:      key,
		ClientId: kvc.clientId,
		SeqId:    atomic.AddInt64(&kvc.seqId, 1),
	}
	targetId := kvc.leaderId
	for {
		reply, err := kvc.SendGetInRaft(targetId, args)
		if err != nil {
			return "", false, err
		}
		if reply.Err == raft.OK {
			return reply.Value, true, nil
		} else if reply.Err == raft.ErrNoKey {
			return reply.Value, false, nil
		} else if reply.Err == raft.ErrWrongLeader {
			targetId = int(reply.LeaderId)
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// SendGetInRaft方法实现
func (kvc *KVClient) SendGetInRaft(targetId int, request *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	p := kvc.pools[targetId]
	conn, err := p.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	reply, err := client.GetInRaft(ctx, request)
	if err != nil {
		return nil, err
	}
	return reply, nil
}

// PutInRaft方法实现
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

	fmt.Printf("Starting mixed workload test with %.1f%% writes\n", *writeRatio*100)
	fmt.Printf("Total operations: %d, Threads: %d, Write value size: %d bytes\n", *dnums, *cnums, *vsize)

	startTime := time.Now()
	stats := kvc.mixedWorkload(*writeRatio, value)
	elapsedTime := time.Since(startTime)

	// 计算统计信息
	// avgReadLatency, _ := calculateAverage(stats.readLatencies)
	// avgWriteLatency, _ := calculateAverage(stats.writeLatencies)
	// totalLatency := totalReadAverageLatency + totalWriteAverageLatency

	// readDataSize := float64(stats.totalReads*int64(len(value))) / 1000000   // MB
	// writeDataSize := float64(stats.totalWrites*int64(len(value))) / 1000000 // MB

	// totalThroughput := (readDataSize + writeDataSize) / totalLatency.Seconds()

	// 打印结果
	fmt.Printf("\nTest Results:\n")
	fmt.Printf("Total time: %v\n", elapsedTime)
	fmt.Printf("Read operations: %d (%.1f%%)\n", stats.totalReads, float64(stats.totalReads)*100/float64(*dnums))
	fmt.Printf("Write operations: %d (%.1f%%)\n", stats.totalWrites, float64(stats.totalWrites)*100/float64(*dnums))
	fmt.Printf("Average read latency: %v\n", stats.avgReadLatency)
	fmt.Printf("Average write latency: %v\n", stats.avgWriteLatency)
	fmt.Printf("Total throughput: %.2f MB/s\n", stats.throughput)

	// 清理资源
	for _, pool := range kvc.pools {
		pool.Close()
	}
}
