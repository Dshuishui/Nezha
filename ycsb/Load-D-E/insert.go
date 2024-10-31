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
	throughput float64
	mu         sync.Mutex
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
	}
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

type mixedWorkloadResult struct {
	totalCount   int           // 总操作数
	totalLatency time.Duration //
	valueSize    int
}

func (kvc *KVClient) mixedWorkload(writeRatio float64, value string) *WorkloadStats {
    stats := &WorkloadStats{
        readLatencies:  make([]time.Duration, 0),
        writeLatencies: make([]time.Duration, 0),
    }

    wg := sync.WaitGroup{}
    opsPerThread := *dnums / *cnums
    wg.Add(*cnums)

    // 为读操作预生成唯一的key集合
    maxReadKey := 2000000
    allReadKeys := generateUniqueRandomInts(0, maxReadKey)
    
    // 为写操作预生成不同范围的key集合
    writeKeyStart := maxReadKey + 1  // 确保写操作的key在读操作范围之外
    writeKeyEnd := writeKeyStart + 2000000
    allWriteKeys := generateUniqueRandomInts(writeKeyStart, writeKeyEnd)
    
    results := make(chan mixedWorkloadResult, *cnums)

    for i := 0; i < *cnums; i++ {
        go func(threadID int) {
            defer wg.Done()

            localResult := mixedWorkloadResult{}
            
            // 计算每个线程的操作范围
            start := threadID * opsPerThread
            end := (threadID + 1) * opsPerThread
            if threadID == *cnums-1 {
                end = *dnums
            }

            localOpsCount := end - start
            writeCount := int(float64(localOpsCount) * writeRatio)
            readCount := localOpsCount - writeCount

            // 从两个不同的key集合中获取对应范围的key
            var localReadKeys, localWriteKeys []int
            if start < len(allReadKeys) {
                readEnd := min(start+readCount, len(allReadKeys))
                localReadKeys = allReadKeys[start:readEnd]
            }
            if start < len(allWriteKeys) {
                writeEnd := min(start+writeCount, len(allWriteKeys))
                localWriteKeys = allWriteKeys[start:writeEnd]
            }

            // 创建混合操作序列
            operations := make([]bool, localOpsCount) // true表示写操作
            for i := 0; i < writeCount; i++ {
                operations[i] = true
            }
            
            // 随机打乱操作顺序
            rand.Shuffle(len(operations), func(i, j int) {
                operations[i], operations[j] = operations[j], operations[i]
            })

            writeKeyIndex := 0
            readKeyIndex := 0

            // 执行操作序列
            for _, isWrite := range operations {
                var key string
                if isWrite {
                    if writeKeyIndex >= len(localWriteKeys) {
                        continue
                    }
                    key = strconv.Itoa(localWriteKeys[writeKeyIndex])
                    writeKeyIndex++
                } else {
                    if readKeyIndex >= len(localReadKeys) {
                        continue
                    }
                    key = strconv.Itoa(localReadKeys[readKeyIndex])
                    readKeyIndex++
                }

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
                } else {
                    value, exists, err := kvc.Get(key)
                    result = OperationResult{
                        isWrite:   false,
                        latency:   time.Since(startTime),
                        success:   err == nil && exists,
                        valueSize: len([]byte(value)),
                    }
                }

                localResult.valueSize = result.valueSize
                localResult.totalCount++
                localResult.totalLatency += result.latency

                stats.addResult(result)
            }
            results <- localResult
        }(i)
    }

    wg.Wait()
    close(results)

    var totalCount float64
    var maxDuration time.Duration
    var valueSize int

    for result := range results {
        totalCount += float64(result.totalCount)
        if result.totalLatency > maxDuration {
            maxDuration = result.totalLatency
        }
        valueSize = result.valueSize
    }
    stats.throughput = (totalCount * float64(valueSize) / 1000000) / maxDuration.Seconds()

    return stats
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
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
	avgReadLatency, _ := calculateAverage(stats.readLatencies)
	avgWriteLatency, _ := calculateAverage(stats.writeLatencies)
	// totalLatency := totalReadAverageLatency + totalWriteAverageLatency

	// readDataSize := float64(stats.totalReads*int64(len(value))) / 1000000   // MB
	// writeDataSize := float64(stats.totalWrites*int64(len(value))) / 1000000 // MB

	// totalThroughput := (readDataSize + writeDataSize) / totalLatency.Seconds()

	// 打印结果
	fmt.Printf("\nTest Results:\n")
	fmt.Printf("Total time: %v\n", elapsedTime)
	fmt.Printf("Read operations: %d (%.1f%%)\n", stats.totalReads, float64(stats.totalReads)*100/float64(*dnums))
	fmt.Printf("Write operations: %d (%.1f%%)\n", stats.totalWrites, float64(stats.totalWrites)*100/float64(*dnums))
	fmt.Printf("Average read latency: %v\n", avgReadLatency)
	fmt.Printf("Average write latency: %v\n", avgWriteLatency)
	fmt.Printf("Total throughput: %.2f MB/s\n", stats.throughput)

	// 清理资源
	for _, pool := range kvc.pools {
		pool.Close()
	}
}
