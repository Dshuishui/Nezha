package main

import (
    "flag"
    "fmt"
    "math/rand"
    "context"
    "os"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
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
    vsize = flag.Int("vsize", 64, "value size in type")
    csvFile = flag.String("csvfile", "throughput_data.csv", "CSV file to store throughput data")
    interval = flag.Duration("interval", 100*time.Millisecond, "Statistics collection interval")
)

// 时间快照记录结构
type ThroughputSnapshot struct {
    Timestamp         time.Time // 快照时间戳
    RelativeTime      float64   // 相对开始时间（秒）
    CumulativeOps     int64     // 累积操作数
    CumulativeDataMB  float64   // 累积数据量（MB）
}

type KVClient struct {
    Kvservers []string
    mu        sync.Mutex
    clientId  int64 // 客户端唯一标识
    seqId     int64 // 该客户端单调递增的请求id
    leaderId  int

    pools     []pool.Pool
    
    // 统计相关 - 使用原子操作
    totalOps   int64 // 总操作数
    totalBytes int64 // 总字节数
    
    // 快照记录相关
    snapshotMu sync.Mutex
    snapshots  []ThroughputSnapshot
    startTime  time.Time
    stopChan   chan struct{} // 用于停止定时器
}

// 启动定时统计
func (kvc *KVClient) startPeriodicStats() {
    kvc.startTime = time.Now()
    kvc.stopChan = make(chan struct{})
    
    go func() {
        ticker := time.NewTicker(*interval)
        defer ticker.Stop()
        
        for {
            select {
            case <-ticker.C:
                kvc.takeSnapshot()
            case <-kvc.stopChan:
                // 最后取一次快照
                kvc.takeSnapshot()
                return
            }
        }
    }()
}

// 停止定时统计
func (kvc *KVClient) stopPeriodicStats() {
    close(kvc.stopChan)
}

// 取一次快照
func (kvc *KVClient) takeSnapshot() {
    now := time.Now()
    ops := atomic.LoadInt64(&kvc.totalOps)
    bytes := atomic.LoadInt64(&kvc.totalBytes)
    dataMB := float64(bytes) / 1000000.0
    relativeTime := now.Sub(kvc.startTime).Seconds()
    
    snapshot := ThroughputSnapshot{
        Timestamp:        now,
        RelativeTime:     relativeTime,
        CumulativeOps:    ops,
        CumulativeDataMB: dataMB,
    }
    
    kvc.snapshotMu.Lock()
    kvc.snapshots = append(kvc.snapshots, snapshot)
    kvc.snapshotMu.Unlock()
}

// 简化版的批量写入测试
func (kvc *KVClient) batchRawPut(value string) {
    wg := sync.WaitGroup{}
    base := *dnums / *cnums
    wg.Add(*cnums)
    
    // 初始化快照切片
    kvc.snapshots = make([]ThroughputSnapshot, 0, 100000)
    
    // 值大小（字节）
    valueSize := int64(len(value))
    
    // 生成随机键
    allKeys := generateUniqueRandomInts(0, 6250000)

    // 启动定时统计
    kvc.startPeriodicStats()

    // 开始多线程测试
    for i := 0; i < *cnums; i++ {
        go func(i int) {
            defer wg.Done()
            
            // 确定当前线程处理的键范围
            start := i * base
            end := (i + 1) * base
            if i == *cnums-1 {
                end = *dnums
            }
            keys := allKeys[start:end]
            
            // 执行写入操作
            for j := 0; j < len(keys); j++ {
                key := strconv.Itoa(keys[j])
                
                // 执行写入
                reply, err := kvc.PutInRaft(key, value)
                
                // 如果操作成功，更新统计
                if err == nil && reply != nil && reply.Err != "defeat" {
                    // 原子递增操作数和字节数
                    atomic.AddInt64(&kvc.totalOps, 1)
                    atomic.AddInt64(&kvc.totalBytes, valueSize)
                }
            }
        }(i)
    }
    
    // 等待所有线程完成
    wg.Wait()
    
    // 停止定时统计
    kvc.stopPeriodicStats()
    
    // 等待一小段时间确保最后的快照被记录
    time.Sleep(50 * time.Millisecond)
    
    // 关闭所有连接池
    for _, pool := range kvc.pools {
        pool.Close()
        util.DPrintf("The raft pool has been closed")
    }
}

// 将快照数据保存到CSV文件
func (kvc *KVClient) saveSnapshotsToCSV(filename string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    // 写入CSV头
    _, err = fmt.Fprintln(file, "RelativeTimeSec,CumulativeOps,CumulativeDataMB")
    if err != nil {
        return err
    }
    
    // 写入快照数据
    kvc.snapshotMu.Lock()
    defer kvc.snapshotMu.Unlock()
    
    for _, snapshot := range kvc.snapshots {
        _, err = fmt.Fprintf(file, "%.3f,%d,%.6f\n", 
            snapshot.RelativeTime, 
            snapshot.CumulativeOps, 
            snapshot.CumulativeDataMB)
        if err != nil {
            return err
        }
    }
    
    return nil
}

func generateUniqueRandomInts(min, max int) []int {
    nums := make([]int, max-min+1)
    for i := range nums {
        nums[i] = min + i
    }
    rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
    return nums
}

// Method of Send RPC of PutInRaft
func (kvc *KVClient) PutInRaft(key string, value string) (*kvrpc.PutInRaftResponse, error) {
    request := &kvrpc.PutInRaftRequest{
        Key:      key,
        Value:    value,
        Op:       "Put",
        ClientId: kvc.clientId,
        SeqId:    atomic.AddInt64(&kvc.seqId, 1),
    }
    for {
        p := kvc.pools[kvc.leaderId] // 拿到leaderid对应的那个连接池
        conn, err := p.Get()
        if err != nil {
            util.EPrintf("failed to get conn: %v", err)
        }
        defer conn.Close()
        client := kvrpc.NewKVClient(conn.Value())
        ctx, cancel := context.WithTimeout(context.Background(), time.Second*120) // 设置120秒定时往下传
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

func (kvc *KVClient) changeToLeader(Id int) (leaderId int) {
    kvc.mu.Lock()
    defer kvc.mu.Unlock()
    kvc.leaderId = Id
    return kvc.leaderId
}

func (kvc *KVClient) InitPool() {
    // 这就是自己修改grpc线程池option参数的做法
    DesignOptions := pool.Options{
        Dial:                 pool.Dial,
        MaxIdle:              150,
        MaxActive:            300,
        MaxConcurrentStreams: 800,
        Reuse:                true,
    }
    fmt.Printf("servers:%v\n", kvc.Kvservers)
    // 根据servers的地址，创建了一一对应server地址的grpc连接池
    for i := 0; i < len(kvc.Kvservers); i++ {
        peers_single := []string{kvc.Kvservers[i]}
        p, err := pool.New(peers_single, DesignOptions)
        if err != nil {
            util.EPrintf("failed to new pool: %v", err)
        }
        // grpc连接池组
        kvc.pools = append(kvc.pools, p)
    }
}

func nrand() int64 { //随机生成clientId
    max := big.NewInt(int64(1) << 62)
    bigx, _ := crand.Int(crand.Reader, max)
    x := bigx.Int64()
    return x
}

func main() {
    flag.Parse()
    valueSize := *vsize
    servers := strings.Split(*ser, ",")
    kvc := new(KVClient)
    kvc.Kvservers = servers
    kvc.clientId = nrand()

    value := util.GenerateLargeValue(valueSize)
    kvc.InitPool()

    // 记录开始时间
    startTime := time.Now()
    
    // 运行测试
    kvc.batchRawPut(value)
    
    // 计算耗时
    elapsedTime := time.Since(startTime)

    // 计算统计信息
    totalBytes := atomic.LoadInt64(&kvc.totalBytes)
    totalOps := atomic.LoadInt64(&kvc.totalOps)
    totalMB := float64(totalBytes) / 1000000
    actualThroughputMB := totalMB / elapsedTime.Seconds()
    avgLatency := elapsedTime / time.Duration(totalOps)

    // 输出结果
    fmt.Printf("\n测试结果总结:\n")
    fmt.Printf("运行时间: %v\n", elapsedTime)
    fmt.Printf("实际平均吞吐量: %.4f MB/S\n", actualThroughputMB)
    fmt.Printf("总请求数: %v\n", *dnums)
    fmt.Printf("成功请求数: %v\n", totalOps)
    fmt.Printf("值大小: %v 字节\n", *vsize)
    fmt.Printf("客户端线程数: %v\n", *cnums)
    fmt.Printf("总数据大小: %.2f MB\n", totalMB)
    fmt.Printf("平均延迟: %v\n", avgLatency)
    fmt.Printf("统计间隔: %v\n", *interval)
    fmt.Printf("收集的快照数: %d\n", len(kvc.snapshots))

    // 保存快照数据到CSV文件
    err := kvc.saveSnapshotsToCSV(*csvFile)
    if err != nil {
        fmt.Printf("保存CSV文件时出错: %v\n", err)
    } else {
        fmt.Printf("吞吐量数据已保存到: %s\n", *csvFile)
        fmt.Printf("CSV文件包含 %d 个快照数据点\n", len(kvc.snapshots))
        fmt.Printf("可以用这个数据分析吞吐量趋势和计算不同时间点的延迟\n")
    }
}