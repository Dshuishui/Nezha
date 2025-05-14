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
    csvFile = flag.String("csvfile", "raw_latency_data.csv", "CSV file to store raw latency data")
)

// 简单的操作记录结构
type OperationRecord struct {
    Timestamp    time.Time     // 操作时间戳
    Latency      time.Duration // 操作时延
    DataSize     int64         // 累积写入的数据量（字节）
}

type KVClient struct {
    Kvservers []string
    mu        sync.Mutex
    clientId  int64 // 客户端唯一标识
    seqId     int64 // 该客户端单调递增的请求id
    leaderId  int

    pools     []pool.Pool
    goodPut   int32 // 改为int32类型，使用原子操作
    
    // 记录器相关
    recordMu   sync.Mutex
    records    []OperationRecord
    totalBytes int64 // 原子计数器，记录总写入字节数
}

// 记录一次写操作
func (kvc *KVClient) recordOperation(latency time.Duration) {
    bytes := atomic.LoadInt64(&kvc.totalBytes)
    
    kvc.recordMu.Lock()
    defer kvc.recordMu.Unlock()
    
    kvc.records = append(kvc.records, OperationRecord{
        Timestamp: time.Now(),
        Latency:   latency,
        DataSize:  bytes,
    })
}

// 简化版的批量写入测试
func (kvc *KVClient) batchRawPut(value string) {
    wg := sync.WaitGroup{}
    base := *dnums / *cnums
    wg.Add(*cnums)
    
    // 初始化记录
    kvc.records = make([]OperationRecord, 0, *dnums)
    
    // 值大小（字节）
    valueSize := int64(len(value))
    
    // 生成随机键
    allKeys := generateUniqueRandomInts(0, 6250000)

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
                
                // 记录操作开始时间
                opStartTime := time.Now()
                
                // 执行写入
                reply, err := kvc.PutInRaft(key, value)
                
                // 如果操作成功，记录时延
                if err == nil && reply != nil && reply.Err != "defeat" {
                    // 计算操作时延
                    opLatency := time.Since(opStartTime)
                    
                    // 更新总字节数并记录操作
                    atomic.AddInt64(&kvc.totalBytes, valueSize)
                    kvc.recordOperation(opLatency)
                    
                    // 原子递增成功计数
                    atomic.AddInt32(&kvc.goodPut, 1)
                }
            }
        }(i)
    }
    
    // 等待所有线程完成
    wg.Wait()
    
    // 关闭所有连接池
    for _, pool := range kvc.pools {
        pool.Close()
        util.DPrintf("The raft pool has been closed")
    }
}

// 将原始操作记录保存到CSV文件
func (kvc *KVClient) saveRawDataToCSV(filename string) error {
    file, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer file.Close()
    
    // 写入CSV头
    _, err = fmt.Fprintln(file, "Timestamp,LatencyMs,CumulativeDataMB")
    if err != nil {
        return err
    }
    
    // 获取开始时间作为相对时间基准
    var baseTime time.Time
    if len(kvc.records) > 0 {
        baseTime = kvc.records[0].Timestamp
    } else {
        baseTime = time.Now()
    }
    
    // 写入数据
    for _, record := range kvc.records {
        // 将纳秒转换为毫秒
        latencyMs := float64(record.Latency.Nanoseconds()) / 1000000.0
        // 将字节转换为MB
        dataMB := float64(record.DataSize) / 1000000.0
        // 计算相对时间（秒）
        relativeTimeSec := record.Timestamp.Sub(baseTime).Seconds()
        
        _, err = fmt.Fprintf(file, "%f,%f,%f\n", relativeTimeSec, latencyMs, dataMB)
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

func (kvc *KVClient) changeToLeader(Id int) (leaderId int) {
    kvc.mu.Lock()
    defer kvc.mu.Unlock()
    kvc.leaderId = Id
    return kvc.leaderId
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
    totalMB := float64(totalBytes) / 1000000
    actualThroughputMB := totalMB / elapsedTime.Seconds()
    goodPut := atomic.LoadInt32(&kvc.goodPut)

    // 输出结果
    fmt.Printf("\n测试结果总结:\n")
    fmt.Printf("运行时间: %v\n", elapsedTime)
    fmt.Printf("实际平均吞吐量: %.4f MB/S\n", actualThroughputMB)
    fmt.Printf("总请求数: %v\n", *dnums)
    fmt.Printf("成功请求数: %v\n", goodPut)
    fmt.Printf("值大小: %v 字节\n", *vsize)
    fmt.Printf("客户端线程数: %v\n", *cnums)
    fmt.Printf("总数据大小: %.2f MB\n", totalMB)
    fmt.Printf("收集的操作记录数: %d\n", len(kvc.records))

    // 保存原始数据到CSV文件
    err := kvc.saveRawDataToCSV(*csvFile)
    if err != nil {
        fmt.Printf("保存CSV文件时出错: %v\n", err)
    } else {
        fmt.Printf("原始延迟数据已保存到: %s\n", *csvFile)
        fmt.Printf("CSV文件包含 %d 个数据点\n", len(kvc.records))
    }
}