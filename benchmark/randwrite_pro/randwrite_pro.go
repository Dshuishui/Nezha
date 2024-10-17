package main

import (
	"flag"
	"fmt"
	"math/rand"

	"strconv"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// kvc "gitee.com/dong-shuishui/FlexSync/kvstore/kvclient"
	"gitee.com/dong-shuishui/FlexSync/pool"
	"gitee.com/dong-shuishui/FlexSync/raft"

	// raftrpc "gitee.com/dong-shuishui/FlexSync/rpc/Raftrpc"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

	crand "crypto/rand"
	"math/big"
)

var (
	ser = flag.String("servers", "", "the Server, Client Connects to")
	// mode     = flag.String("mode", "RequestRatio", "Read or Put and so on")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	// getratio = flag.Int("getratio", 1, "Get Times per Put Times")
	vsize = flag.Int("vsize", 64, "value size in type")
)

type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64 // 客户端唯一标识
	seqId     int64 // 该客户端单调递增的请求id
	leaderId  int

	pools   []pool.Pool
	goodPut int // 有效吞吐量
	totalLatency time.Duration // 添加总延迟字段
}

// func (kvc *KVClient) batchRawPut(value string) {
//     wg := sync.WaitGroup{}
//     base := *dnums / *cnums
//     wg.Add(*cnums)
    
//     // Create a channel to collect results from goroutines
//     resultChan := make(chan int, *cnums)

//     for i := 0; i < *cnums; i++ {
//         go func(i int) {
//             defer wg.Done()
//             localGoodPut := 0
//             rand.Seed(time.Now().UnixNano())
//             for j := 0; j < base; j++ {
//                 key := util.GenerateFixedSizeKey(5)
//                 reply, err := kvc.PutInRaft(key, value)
//                 if err == nil && reply != nil && reply.Err != "defeat" {
//                     localGoodPut++
//                 }
//             }
//             // Send the local result to the channel
//             resultChan <- localGoodPut
//         }(i)
//     }

//     // Close the result channel when all goroutines are done
//     go func() {
//         wg.Wait()
//         close(resultChan)
//     }()

//     // Collect and sum up the results
//     totalGoodPut := 0
//     for localGoodPut := range resultChan {
//         totalGoodPut += localGoodPut
//     }

//     kvc.goodPut = totalGoodPut

//     for _, pool := range kvc.pools {
//         pool.Close()
//         util.DPrintf("The raft pool has been closed")
//     }
// }

// batchRawPut blinds put bench.
func (kvc *KVClient) batchRawPut(value string) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0
	kvc.totalLatency = 0 // 重置总延迟

	// 为每个goroutine创建一个唯一的随机数生成器
	randomGens := make([]*rand.Rand, *cnums)
	for i := range randomGens {
		randomGens[i] = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	// 生成一个包含所有可能key的切片
	// allKeys := generateUniqueRandomInts(*dnums+5000000,*dnums+10000000)
	allKeys := generateUniqueRandomInts(12000000,60000000)

	type putResult struct {
		goodPut int
		latency time.Duration
	}
	results := make(chan putResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
            defer wg.Done()
			localResult := putResult{}
            
            // 为每个goroutine分配一部分key
            start := i * base
            end := (i + 1) * base
            if i == *cnums-1 {
                end = *dnums // 确保最后一个goroutine使用所有剩余的key
            }
            keys := allKeys[start:end]
            
            // 打乱这部分key的顺序
            // randomGens[i].Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

            for j := 0; j < base; j++ {
                if j >= len(keys) {
                    break // 防止越界
                }
                key := strconv.Itoa(keys[j])
                startTime := time.Now()
				reply, err := kvc.PutInRaft(key, value)
				duration := time.Since(startTime)
				if err == nil && reply != nil && reply.Err != "defeat" {
					localResult.goodPut++
					localResult.latency += duration
				}
            }
			results <- localResult
        }(i)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	for result := range results {
		kvc.goodPut += result.goodPut
		kvc.totalLatency += result.latency
	}

	for _, pool := range kvc.pools {
		pool.Close()
		util.DPrintf("The raft pool has been closed")
	}
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
		// conn, err := grpc.Dial(kvc.Kvservers[kvc.leaderId], grpc.WithInsecure(), grpc.WithBlock())
		// if err != nil {
		// 	util.EPrintf("failed to get conn: %v", err)
		// }
		// defer conn.Close()
		p := kvc.pools[kvc.leaderId] // 拿到leaderid对应的那个连接池
		// fmt.Printf("拿出连接池对应的地址为%v",p.GetAddress())
		conn, err := p.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
		}
		defer conn.Close()
		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*1) // 设置4秒定时往下传
		defer cancel()

		reply, err := client.PutInRaft(ctx, request)
		if err != nil {
			// fmt.Println("客户端调用PutInRaft有问题")
			// util.EPrintf("err in PutInRaft-调用了服务器的put方法: %v", err)
			// util.EPrintf("seqid：%v, err in PutInRaft-调用了服务器的put方法: %v",request.SeqId, err)
			// 这里防止服务器是宕机了，所以要change leader
			return nil, err
		}
		if reply.Err == raft.OK {
			// fmt.Printf("找到了leader %v\n",kvc.leaderId)
			return reply, nil
		} else if reply.Err == raft.ErrWrongLeader {
			kvc.changeToLeader(int(reply.LeaderId))
			// fmt.Printf("等待leader的出现,更改后的leaderid是%v\n",kvc.leaderId)
			// time.Sleep(6 * time.Millisecond)
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
		// fmt.Println("进入到生成连接池的for循环")
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
	startTime := time.Now()
	
	kvc.batchRawPut(value)

	elapsedTime := time.Since(startTime)
	sum_Size_MB := float64(kvc.goodPut*valueSize) / 1000000
	throughput := float64(sum_Size_MB) / elapsedTime.Seconds()
	avgLatency := kvc.totalLatency / time.Duration(kvc.goodPut)

	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, avg latency:%v, total %v, goodPut %v, value %v, client %v, Size %vMB\n",
		elapsedTime, throughput, avgLatency, *dnums, kvc.goodPut, *vsize, *cnums, sum_Size_MB)
}
