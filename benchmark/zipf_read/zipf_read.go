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
	ser   = flag.String("servers", "", "the Server, Client Connects to")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	key   = flag.Int("key", 6, "target key")
)

type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64
	seqId     int64
	leaderId  int
	pools     []pool.Pool
	goodPut   int
	valuesize int
}

type getResult struct {
	count         int
	avgLatency    time.Duration
	totalDataSize float64
	valueSize     int
	duration      time.Duration
}

const (
	KEY_SPACE = 6400000 // 键空间大小
	ZIPF_S    = 1.01   // Zipf 分布的偏度参数
	ZIPF_V    = 1      // 最小值
)

func (kvc *KVClient) randRead() (float64, time.Duration) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0

	resultChan := make(chan getResult, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			localResult := getResult{}

			// 为每个 goroutine 创建独立的随机数生成器
			source := rand.NewSource(time.Now().UnixNano() + int64(i))
			rnd := rand.New(source)

			// 创建 Zipf 分布，使用更保守的参数
			zipf := rand.NewZipf(rnd, ZIPF_S, ZIPF_V, uint64(KEY_SPACE-1))
			if zipf == nil {
				fmt.Printf("Failed to create Zipf distribution with parameters: s=%.2f, v=%d, imax=%d\n",
					ZIPF_S, ZIPF_V, KEY_SPACE-1)
				return
			}

			startTime := time.Now()
			for j := 0; j < base; j++ {
				// 使用 Zipf 分布生成键
				keyNum := zipf.Uint64() % uint64(KEY_SPACE) // 确保键在有效范围内
				targetKey := strconv.FormatUint(keyNum, 10)

				value, keyExist, err := kvc.Get(targetKey)
				if err == nil && keyExist && value != "ErrNoKey" {
					localResult.count++
					localResult.valueSize = len([]byte(value))
				}
				// fmt.Printf("exist? %v\n",keyExist)
			}

			localResult.duration = time.Since(startTime)
			if localResult.count > 0 {
				localResult.avgLatency = localResult.duration / time.Duration(localResult.count)
				localResult.totalDataSize = float64(localResult.count*localResult.valueSize) / 1000000
			}
			resultChan <- localResult
		}(i)
	}

	wg.Wait()
	close(resultChan)

	var maxDuration time.Duration
	var totalData float64
	var totalAvgLatency time.Duration
	var totalCount int
	goroutineCount := 0

	for result := range resultChan {
		if result.count > 0 {
			if result.duration > maxDuration {
				maxDuration = result.duration
			}
			totalData += result.totalDataSize
			totalAvgLatency += result.avgLatency
			kvc.valuesize = result.valueSize
			goroutineCount++
		}
		totalCount += result.count
	}

	kvc.goodPut = totalCount

	if maxDuration > 0 {
		throughput := totalData / maxDuration.Seconds()
		avgLatency := totalAvgLatency
		if goroutineCount > 0 {
			avgLatency = totalAvgLatency / time.Duration(goroutineCount)
		}

		for _, pool := range kvc.pools {
			pool.Close()
			util.DPrintf("The raft pool has been closed")
		}

		return throughput, avgLatency
	}

	return 0, 0
}

// Method of Send RPC of GetInRaft
func (kvc *KVClient) SendGetInRaft(targetId int, request *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	// p := kvc.pools[rand.Intn(len(kvc.Kvservers))]		// 随机对一个server进行scan查询
	// fmt.Printf("拿出连接池对应的地址为%v",p.GetAddress())
	p := kvc.pools[targetId] // 拿到leaderid对应的那个连接池
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	reply, err := client.GetInRaft(ctx, request)
	if err != nil {
		// util.EPrintf("err in SendGetInRaft: %v, address:%v", err, kvc.Kvservers[targetId])
		return nil, err
	}
	return reply, nil
	// 1、直接先调用raft层的方法，用来获取当前leader的commitindex
	// 2、raft层的方法，通过grpc发送消息给leaderid的节点，首先判读是否是自己，是自己就当leader处理。
	// 3、不是自己就通过grpc调用leader的方法获取commmitindex的方法，leader要先发送心跳，根据收到的回复，再回复commitindex，这里先假设都不会过期。如果过期了，需要返回leaderid给follow，重新执行并替换第二部的leaderid
	// 4、拿到commitindex后，等待applyindex大于等于commitindex。再执行get请求。
	// 5、如果是自己：直接调用上述leader获取commitindex的方法，拿到commitindex后直接执行get请求。
}

func (kvc *KVClient) Get(key string) (string, bool, error) {
	args := &kvrpc.GetInRaftRequest{
		Key:      key,
		ClientId: kvc.clientId,
		SeqId:    atomic.AddInt64(&kvc.seqId, 1),
	}
	// targetId := rand.Intn(len(kvc.Kvservers))		// 理论上是先随机找个目标服务器
	targetId := kvc.leaderId // 这里先固定为leader
	for {
		reply, err := kvc.SendGetInRaft(targetId, args)
		if err != nil {
			// fmt.Println("can   not connect ", kvc.Kvservers[targetId], "or it's not leader")
			return "", false, err
		}
		if reply.Err == raft.OK {
			return reply.Value, true, nil
		} else if reply.Err == raft.ErrNoKey {
			return reply.Value, false, nil
		} else if reply.Err == raft.ErrWrongLeader {
			// kvc.changeToLeader(int(reply.LeaderId))
			targetId = int(reply.LeaderId)
			time.Sleep(1 * time.Millisecond)
		}
	}
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*50) // 设置5秒定时往下传
		defer cancel()

		reply, err := client.PutInRaft(ctx, request)
		if err != nil {
			// fmt.Println("客户端调用PutInRaft有问题")
			util.EPrintf("err in PutInRaft-调用了服务器的put方法: %v", err)
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

func runTest() (float64, time.Duration) {
	flag.Parse()
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool()
	startTime := time.Now()
	throughput, averageLatency := kvc.randRead()

	elapsedTime := time.Since(startTime)
	sum_Size_MB := float64(kvc.goodPut*kvc.valuesize) / 1000000

	fmt.Printf("Elapse: %v, Throughput: %.4f MB/S, Total: %v, GoodPut: %v, Value: %v, Client: %v, Size: %.2f MB, Average Latency: %v\n",
		elapsedTime, throughput, *dnums, kvc.goodPut, kvc.valuesize, *cnums, sum_Size_MB, averageLatency)

	return throughput, averageLatency
}

func main() {
	numTests := 10
	var totalThroughput float64
	var totalAverageLatency time.Duration

	for i := 0; i < numTests; i++ {
		fmt.Printf("\n运行测试 %d / %d\n", i+1, numTests)
		throughput, averageLatency := runTest()
		totalThroughput += throughput
		totalAverageLatency += averageLatency

		if i < numTests-1 {
			fmt.Println("等待5秒后进行下一次测试...")
			time.Sleep(5 * time.Second)
		}
	}

	averageThroughput := totalThroughput / float64(numTests)
	overallAverageLatency := totalAverageLatency / time.Duration(numTests)
	fmt.Printf("\n%d 次测试的平均吞吐量: %.4f MB/S\n", numTests, averageThroughput)
	fmt.Printf("%d 次测试的总平均延迟: %v\n", numTests, overallAverageLatency)
}
