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
	"gitee.com/dong-shuishui/FlexSync/pool"
	"gitee.com/dong-shuishui/FlexSync/raft"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
	crand "crypto/rand"
	"math/big"
)

// 保留原有的标志定义
var (
	ser   = flag.String("servers", "", "the Server, Client Connects to")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	key   = flag.Int("key", 6, "target key")
)
type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64 // 客户端唯一标识
	seqId     int64 // 该客户端单调递增的请求id
	leaderId  int

	pools   []pool.Pool
	goodPut int // 有效吞吐量
}

func (kvc *KVClient) randRead() {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	// last := 0
	kvc.goodPut = 0

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			num := 0
			rand.Seed(time.Now().Unix())
			for j := 0; j < base; j++ {
				key := rand.Intn(100000000)
				//k := base*i + j
				// key := fmt.Sprintf("key_%d", k)
				targetkey := strconv.Itoa(key)
				//fmt.Printf("Goroutine %v put key: key_%v\n", i, k)
				time.Sleep(300 * time.Millisecond)
				_, keyExist, err := kvc.Get(targetkey) // 先随机传入一个地址的连接池
				// fmt.Println("after putinraft , j:",j)
				if err == nil {
					kvc.goodPut++
					// fmt.Println("点查询key为：",key)
				}
				// if err == nil && keyExist {
					// kvc.goodPut++
					// fmt.Printf("Got the value:** corresponding to the key:%v === exist\n ", key)
				// }
				if !keyExist {
					// kvc.PutInRaft(targetkey, value) // 找到不存在的，先随便弥补一个键值对
					// fmt.Printf("Got the value:%v corresponding to the key:%v === nokey\n ", value, key)
				}
				if j >= num+100 {
					num = j
					// fmt.Printf("Goroutine %v put key num: %v\n", i, num)
				}
			}
		}(i)
	}
	wg.Wait()
	for _, pool := range kvc.pools {
		pool.Close()
		util.DPrintf("The raft pool has been closed")
	}
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
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
			// fmt.Println("can not connect ", kvc.Kvservers[targetId], "or it's not leader")
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
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5) // 设置5秒定时往下传
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

func runTest() float64 {
	flag.Parse()
	servers := strings.Split(*ser, ",")
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool()
	startTime := time.Now()
	kvc.randRead()
	valuesize := 64000

	sum_Size_MB := float64(kvc.goodPut*valuesize) / 1000000
	throughput := float64(sum_Size_MB) / time.Since(startTime).Seconds()
	
	fmt.Printf("Elapse: %v, Throughput: %.4f MB/S, Total: %v, GoodPut: %v, Value: %v, Client: %v, Size: %.2f MB\n",
		time.Since(startTime), throughput, *dnums, kvc.goodPut, valuesize, *cnums, sum_Size_MB)

	return throughput
}

func main() {
	numTests := 20
	var totalThroughput float64

	for i := 0; i < numTests; i++ {
		fmt.Printf("\nRunning test %d of %d\n", i+1, numTests)
		throughput := runTest()
		totalThroughput += throughput

		if i < numTests-1 {
			fmt.Println("Waiting 5 seconds before next test...")
			time.Sleep(5 * time.Second)
		}
	}

	averageThroughput := totalThroughput / float64(numTests)
	fmt.Printf("\nAverage throughput over %d tests: %.4f MB/S\n", numTests, averageThroughput)
}