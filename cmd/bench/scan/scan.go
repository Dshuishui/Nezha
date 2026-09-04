package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"

	// "strconv"
	"context"
	"strings"
	"sync"

	// "sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"

	// raftrpc "gitee.com/dong-shuishui/FlexSync/rpc/Raftrpc"
	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	crand "crypto/rand"
	"math/big"
	// "google.golang.org/grpc"
)

var (
	ser = flag.String("servers", "", "the Server, Client Connects to")
	// mode     = flag.String("mode", "RequestRatio", "Read or Put and so on")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	// getratio = flag.Int("getratio", 1, "Get Times per Put Times")
	k1 = flag.Int("startkey", 0, "first key")
	k2 = flag.Int("endkey", 20, "last key")
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

// randread
func (kvc *KVClient) scan(gapkey int) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	// last := 0
	kvc.goodPut = 0

	// 使用通道来收集每个 goroutine 的结果
	results := make(chan int, *cnums)

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			localGoodPut := 0                           // 本地变量，用于统计当前 goroutine 的 goodPut
			rand.Seed(time.Now().UnixNano() + int64(i)) // 使用不同的种子
			for j := 0; j < base; j++ {
				k1 := rand.Intn(100000)
				k2 := k1 + gapkey
				startKey := strconv.Itoa(k1)
				endKey := strconv.Itoa(k2)
				// 生成随机的startKey和endKey
				// startKey := fmt.Sprintf("key_%d", k1)
				// endKey := fmt.Sprintf("key_%d", k2)
				// 确保startKey小于endKey
				if startKey > endKey {
					startKey, endKey = endKey, startKey
				}
				//fmt.Printf("Goroutine %v put key: key_%v\n", i, k)
				reply, err := kvc.rangeGet(startKey, endKey) // 先随机传入一个地址的连接池
				// fmt.Println("after putinraft , j:",j)
				if err == nil {
					// fmt.Printf("got the key range %v-%v\n", startKey, endKey)
					// kvc.goodPut++
					// 统计所有的scan中读取到的有效值
					localGoodPut += len(reply.KeyValuePairs)
					if localGoodPut%100 == 1 {
						fmt.Println("这个goroutine的数量为多少：", localGoodPut)
					}
				}
				// if j >= num+100 {
				// num = j
				// fmt.Printf("Goroutine %v put key num: %v\n", i, num)
				// }
				// fmt.Printf("This the result of scan:%+v\n", reply)
				// fmt.Printf("got the key range %v-%v",startKey,endKey)
			}
			// 将本地结果发送到通道
			results <- localGoodPut
		}(i)
	}
	// 等待所有 goroutine 完成
	go func() {
		wg.Wait()
		close(results)
	}()
	// 统计总的 goodPut
	totalGoodPut := 0
	for result := range results {
		totalGoodPut += result
	}

	kvc.goodPut = totalGoodPut
	for _, pool := range kvc.pools {
		pool.Close()
		util.DPrintf("The raft pool has been closed")
	}
}

func (kvc *KVClient) rangeGet(key1 string, key2 string) (*kvrpc.ScanRangeResponse, error) {
	args := &kvrpc.ScanRangeRequest{
		StartKey: key1,
		EndKey:   key2,
	}
	for {
		p := kvc.pools[kvc.leaderId] // 拿到leaderid对应的那个连接池
		// p := kvc.pools[rand.Intn(len(kvc.Kvservers))]		// 随机对一个server进行scan查询
		conn, err := p.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
		}
		defer conn.Close()
		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()
		reply, err := client.ScanRangeInRaft(ctx, args)
		if err != nil {
			util.EPrintf("err in ScanRangeInRaft: %v", err)
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
	// dataNum := *dnums
	// startkey := int32(*k1)
	// endkey := int32(*k2)
	gapkey := 100
	servers := strings.Split(*ser, ",")
	// fmt.Printf("servers:%v\n",servers)
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool() // 初始化grpc连接池
	startTime := time.Now()
	// 开始发送请求
	kvc.scan(gapkey)
	valuesize := 4000

	// sum_Size_MB := float64(kvc.goodPut*valuesize*gapkey) / 1000000
	// 由于上述kvc.goodput均为所有的scan中读取到的有效的key的数量，所以不用乘以gapkey
	sum_Size_MB := float64(kvc.goodPut*valuesize) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, goodPut %v, value %v, client %v, Size %vMB\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, kvc.goodPut, valuesize, *cnums, sum_Size_MB)
}
