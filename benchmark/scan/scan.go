package main

import (
	"flag"
	"fmt"
	"math/rand"

	// "strconv"
	"context"
	"strings"
	"sync"

	// "sync/atomic"
	"time"

	// kvc "gitee.com/dong-shuishui/FlexSync/kvstore/kvclient"
	"gitee.com/dong-shuishui/FlexSync/pool"

	// raftrpc "gitee.com/dong-shuishui/FlexSync/rpc/Raftrpc"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

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
func (kvc *KVClient) scan(k1 int32, k2 int32) {
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
				// k1 := rand.Intn(*dnums)
				// k2 := rand.Intn(*dnums)
				startKey := int32(k1)
				endKey := int32(k2)
				// 生成随机的startKey和endKey
				// startKey := fmt.Sprintf("key_%d", k1)
				// endKey := fmt.Sprintf("key_%d", k2)
				// 确保startKey小于endKey
				if startKey > endKey {
					startKey, endKey = endKey, startKey
				}
				//fmt.Printf("Goroutine %v put key: key_%v\n", i, k)
				_, err := kvc.rangeGet(startKey, endKey) // 先随机传入一个地址的连接池
				// fmt.Println("after putinraft , j:",j)
				if err != nil {
					kvc.goodPut++
				}
				if j >= num+100 {
					num = j
					// fmt.Printf("Goroutine %v put key num: %v\n", i, num)
				}
				// fmt.Printf("This the result of scan:%+v\n",reply)
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
func (kvc *KVClient) SendRangeGetInRaft(address string, request *kvrpc.ScanRangeRequest) (*kvrpc.ScanRangeResponse, error) {
	// conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	// p := kvc.pools[kvc.leaderId] // 拿到leaderid对应的那个连接池		
	p := kvc.pools[rand.Intn(len(kvc.Kvservers))]		// 随机对一个server进行scan查询
	// fmt.Printf("拿出连接池对应的地址为%v",p.GetAddress())
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*6)
	defer cancel()
	reply, err := client.ScanRangeInRaft(ctx, request)
	if err != nil {
		util.EPrintf("err in SendGetInRaft: %v", err)
		return nil, err
	}
	return reply, nil
}

func (kvc *KVClient) rangeGet(key1 int32, key2 int32) (*kvrpc.ScanRangeResponse, error) {
	args := &kvrpc.ScanRangeRequest{
		StartKey: key1,
		EndKey:   key2,
	}
	reply, err := kvc.SendRangeGetInRaft(kvc.Kvservers[kvc.leaderId], args)
	if err != nil {
		util.EPrintf("can not connect ", kvc.Kvservers[kvc.leaderId], "or it's not leader")
		return nil, err
	}
	return reply, nil
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
	startkey := int32(*k1)
	endkey := int32(*k2)
	gapkey := int(endkey-startkey)
	servers := strings.Split(*ser, ",")
	// fmt.Printf("servers:%v\n",servers)
	kvc := new(KVClient)
	kvc.Kvservers = servers
	kvc.clientId = nrand()

	kvc.InitPool() // 初始化grpc连接池
	startTime := time.Now()
	// 开始发送请求
	kvc.scan(startkey,endkey)

	sum_Size_MB := float64(kvc.goodPut*256000*gapkey) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, goodPut %v, value %v, client %v, Size %vMB\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, kvc.goodPut, 256000, *cnums, sum_Size_MB)
}
