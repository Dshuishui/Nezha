package main

import (
	"flag"
	"fmt"
	"math/rand"
	// "strconv"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	// kvc "github.com/JasonLou99/Hybrid_KV_Store/kvstore/kvclient"
	"github.com/JasonLou99/Hybrid_KV_Store/pool"
	"github.com/JasonLou99/Hybrid_KV_Store/raft"
	// raftrpc "github.com/JasonLou99/Hybrid_KV_Store/rpc/Raftrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	crand "crypto/rand"
	"math/big"
	"google.golang.org/grpc"
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

	pools []*pool.Pool
}

// batchRawPut blinds put bench.
func (kvc *KVClient) batchRawPut(value []byte) {
	// 这就是自己修改grpc线程池option参数的做法
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              64,
		MaxActive:            128,
		MaxConcurrentStreams: 64,
		Reuse:                true,
	}
	// fmt.Println("进入到batchRawPut函数")
	// 根据servers的地址，创建了一一对应server地址的grpc连接池
	for i := 0; i < len(kvc.Kvservers); i++ {
		// fmt.Println("进入到生成连接池的for循环")
		peers_single := []string{kvc.Kvservers[i]}
		p, err := pool.New(peers_single, DesignOptions)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		// grpc连接池组
		kvc.pools = append(kvc.pools, &p)
	}

	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	for i := 0; i < *cnums; i++ {
		go func() {
			defer wg.Done()

			// num := 0
			rand.Seed(time.Now().Unix())
			for j := 0; j < base; j++ {
				k := rand.Intn(*dnums)
				//k := base*i + j
				key := fmt.Sprintf("key_%d", k)
				//fmt.Printf("Goroutine %v put key: key_%v\n", i, k)
				kvc.PutInRaft(key, string(value), kvc.pools)		// 先随机传入一个地址的连接池
				// if j >= num+1000 {
				// 	num = j
				// 	fmt.Printf("Client %v put key num: %v\n", i+1, num)
				// }
			}
		}()
	}
	wg.Wait()
}

// Method of Send RPC of GetInRaft
func (kvc *KVClient) SendGetInRaft(address string, request *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		util.EPrintf("err in SendGetInRaft: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.GetInRaft(ctx, request)
	if err != nil {
		util.EPrintf("err in SendGetInRaft: %v", err)
		return nil, err
	}
	return reply, nil
}

func (kvc *KVClient) Get(key string) (string,bool) {
	args := &kvrpc.GetInRaftRequest{		
		Key: key,
		ClientId: kvc.clientId,
		SeqId: atomic.AddInt64(&kvc.seqId, 1),
	}

	for {
		reply, err := kvc.SendGetInRaft(kvc.Kvservers[kvc.leaderId], args)
		if err!=nil {
			util.EPrintf("can not connect ", kvc.Kvservers[kvc.leaderId], "or it's not leader")
			return "",false
		}
		if reply.Err==raft.OK {
			return reply.Value,true
		}else if reply.Err == raft.ErrNoKey{
			return "",false
		}else if reply.Err == raft.ErrWrongLeader{
			kvc.changeToLeader(int(reply.LeaderId))
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// Method of Send RPC of PutInRaft
func (kvc *KVClient) PutInRaft(key string, value string, pools []*pool.Pool) (*kvrpc.PutInRaftResponse, error) {
	request := &kvrpc.PutInRaftRequest{
		Key: key,
		Value: value,
		Op: "put",
		ClientId: kvc.clientId,
		SeqId: atomic.AddInt64(&kvc.seqId, 1),
	}
	for{
		// conn, err := grpc.Dial(kvc.Kvservers[kvc.leaderId], grpc.WithInsecure(), grpc.WithBlock())
		// if err != nil {
		// 	util.EPrintf("failed to get conn: %v", err)
		// }
		// defer conn.Close()
		p := pools[kvc.leaderId]
		pp := *p	// 拿到leaderid对应的那个连接池
		conn, err := pp.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
		}
		defer conn.Close()
		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*100) // 设置100秒定时往下传
		defer cancel()

		reply, err := client.PutInRaft(ctx, request)
		if err != nil {
			fmt.Println("客户端调用PutInRaft有问题")
			util.EPrintf("err in PutInRaft-调用了服务器的put方法: %v", err)
			return nil, err
		}
		if reply.Err == raft.OK {
			return reply, nil
		}else if reply.Err ==raft.ErrWrongLeader{
			kvc.changeToLeader(int(reply.LeaderId))
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (kvc *KVClient) changeToLeader(Id int) (leaderId int) {
	kvc.mu.Lock()
	defer kvc.mu.Unlock()
	kvc.leaderId = Id
	return kvc.leaderId
}

func nrand() int64 {		//随机生成clientId
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func main() {
	flag.Parse()
	dataNum := *dnums
	valueSize := *vsize
	servers := strings.Split(*ser, ",")
	// fmt.Printf("servers:%v\n",servers)
	kvc := new(KVClient)
	kvc.Kvservers = make([]string, len(servers))
	// for i := 0; i < len(servers); i++ {
	// 	kvc.Kvservers[i] = servers[i] + "1"
	// }
	kvc.clientId = nrand()

	value := make([]byte, *vsize)
	startTime := time.Now()
	// 开始发送请求
	kvc.batchRawPut(value)

	sum_Size_MB := float64(dataNum*valueSize) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, value %v, client %v\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, *vsize, *cnums)
}