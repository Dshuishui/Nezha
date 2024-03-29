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

	// kvc "github.com/JasonLou99/Hybrid_KV_Store/kvstore/kvclient"
	"github.com/JasonLou99/Hybrid_KV_Store/pool"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"
)

var (
	ser = flag.String("servers", "", "the Server, Client Connects to")
	// mode     = flag.String("mode", "RequestRatio", "Read or Put and so on")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	// getratio = flag.Int("getratio", 1, "Get Times per Put Times")
	vsize = flag.Int("vsize", 64, "value size in type")
)

// batchRawPut blinds put bench.
func batchRawPut(value []byte) {
	servers := strings.Split(*ser, ",")

	// 这就是自己修改grpc线程池option参数的做法
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              64,
		MaxActive:            128,
		MaxConcurrentStreams: 64,
		Reuse:                true,
	}
	p, err := pool.New(servers, DesignOptions) // 先把这个连接池里面的地址固定，后面需要改new函数里面的生成tcp连接的方法
	if err != nil {
		util.EPrintf("failed to new pool: %v", err)
	}
	defer p.Close()

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
				PutInCausal(key, string(value), p)
				// if j >= num+1000 {
				// 	num = j
				// 	fmt.Printf("Client %v put key num: %v\n", i+1, num)
				// }
			}
		}()
	}
	wg.Wait()
}

// Method of Send RPC of PutInCausal
func PutInCausal(key string, value string, p pool.Pool) (*kvrpc.PutInCausalResponse, error) {
	request := &kvrpc.PutInCausalRequest{
		Key:   key,
		Value: value,
	}

	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()

	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1000) // 设置五秒定时往下传
	defer cancel()
	reply, err := client.PutInCausal(ctx, request)
	if err != nil {
		fmt.Println("客户端调用PutInCausal有问题")
		util.EPrintf("err in SendPutInCausal-调用了服务器的put方法: %v", err)
		return nil, err
	}
	return reply, nil
}

func main() {
	flag.Parse()

	value := make([]byte, *vsize)
	startTime := time.Now()
	batchRawPut(value)
	dataNum := *dnums
	valueSize := *vsize
	sum_Size_MB := float64(dataNum*valueSize) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, value %v, client %v\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, *vsize, *cnums)
}
