package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/client"
	"gitee.com/dong-shuishui/FlexSync/internal/util"
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
	c         *client.Client

	goodPut int // 有效吞吐量
}

// batchRawPut blinds put bench.
func (kvc *KVClient) batchRawPut(value string) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	// last := 0
	kvc.goodPut = 0

	// ticker := time.NewTicker(2 * time.Second)
	// defer ticker.Stop()
	// go func() {
	// 	for range ticker.C {
	// 		fmt.Printf("PutInRaft called %d times in the last 2 seconds\n", num-last)
	// 		last = num
	// 	}
	// }()
	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			rand.Seed(time.Now().UnixNano())
			for j := 0; j < base; j++ {
				key := i*base + j
				strkey := strconv.Itoa(key)

				// 添加重试逻辑
				maxRetries := 1                      // 最大重试次数
				retryDelay := time.Millisecond * 500 // 重试间隔

				for retry := 0; retry < maxRetries; retry++ {
					reply, err := kvc.c.Put(strkey, value)

					if err == nil && reply != nil && reply.Err != "defeat" {
						kvc.goodPut++
						break // 请求成功，退出重试循环
					}

					if retry < maxRetries-1 {
						// 如果不是最后一次重试，则等待一段时间后再重试
						time.Sleep(retryDelay)
						// 可以选择增加重试间隔时间，例如：
						// retryDelay *= 2
					}
				}
			}
		}(i)
	}
	wg.Wait()
	kvc.c.Close()
}

// InitPool builds the shared cluster client (one connection pool per server).
func (kvc *KVClient) InitPool() {
	kvc.c = client.MustNew(kvc.Kvservers, client.Options{})
}

func main() {
	flag.Parse()
	// dataNum := *dnums
	valueSize := *vsize
	servers := strings.Split(*ser, ",")
	// fmt.Printf("servers:%v\n",servers)
	kvc := new(KVClient)
	kvc.Kvservers = servers

	// value := make([]byte, valueSize)
	value := util.GenerateLargeValue(valueSize)
	kvc.InitPool()
	startTime := time.Now()
	// 开始发送请求
	kvc.batchRawPut(value)

	sum_Size_MB := float64(kvc.goodPut*valueSize) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, goodPut %v, value %v, client %v, Size %vMB\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, kvc.goodPut, *vsize, *cnums, sum_Size_MB)
}
