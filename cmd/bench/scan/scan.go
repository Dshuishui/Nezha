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
	c         *client.Client

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
				reply, err := kvc.c.Scan(startKey, endKey) // 先随机传入一个地址的连接池
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
	kvc.c.Close()
}

// InitPool builds the shared cluster client (one connection pool per server).
func (kvc *KVClient) InitPool() {
	kvc.c = client.MustNew(kvc.Kvservers, client.Options{})
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
