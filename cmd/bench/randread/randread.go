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

// go run ./benchmark/randread/randread.go -cnums 400 -dnums 100000 -servers 192.168.1.62:3088,192.168.1.100:3088,192.168.1.104:3088
var (
	ser = flag.String("servers", "", "the Server, Client Connects to")
	// mode     = flag.String("mode", "RequestRatio", "Read or Put and so on")
	cnums = flag.Int("cnums", 1, "Client Threads Number")
	dnums = flag.Int("dnums", 1000000, "data num")
	// getratio = flag.Int("getratio", 1, "Get Times per Put Times")
	key = flag.Int("key", 6, "target key")
)

type KVClient struct {
	c         *client.Client
	Kvservers []string

	goodPut int // 有效吞吐量
}

// randread
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
				// time.Sleep(100 * time.Millisecond)
				_, keyExist, err := kvc.c.Get(targetkey) // 先随机传入一个地址的连接池
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
					// kvc.c.Put(targetkey, value) // 找到不存在的，先随便弥补一个键值对
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
	kvc.c.Close()
}

// InitPool builds the shared cluster client (one connection pool per server).
func (kvc *KVClient) InitPool() {
	kvc.c = client.MustNew(kvc.Kvservers, client.Options{})
}

func main() {
	flag.Parse()
	// dataNum := *dnums
	// key := *key
	servers := strings.Split(*ser, ",")
	// fmt.Printf("servers:%v\n",servers)
	kvc := new(KVClient)
	kvc.Kvservers = servers

	kvc.InitPool() // 初始化grpc连接池
	startTime := time.Now()
	// 开始发送请求
	kvc.randRead()
	valuesize := 1000

	sum_Size_MB := float64(kvc.goodPut*valuesize) / 1000000
	fmt.Printf("\nelapse:%v, throught:%.4fMB/S, total %v, goodPut %v, value %v, client %v, Size %vMB\n",
		time.Since(startTime), float64(sum_Size_MB)/time.Since(startTime).Seconds(), *dnums, kvc.goodPut, valuesize, *cnums, sum_Size_MB)
}
