package main

import (
	"flag"
	"fmt"
	"math/rand"
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

func (kvc *KVClient) batchRawPut(value string) {
	wg := sync.WaitGroup{}
	base := *dnums / *cnums
	wg.Add(*cnums)
	kvc.goodPut = 0

	for i := 0; i < *cnums; i++ {
		go func(i int) {
			defer wg.Done()
			num := 0
			rand.Seed(time.Now().Unix())
			for j := 0; j < base; j++ {
				// k := rand.Intn(dnums)
				//k := basei + j
				// key := fmt.Sprintf("key_%d", k)
				key := util.GenerateFixedSizeKey(5)
				// key := strconv.Itoa(rand.Intn(dnums))
				// key:= generateUniqueRandomInts(1, *dnums)
				// key= strconv.Itoa(key)
				//fmt.Printf("Goroutine %v put key: key_%v\n", i, k)
				reply, err := kvc.c.Put(key, value) // 先随机传入一个地址的连接池
				// fmt.Println("after putinraft , j:",j)
				if err == nil && reply != nil && reply.Err != "defeat" {
					kvc.goodPut++
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

// batchRawPut blinds put bench.
// func (kvc *KVClient) batchRawPut(value string) {
// 	wg := sync.WaitGroup{}
// 	base := *dnums / *cnums
// 	wg.Add(*cnums)
// 	kvc.goodPut = 0

// 	// 为每个goroutine创建一个唯一的随机数生成器
// 	randomGens := make([]*rand.Rand, *cnums)
// 	for i := range randomGens {
// 		randomGens[i] = rand.New(rand.NewSource(time.Now().UnixNano()))
// 	}
// 	// 生成一个包含所有可能key的切片
// 	// allKeys := generateUniqueRandomInts(*dnums+5000000,*dnums+10000000)
// 	allKeys := generateUniqueRandomInts(0,*dnums)

// 	for i := 0; i < *cnums; i++ {
// 		go func(i int) {
//             defer wg.Done()

//             // 为每个goroutine分配一部分key
//             start := i * base
//             end := (i + 1) * base
//             if i == *cnums-1 {
//                 end = *dnums // 确保最后一个goroutine使用所有剩余的key
//             }
//             keys := allKeys[start:end]

//             // 打乱这部分key的顺序
//             // randomGens[i].Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

//             for j := 0; j < base; j++ {
//                 if j >= len(keys) {
//                     break // 防止越界
//                 }
//                 key := strconv.Itoa(keys[j])

//                 // 这里使用key进行你的操作
//                 reply, err := kvc.c.Put(key, value)
//                 if err == nil && reply != nil && reply.Err != "defeat" {
//                     kvc.goodPut++
//                 }
//             }
//         }(i)
// 	}
// 	wg.Wait()
// 	for _, pool := range kvc.pools {
// 		pool.Close()
// 		util.DPrintf("The raft pool has been closed")
// 	}
// }

func generateUniqueRandomInts(min, max int) []int {
	nums := make([]int, max-min+1)
	for i := range nums {
		nums[i] = min + i
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return nums
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
