package main

/*
go run ./test.go -cnums 10 -onums 100 -getratio 4 -servers 192.168.10.120:6379,192.168.10.121:6379,192.168.10.122:6379
*/
import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/util"

	"github.com/redis/go-redis/v9"
)

var count int32 = 0
var putCount int32 = 0
var getCount int32 = 0
var falseTime int32 = 0

func main() {
	var ser = flag.String("servers", "", "the Redis Server, Client Connects to")
	var cnums = flag.String("cnums", "1", "Client Threads Number")
	var onums = flag.String("onums", "1", "Client Requests Times")
	var getratio = flag.String("getratio", "1", "Get Times per Put Times")
	flag.Parse()
	clientNumm, _ := strconv.Atoi(*cnums)
	optionNumm, _ := strconv.Atoi(*onums)
	getRatio, _ := strconv.Atoi(*getratio)
	servers := strings.Split(*ser, ",")
	for i := 0; i < clientNumm; i++ {
		go RequestRatio(clientNumm, optionNumm, servers, getRatio)
	}
	time.Sleep(time.Second * 1200)
}

func RequestRatio(cnum int, num int, servers []string, getRatio int) {
	// testClient := redis.NewClient(&redis.Options{
	// 	Addr:     server,
	// 	Password: "", // no password set
	// 	DB:       0,  // use default DB
	// })
	testClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    servers,
		Password: "",
		// DB:       0,
	})
	ctx := context.Background()
	start_time := time.Now()
	for i := 0; i < num; i++ {
		rand.Seed(time.Now().UnixNano())
		key := rand.Intn(100000)
		value := rand.Intn(100000)
		// 写操作
		// kvc.PutInCausal("key"+strconv.Itoa(key), "value"+strconv.Itoa(value))
		err := testClient.Set(ctx, "key"+strconv.Itoa(key), "value"+strconv.Itoa(value), 0).Err()
		if err != nil {
			panic(err)
		}
		atomic.AddInt32(&putCount, 1)
		atomic.AddInt32(&count, 1)
		// util.DPrintf("put success")
		for j := 0; j < getRatio; j++ {
			// 读操作
			k := "key" + strconv.Itoa(key)
			var v string
			v, _ = testClient.Get(ctx, k).Result()
			atomic.AddInt32(&getCount, 1)
			atomic.AddInt32(&count, 1)
			if v != "" {
				// util.DPrintf("TestCount: ", count, ",Get ", k, ": ", ck.Get(k))
				util.DPrintf("TestCount: %v ,Get %v: %v, getCount: %v, putCount: %v", count, k, v, getCount, putCount)
				// util.DPrintf("spent: %v", time.Since(start_time))
			}
		}
	}
	fmt.Printf("TestCount: %v, getCount: %v, putCount: %v\n", count, getCount, putCount)
	if int(count) == num*cnum*(getRatio+1) {
		fmt.Printf("Task is completed, spent: %v\n", time.Since(start_time))
		fmt.Printf("falseTimes: %v\n", falseTime)
	}
}
