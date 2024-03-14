package main

import (
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	kvc "github.com/JasonLou99/Hybrid_KV_Store/kvstore/kvclient"
	"github.com/JasonLou99/Hybrid_KV_Store/util"
)

const (
	CAUSAL = iota
	BoundedStaleness
)

var count int32 = 0
var putCount int32 = 0
var getCount int32 = 0
var falseTime int32 = 0

// Test the consistency performance at different read/write ratios
func RequestRatio(cnum int, num int, servers []string, getRatio int, consistencyLevel int, quorum int) {
	fmt.Printf("servers: %v\n", servers)
	// 为切片和映射分配空间
	kvc := kvc.KVClient{
		Kvservers:   make([]string, len(servers)),
		Vectorclock: make(map[string]int32),
	}
	kvc.ConsistencyLevel = CAUSAL
	for _, server := range servers {
		kvc.Vectorclock[server+"1"] = 0
	}
	copy(kvc.Kvservers, servers)
	start_time := time.Now() // 记录所有请求操作开始前的时间，用于计算最后整个任务完成花了多久
	for i := 0; i < num; i++ {		// 这里的num对应的是操作次数，也就是put的操作次数
		rand.Seed(time.Now().UnixNano())
		key := rand.Intn(100000)

		// 设置生成value的大小为4KB
		value := util.GenerateLargeValue(1024*4)

		startTime := time.Now().UnixMicro()// 记录每次写操作开始的时间，用于返回从 Unix 时间开始至今的纳秒数，用于计算每次写操作花的时间，并记录到csv文件中

		// 写操作
		kvc.PutInCausal("key"+strconv.Itoa(key), string(value))

		// value := rand.Intn(100000)
		// kvc.PutInCausal("key"+strconv.Itoa(key), "value"+strconv.Itoa(value))

		spentTime := int(time.Now().UnixMicro() - startTime)
		// util.DPrintf("%v", spentTime)
		// 把每次写操作的花费时间添加进来
		kvc.PutSpentTimeArr = append(kvc.PutSpentTimeArr, spentTime)
		atomic.AddInt32(&putCount, 1)
		atomic.AddInt32(&count, 1)
		// util.DPrintf("put success")
		for j := 0; j < getRatio; j++ {
			// 读操作
			// 将字符串 "key" 与整数 key 转换为的字符串拼接在一起，并将结果存储在变量 k 中
			k := "key" + strconv.Itoa(key)
			var v string
			if quorum == 1 {
				v, _ = kvc.GetInCausalWithQuorum(k)
			} else {
				v, _ = kvc.GetInCausal(k)
			}
			// if GetInCausal return, it must be success
			atomic.AddInt32(&getCount, 1)
			atomic.AddInt32(&count, 1)
			if v != "" {
				// 查询出了值就输出，屏蔽请求非Leader的情况
				// util.DPrintf("TestCount: ", count, ",Get ", k, ": ", ck.Get(k))
				util.DPrintf("TestCount: %v ,Get %v: %v, VectorClock: %v, getCount: %v, putCount: %v", count, k, v, kvc.Vectorclock, getCount, putCount)
				// util.DPrintf("spent: %v", time.Since(start_time))
			}
		}
		// 随机切换下一个节点
		kvc.KvsId = rand.Intn(len(kvc.Kvservers)+10) % len(kvc.Kvservers)
	}
	fmt.Printf("TestCount: %v, VectorClock: %v, getCount: %v, putCount: %v\n", count, kvc.Vectorclock, getCount, putCount)
	if int(count) == num*cnum*(getRatio+1) {
		fmt.Printf("Task is completed, spent: %v\n", time.Since(start_time))
		fmt.Printf("falseTimes: %v\n", falseTime)
	}
	// 这个点表示的当前目录是整个项目的当前目录，而不是go文件所在的当前目录
	util.WriteCsv("./benchmark/result/causal_put-latency.csv", kvc.PutSpentTimeArr)
	// util.WriteCsv("../result/causal_put-latency.csv", kvc.PutSpentTimeArr)
}

/*
	根据csv文件中的读写频次发送请求
*/
func benchmarkFromCSV(filepath string, servers []string, clientNumber int) {

	kvc := kvc.KVClient{
		Kvservers:   make([]string, len(servers)),
		Vectorclock: make(map[string]int32),
	}
	for _, server := range servers {
		kvc.Vectorclock[server+"1"] = 0
	}
	copy(kvc.Kvservers, servers)
	writeCounts := util.ReadCsv(filepath)
	start_time := time.Now()
	// 经历两层for循环，得出的读请求次数为csv文件中，第二列（write_counts）中前1000个数的总和
	for i := 0; i < len(writeCounts); i++ {
		for j := 0; j < int(writeCounts[i]); j++ {
			// 重复发送写请求
			startTime := time.Now().UnixMicro()
			// kvc.PutInWritelessCausal("key"+strconv.Itoa(i), "value"+strconv.Itoa(i+j))
			kvc.PutInWritelessCausal("key", "value"+strconv.Itoa(i+j))
			spentTime := int(time.Now().UnixMicro() - startTime)
			// 记录的是每次put花的时间
			kvc.PutSpentTimeArr = append(kvc.PutSpentTimeArr, spentTime)
			util.DPrintf("%v", spentTime)
			atomic.AddInt32(&putCount, 1)
			atomic.AddInt32(&count, 1)
		}
		// 发送一次读请求
		// v, _ := kvc.GetInWritelessCausal("key" + strconv.Itoa(i))
		v, _ := kvc.GetInWritelessCausal("key")
		atomic.AddInt32(&getCount, 1)
		atomic.AddInt32(&count, 1)
		if v != "" {
			// 查询出了值就输出，屏蔽请求非Leader的情况
			// util.DPrintf("TestCount: ", count, ",Get ", k, ": ", ck.Get(k))
			util.DPrintf("TestCount: %v ,Get %v: %v, VectorClock: %v, getCount: %v, putCount: %v", count, "key"+strconv.Itoa(i), v, kvc.Vectorclock, getCount, putCount)
			// util.DPrintf("spent: %v", time.Since(start_time))
		}
		// 随机切换下一个节点
		kvc.KvsId = rand.Intn(len(kvc.Kvservers)+10) % len(kvc.Kvservers)
	}
	fmt.Printf("Task is completed, spent: %v. TestCount: %v, getCount: %v, putCount: %v \n", time.Since(start_time), count, getCount, putCount)
	// 时延数据写入csv
	util.WriteCsv("./benchmark/result/writless-causal_put-latency"+strconv.Itoa(clientNumber)+".csv", kvc.PutSpentTimeArr)
}

// zipf分布 高争用情况 zipf=x
func zipfDistributionBenchmark(x int, n int) int {
	return 0
}
func main() {
	var ser = flag.String("servers", "", "the Server, Client Connects to")
	// var mode = flag.String("mode", "RequestRatio", "Read or Put and so on")
	var mode = flag.String("mode", "BenchmarkFromCSV", "Read or Put and so on")
	var cnums = flag.String("cnums", "1", "Client Threads Number")
	var onums = flag.String("onums", "1", "Client Requests Times")
	var getratio = flag.String("getratio", "1", "Get Times per Put Times")
	var cLevel = flag.Int("consistencyLevel", CAUSAL, "Consistency Level")
	var quorumArg = flag.Int("quorum", 0, "Quorum Read")
	flag.Parse()
	servers := strings.Split(*ser, ",")
	// 用于将字符串转换为整数。在这里，Atoi 表示 "ASCII to integer"，即将ASCII码表示的数字字符串转换为整数形式
	// 使用下划线 _ 来忽略不需要的返回值或变量，这里忽略了 strconv.Atoi 函数可能返回的错误
	clientNumm, _ := strconv.Atoi(*cnums)
	optionNumm, _ := strconv.Atoi(*onums)
	getRatio, _ := strconv.Atoi(*getratio)
	consistencyLevel := int(*cLevel)
	quorum := int(*quorumArg)

	if clientNumm == 0 {
		fmt.Println("### Don't forget input -cnum's value ! ###")
		return
	}
	if optionNumm == 0 {
		fmt.Println("### Don't forget input -onumm's value ! ###")
		return
	}

	// Request Times = clientNumm * optionNumm
	if *mode == "RequestRatio" {
		for i := 0; i < clientNumm; i++ {
			go RequestRatio(clientNumm, optionNumm, servers, getRatio, consistencyLevel, quorum)
		}
	} else if *mode == "BenchmarkFromCSV" {
		for i := 0; i < clientNumm; i++ {
			go benchmarkFromCSV("./writeless/dataset/btcusd_low.csv", servers, clientNumm)
		}
	} else {
		fmt.Println("### Wrong Mode ! ###")
		return
	}
	// fmt.Println("count")
	// time.Sleep(time.Second * 3)
	// fmt.Println(count)
	//	return

	// keep main thread alive
	time.Sleep(time.Second * 1200)
}
