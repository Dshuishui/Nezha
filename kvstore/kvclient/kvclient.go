package kvclient

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/shimingyah/pool"
)

/*
	kvclient有三种方式进行通信，http协议、原生tcp协议、grpc通信
	前两种面向wasm runtime调用，后一种是常规的kvs通信模式，用于性能对比
*/
type KVClient struct {
	Kvservers        []string
	Vectorclock      map[string]int32
	ConsistencyLevel int32
	KvsId            int // target node
	PutSpentTimeArr  []int
}

// func MakeKVClient(kvservers []string) *KVClient {
// 	return &KVClient{
// 		kvservers:   kvservers,
// 		vectorclock: make(map[string]int32),
// 	}
// }

const (
	CAUSAL = iota
	BoundedStaleness
)

/*
	CAUSAL
*/
// Method of Send RPC of GetInCausal
func (kvc *KVClient) SendGetInCausal(address string, request *kvrpc.GetInCausalRequest) (*kvrpc.GetInCausalResponse, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.EPrintf("err in SendGetInCausal: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.GetInCausal(ctx, request)
	if err != nil {
		util.EPrintf("err in SendGetInCausal: %v", err)
		return nil, err
	}
	return reply, nil
}

// Method of Send RPC of PutInCausal
func (kvc *KVClient) SendPutInCausal(address string, request *kvrpc.PutInCausalRequest) (*kvrpc.PutInCausalResponse, error) {
	// conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	// if err != nil {
	// 	util.EPrintf("err in SendPutInCausal-连接到服务器address有问题: %v", err)
	// 	return nil, err
	// }
	// defer conn.Close()
	// client := kvrpc.NewKVClient(conn)
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)		// 设置五秒定时往下传
	// defer cancel()
	// reply, err := client.PutInCausal(ctx, request)
	// if err != nil {
	// 	fmt.Println("客户端调用PutInCausal有问题")
	// 	util.EPrintf("err in SendPutInCausal-调用了服务器的put方法: %v", err)
	// 	return nil, err
	// }
	// return reply, nil
		// 这就是自己修改option参数的做法
		DesignOptions:=pool.Options{
			Dial:                 pool.Dial,
			MaxIdle:              8,
			MaxActive:            64,
			MaxConcurrentStreams: 64,
			Reuse:                true,
		}
	p, err := pool.New(address, DesignOptions)
	// p, err := pool.New(address, pool.DefaultOptions)
	if err != nil {
		util.EPrintf("failed to new pool: %v", err)
	}
	defer p.Close()

	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()

	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)		// 设置五秒定时往下传
	defer cancel()
	reply,err := client.PutInCausal(ctx,request)
	if err != nil {
		fmt.Println("客户端调用PutInCausal有问题")
		util.EPrintf("err in SendPutInCausal-调用了服务器的put方法: %v", err)
		return nil, err
	}
	return reply, nil
}

/*
	Writeless-CAUSAL
*/
// Method of Send RPC of GetInWritelessCausal
func (kvc *KVClient) SendPutInWritelessCausal(address string, request *kvrpc.PutInWritelessCausalRequest) (*kvrpc.PutInWritelessCausalResponse, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.EPrintf("err in SendPutInWritelessCausal: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.PutInWritelessCausal(ctx, request)
	if err != nil {
		util.EPrintf("err in SendPutInWritelessCausal: %v", err)
		return nil, err
	}
	return reply, nil
}

// Method of Send RPC of GetInWritelessCausal
func (kvc *KVClient) SendGetInWritelessCausal(address string, request *kvrpc.GetInWritelessCausalRequest) (*kvrpc.GetInWritelessCausalResponse, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		util.EPrintf("err in SendGetInWritelessCausal: %v", err)
		return nil, err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	reply, err := client.GetInWritelessCausal(ctx, request)
	if err != nil {
		util.EPrintf("err in SendGetInWritelessCausal: %v", err)
		return nil, err
	}
	return reply, nil
}

/*
	Writeless-CAUSAL
*/
func (kvc *KVClient) GetInWritelessCausal(key string) (string, bool) {
	request := &kvrpc.GetInWritelessCausalRequest{
		Key:         key,
		Vectorclock: kvc.Vectorclock,
	}
	for {
		request.Timestamp = time.Now().UnixMilli()
		reply, err := kvc.SendGetInWritelessCausal(kvc.Kvservers[kvc.KvsId], request)
		if err != nil {
			util.EPrintf("err in GetInWritelessCausal: %v", err)
			return "", false
		}
		if reply.Vectorclock != nil && reply.Success {
			kvc.Vectorclock = reply.Vectorclock
			return reply.Value, reply.Success
		}
		// refresh the target node
		// util.DPrintf("GetInCausal Failed, refresh the target node: %v", kvc.Kvservers[kvc.KvsId])
		fmt.Println("GetInWritelessCausal Failed, refresh the target node: ", kvc.Kvservers[kvc.KvsId])
		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
		atomic.AddInt32(&falseTime, 1)
	}
}
func (kvc *KVClient) PutInWritelessCausal(key string, value string) bool {
	request := &kvrpc.PutInWritelessCausalRequest{
		Key:         key,
		Value:       value,
		Vectorclock: kvc.Vectorclock,
		Timestamp:   time.Now().UnixMilli(),
	}
	// keep sending PutInCausal until success
	for {
		reply, err := kvc.SendPutInWritelessCausal(kvc.Kvservers[kvc.KvsId], request)
		if err != nil {
			util.EPrintf("err in PutInCausal: %v", err)
			return false
		}
		if reply.Vectorclock != nil && reply.Success {
			kvc.Vectorclock = reply.Vectorclock
			return reply.Success
		}
		// PutInCausal Failed
		// refresh the target node
		// util.DPrintf("PutInCausal Failed, refresh the target node")
		fmt.Printf("PutInWritelessCausal Failed, refresh the target node")
		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
	}
}

/*
	CAUSAL
*/
// Client Get Value, Read One Replica
func (kvc *KVClient) GetInCausal(key string) (string, bool) {
	request := &kvrpc.GetInCausalRequest{
		Key:         key,
		Vectorclock: kvc.Vectorclock,
	}
	for {
		request.Timestamp = time.Now().UnixMilli()
		reply, err := kvc.SendGetInCausal(kvc.Kvservers[kvc.KvsId], request)
		if err != nil {
			util.EPrintf("err in GetInCausal: %v", err)
			return "", false
		}
		if reply.Vectorclock != nil && reply.Success {
			kvc.Vectorclock = reply.Vectorclock
			return reply.Value, reply.Success
		}
		// refresh the target node
		// util.DPrintf("GetInCausal Failed, refresh the target node: %v", kvc.Kvservers[kvc.KvsId])
		fmt.Println("GetInCausal Failed, refresh the target node: ", kvc.Kvservers[kvc.KvsId])
		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
		atomic.AddInt32(&falseTime, 1)
	}
}

// Client Get Value, Read Quorum Replica
func (kvc *KVClient) GetInCausalWithQuorum(key string) (string, bool) {
	request := &kvrpc.GetInCausalRequest{
		Key:         key,
		Vectorclock: kvc.Vectorclock,
	}
	for {
		request.Timestamp = time.Now().UnixMilli()
		LatestReply := &kvrpc.GetInCausalResponse{
			Vectorclock: util.MakeMap(kvc.Kvservers),
			Value:       "",
			Success:     false,
		}
		// Get Value From All Nodes (Get All, Put One)
		// 向所有的server执行get操作，然后保留最新的reply
		for i := 0; i < len(kvc.Kvservers); i++ {
			reply, err := kvc.SendGetInCausal(kvc.Kvservers[i], request)
			if err != nil {
				util.EPrintf("err in GetInCausalWithQuorum: %v", err)
				return "", false
			}
			// if reply is newer, update the LatestReply
			if util.IsUpper(util.BecomeSyncMap(reply.Vectorclock), util.BecomeSyncMap(LatestReply.Vectorclock)) {
				LatestReply = reply
			}
		}
		if LatestReply.Vectorclock != nil && LatestReply.Success {
			kvc.Vectorclock = LatestReply.Vectorclock
			return LatestReply.Value, LatestReply.Success
		}
		// refresh the target node
		util.DPrintf("GetInCausalWithQuorum Failed, refresh the target node: %v", kvc.Kvservers[kvc.KvsId])
		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
	}
}

// Client Put Value
func (kvc *KVClient) PutInCausal(key string, value string) bool {
	request := &kvrpc.PutInCausalRequest{
		Key:         key,
		Value:       value,
		Vectorclock: kvc.Vectorclock,
		Timestamp:   time.Now().UnixMilli(),
	}
	// keep sending PutInCausal until success
	for {
		reply, err := kvc.SendPutInCausal(kvc.Kvservers[kvc.KvsId], request)
		if err != nil {
			fmt.Println("SendPutInCausal有问题")
			atomic.AddInt32(&falseTime, 1)
			util.EPrintf("err in PutInCausal: %v", err)
			return false
		}
		if reply.Vectorclock != nil && reply.Success {
			kvc.Vectorclock = reply.Vectorclock
			return reply.Success
		}
		// PutInCausal Failed
		// refresh the target node
		// util.DPrintf("PutInCausal Failed, refresh the target node")
		fmt.Printf("PutInCausal Failed, refresh the target node")
		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
	}
}

var count int32 = 0
var putCount int32 = 0
var getCount int32 = 0
var falseTime int32 = 0

// Test the consistency performance at different read/write ratios
func  RequestRatio(cnum int, num int, servers []string, getRatio int, consistencyLevel int, quorum int) {
	fmt.Printf("servers: %v\n", servers)
	kvc := KVClient{
		Kvservers:   make([]string, len(servers)),
		Vectorclock: make(map[string]int32),
	}
	kvc.ConsistencyLevel = CAUSAL
	for _, server := range servers {
		kvc.Vectorclock[server+"1"] = 0
	}
	copy(kvc.Kvservers, servers)
	start_time := time.Now()
	for i := 0; i < num; i++ {
		rand.Seed(time.Now().UnixNano())
		key := rand.Intn(100000)
		value := rand.Intn(100000)
		startTime := time.Now().UnixMicro()
		// 写操作
		kvc.PutInCausal("key"+strconv.Itoa(key), "value"+strconv.Itoa(value))
		spentTime := int(time.Now().UnixMicro() - startTime)
		// util.DPrintf("%v", spentTime)
		kvc.PutSpentTimeArr = append(kvc.PutSpentTimeArr, spentTime)
		atomic.AddInt32(&putCount, 1)
		atomic.AddInt32(&count, 1)
		// util.DPrintf("put success")
		for j := 0; j < getRatio; j++ {
			// 读操作
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
				util.DPrintf("TestCount: %v ,Get_key %v, VectorClock: %v, getCount: %v, putCount: %v", count, k, kvc.Vectorclock, getCount, putCount)
				// util.DPrintf("spent: %v", time.Since(start_time))
			}
		}
		// 随机切换下一个节点
		kvc.KvsId = rand.Intn(len(kvc.Kvservers)+10) % len(kvc.Kvservers)
	}
	fmt.Printf("TestCount: %v, VectorClock: %v, getCount: %v, putCount: %v\n", count, kvc.Vectorclock, getCount, putCount)
	// if int(count) == num*cnum*(getRatio+1) {
	if int(count) == num*cnum*(1) {
		fmt.Printf("Task is completed, spent: %v\n", time.Since(start_time))
		fmt.Printf("falseTimes: %v\n", falseTime)
	}
	// util.WriteCsv("./benchmark/result/causal_put-latency.csv", kvc.PutSpentTimeArr)
}

/*
	根据csv文件中的读写频次发送请求
*/
func benchmarkFromCSV(filepath string, servers []string, clientNumber int) {

	kvc := KVClient{
		Kvservers:   make([]string, len(servers)),
		Vectorclock: make(map[string]int32),
	}
	for _, server := range servers {
		kvc.Vectorclock[server+"1"] = 0
	}
	copy(kvc.Kvservers, servers)
	writeCounts := util.ReadCsv(filepath)
	for i := 0; i < len(writeCounts); i++ {
		for j := 0; j < int(writeCounts[i]); j++ {
			// 重复发送写请求
			startTime := time.Now().UnixMicro()
			// kvc.PutInWritelessCausal("key"+strconv.Itoa(i), "value"+strconv.Itoa(i+j))
			kvc.PutInWritelessCausal("key", "value"+strconv.Itoa(i+j))
			spentTime := int(time.Now().UnixMicro() - startTime)
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
			util.DPrintf("TestCount: %v ,Get_key %v, VectorClock: %v, getCount: %v, putCount: %v", count, "key"+strconv.Itoa(i), kvc.Vectorclock, getCount, putCount)
			// util.DPrintf("spent: %v", time.Since(start_time))
		}
		// 随机切换下一个节点
		kvc.KvsId = rand.Intn(len(kvc.Kvservers)+10) % len(kvc.Kvservers)
	}
	// 时延数据写入csv
	// util.WriteCsv("./benchmark/result/writless-causal_put-latency"+strconv.Itoa(clientNumber)+".csv", kvc.PutSpentTimeArr)
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
	time.Sleep(time.Second * 36000)
}
