package kvclient

import (
	"context"
	// "flag"
	"fmt"
	// "math/rand"
	// "strconv"
	// "strings"
	"sync"
	// "sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"gitee.com/dong-shuishui/FlexSync/pool"
)

/*
	kvclient有三种方式进行通信，http协议、原生tcp协议、grpc通信
	前两种面向wasm runtime调用，后一种是常规的kvs通信模式，用于性能对比
*/
type KVClient struct {
	Kvservers      []string
	KvsId           int // target node
	mu sync.Mutex
	clientId int64		// 客户端唯一标识
	seqId int64			// 该客户端单调递增的请求id
	leaderId int
}

/*
	Raft
*/
// Method of Send RPC of GetInRaft
func (kvc *KVClient) SendGetInRaft(address string, request *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

// Method of Send RPC of PutInRaft
func (kvc *KVClient) SendPutInRaft(address string, request *kvrpc.PutInRaftRequest,p pool.Pool) (*kvrpc.PutInRaftResponse, error) {
	conn, err := p.Get()
	if err != nil {
		util.EPrintf("failed to get conn: %v", err)
	}
	defer conn.Close()

	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)		// 设置五秒定时往下传
	defer cancel()
	reply,err := client.PutInRaft(ctx,request)
	if err != nil {
		fmt.Println("客户端调用PutInRaft有问题")
		util.EPrintf("err in SendPutInRaft-调用了服务器的put方法: %v", err)
		return nil, err
	}
	return reply, nil
}

/*
	Raft
*/
// Client Get Value, Read One Replica
// func (kvc *KVClient) GetInRaft(key string) (string, bool) {
// 	request := &kvrpc.GetInRaftRequest{
// 		Key:         key,
// 	}
// 	for {
// 		request.Timestamp = time.Now().UnixMilli()
// 		reply, err := kvc.SendGetInRaft(kvc.Kvservers[kvc.KvsId], request)
// 		if err != nil {
// 			util.EPrintf("err in GetInRaft: %v", err)
// 			return "", false
// 		}
// 		if reply.Vectorclock != nil && reply.Success {
// 			return reply.Value, reply.Success
// 		}
// 		// refresh the target node
// 		// util.DPrintf("GetInRaft Failed, refresh the target node: %v", kvc.Kvservers[kvc.KvsId])
// 		fmt.Println("GetInRaft Failed, refresh the target node: ", kvc.Kvservers[kvc.KvsId])
// 		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
// 		atomic.AddInt32(&falseTime, 1)
// 	}
// }

// Client Get Value, Read Quorum Replica
// func (kvc *KVClient) GetInRaftWithQuorum(key string) (string, bool) {
// 	request := &kvrpc.GetInRaftRequest{
// 		Key:         key,
// 	}
// 	for {
// 		request.Timestamp = time.Now().UnixMilli()
// 		LatestReply := &kvrpc.GetInRaftResponse{
// 			Vectorclock: util.MakeMap(kvc.Kvservers),
// 			Value:       "",
// 			Success:     false,
// 		}
// 		// Get Value From All Nodes (Get All, Put One)
// 		// 向所有的server执行get操作，然后保留最新的reply
// 		for i := 0; i < len(kvc.Kvservers); i++ {
// 			reply, err := kvc.SendGetInRaft(kvc.Kvservers[i], request)
// 			if err != nil {
// 				util.EPrintf("err in GetInRaftWithQuorum: %v", err)
// 				return "", false
// 			}
// 			// if reply is newer, update the LatestReply
// 			if util.IsUpper(util.BecomeSyncMap(reply.Vectorclock), util.BecomeSyncMap(LatestReply.Vectorclock)) {
// 				LatestReply = reply
// 			}
// 		}
// 		if LatestReply.Vectorclock != nil && LatestReply.Success {
// 			return LatestReply.Value, LatestReply.Success
// 		}
// 		// refresh the target node
// 		util.DPrintf("GetInRaftWithQuorum Failed, refresh the target node: %v", kvc.Kvservers[kvc.KvsId])
// 		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
// 	}
// }

// // Client Put Value
// func (kvc *KVClient) PutInRaft(key string, value string,p pool.Pool) bool {
// 	request := &kvrpc.PutInRaftRequest{
// 		Key:         key,
// 		Value:       value,
// 		Timestamp:   time.Now().UnixMilli(),
// 	}
// 	// keep sending PutInRaft until success
// 	for {
// 		reply, err := kvc.SendPutInRaft(kvc.Kvservers[kvc.KvsId], request,p)
// 		if err != nil {
// 			fmt.Println("SendPutInRaft有问题")
// 			atomic.AddInt32(&falseTime, 1)
// 			util.EPrintf("err in PutInRaft: %v", err)
// 			return false
// 		}
// 		if reply.Vectorclock != nil && reply.Success {
// 			return reply.Success
// 		}
// 		// PutInRaft Failed
// 		// refresh the target node
// 		// util.DPrintf("PutInRaft Failed, refresh the target node")
// 		fmt.Printf("PutInRaft Failed, refresh the target node")
// 		kvc.KvsId = (kvc.KvsId + 1) % len(kvc.Kvservers)
// 	}
// }

var count int32 = 0
var putCount int32 = 0
var getCount int32 = 0
var falseTime int32 = 0

func main() {
	// var ser = flag.String("servers", "", "the Server, Client Connects to")
	// // var mode = flag.String("mode", "RequestRatio", "Read or Put and so on")
	// var mode = flag.String("mode", "BenchmarkFromCSV", "Read or Put and so on")
	// var cnums = flag.String("cnums", "1", "Client Threads Number")
	// var onums = flag.String("onums", "1", "Client Requests Times")
	// var getratio = flag.String("getratio", "1", "Get Times per Put Times")
	// var quorumArg = flag.Int("quorum", 0, "Quorum Read")
	// flag.Parse()
	// servers := strings.Split(*ser, ",")
	// clientNumm, _ := strconv.Atoi(*cnums)
	// optionNumm, _ := strconv.Atoi(*onums)
	// getRatio, _ := strconv.Atoi(*getratio)
	// quorum := int(*quorumArg)

	// if clientNumm == 0 {
	// 	fmt.Println("### Don't forget input -cnum's value ! ###")
	// 	return
	// }
	// if optionNumm == 0 {
	// 	fmt.Println("### Don't forget input -onumm's value ! ###")
	// 	return
	// }

	// Request Times = clientNumm * optionNumm
	// if *mode == "RequestRatio" {
	// 	for i := 0; i < clientNumm; i++ {
	// 		go RequestRatio(clientNumm, optionNumm, servers, getRatio, quorum)
	// 	}
	// } else {
	// 	fmt.Println("### Wrong Mode ! ###")
	// 	return
	// }
	// fmt.Println("count")
	// time.Sleep(time.Second * 3)
	// fmt.Println(count)
	//	return

	// keep main thread alive
	time.Sleep(time.Second * 36000)
}
