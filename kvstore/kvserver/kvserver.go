package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/JasonLou99/Hybrid_KV_Store/config"
	"github.com/JasonLou99/Hybrid_KV_Store/lattices"
	"github.com/JasonLou99/Hybrid_KV_Store/persister"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/causalrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

type KVServer struct {
	peers           []string
	address         string
	internalAddress string // internal address for communication between nodes
	latency         int    // Simulation of geographical delay
	logs            []config.Log
	vectorclock     sync.Map	// 可在多个goroutine并发访问时保证数据的安全性，提供了一些原子操作。
	persister       *persister.Persister // 对数据库进行读写操作的接口
	memdb           *redis.Client
	ctx             context.Context
	// db              sync.Map // memory database
	// causalEntity *causal.CausalEntity

	// variable for writeless
	// "key": ["node1","node2"], ...
	// putCountsByNodes sync.Map
	// "key": 3, ...
	putCountsInProxy sync.Map
	// "key": 5, ...
	predictPutCounts sync.Map
	// "key": 3, ...
	getCountsInTotal sync.Map
	// "key": 3, ...
	putCountsInTotal sync.Map
}

type ValueTimestamp struct {
	value     string
	timestamp int64
	version   int32
}

// TCP Message struct
type TCPReq struct {
	Consistency string           `json:"consistency"`
	Operation   string           `json:"operation"`
	Key         string           `json:"key"`
	Value       string           `json:"value"`
	VectorClock map[string]int32 `json:"vector_clock"`
}

type TCPResp struct {
	Operation   string           `json:"operation"`
	Key         string           `json:"key"`
	Value       string           `json:"value"`
	VectorClock map[string]int32 `json:"vector_clock"`
	Success     bool             `json:"success"`
}

// this method is used to execute the command from client with causal consistency
func (kvs *KVServer)   startInCausal(command interface{}, vcFromClientArg map[string]int32, timestampFromClient int64) bool {
	vcFromClient := util.BecomeSyncMap(vcFromClientArg)	// 将map类型转换成同步安全的Map类型
	// 将 command 这个接口类型的值转换为 config.Log 类型的值
	newLog := command.(config.Log)
	util.DPrintf("Log in Start(): %v ", newLog)
	// util.DPrintf("vcFromClient in Start(): %v", vcFromClient)
	if newLog.Option == "Put" {
		/*
			Put操作中的vectorclock的变更逻辑
			1. 如果要求kvs.vectorclock更大，那么就无法让client跨越更新本地数据（即client收到了其它节点更新的数据，无法直接更新旧的副本节点）
			2. 如果要求vcFromClient更大，则可能造成一直无法put成功。需要副本节点返回vectorclock更新客户端。
			方案2会造成Put错误重试，额外需要一个RTT；同时考虑到更新vc之后，客户端依然是进行错误重试，也就是向副本节点写入上次尝试写入的值。
			所以在这里索性不做vc的要求，而是接收到了put就更新，再视情况更新客户端和本地的vc，直接就减少了错误重试的次数。
		*/
		/* vt, ok := kvs.db.Load(newLog.Key)
		vt2 := &ValueTimestamp{
			value: "",
		}
		if vt == nil {
			// the key is not in the db
			vt2 = &ValueTimestamp{
				value:     "",
				timestamp: 0,
			}
		} else {
			vt2 = &ValueTimestamp{
				value:     vt.(*ValueTimestamp).value,
				timestamp: vt.(*ValueTimestamp).timestamp,
				version:   vt.(*ValueTimestamp).version,
			}
		}
		oldVersion := vt2.version
		if ok && vt2.timestamp > timestampFromClient {
			// the value in the db is newer than the value in the client
			util.DPrintf("the value in the db is newer than the value in the client")
			return false
		} */
		// update vector clock
		// kvs.vectorclock = vcFromClient
		// val, _ := kvs.vectorclock.Load(kvs.internalAddress)
		// kvs.vectorclock.Store(kvs.internalAddress, val.(int32)+1)
		isUpper := util.IsUpper(kvs.vectorclock, vcFromClient)
		if isUpper {	// 把服务器的向量时钟加一
			val, _ := kvs.vectorclock.Load(kvs.internalAddress)
			kvs.vectorclock.Store(kvs.internalAddress, val.(int32)+1)
		} else {
			// vcFromClient is bigger than kvs.vectorclock
			kvs.MergeVC(vcFromClient)		// 服务器的向量时钟落后了，用客户端的将其更新，并且再加一
			val, _ := kvs.vectorclock.Load(kvs.internalAddress)
			kvs.vectorclock.Store(kvs.internalAddress, val.(int32)+1)
		}
		// init MapLattice for sending to other nodes
		ml := lattices.HybridLattice{
			Key: newLog.Key,
			Vl: lattices.ValueLattice{
				Log:         newLog,
				VectorClock: util.BecomeMap(kvs.vectorclock),
			},
		}
		// 开始同步日志
		data, _ := json.Marshal(ml)		// 将结构体m1序列化为JSON格式的字节流，并将结果
		args := &causalrpc.AppendEntriesInCausalRequest{	// 创建了一个指向该类型实例的指针，并把指针赋值给了变量args
			MapLattice: data,
			// Version:    oldVersion + 1,
			Version: 1,
		}
		// async sending to other nodes
		/*
			Gossip Buffer
		*/
		for i := 0; i < len(kvs.peers); i++ {
			if kvs.peers[i] != kvs.internalAddress {
				go kvs.sendAppendEntriesInCausal(kvs.peers[i], args)
			}
		}
		// update value in the db and persist
		kvs.logs = append(kvs.logs, newLog)
		// kvs.db.Store(newLog.Key, &ValueTimestamp{value: newLog.Value, timestamp: time.Now().UnixMilli(), version: oldVersion + 1})
		kvs.persister.Put(newLog.Key, newLog.Value)
		// err := kvs.memdb.Set(kvs.ctx, newLog.Key, newLog.Value, 0).Err()
		// if err != nil {
		// 	panic(err)
		// }
		return true
	} else if newLog.Option == "Get" {
		vcKVS, _ := kvs.vectorclock.Load(kvs.internalAddress)
		vcKVC, _ := vcFromClient.Load(kvs.internalAddress)
		return vcKVS.(int32) >= vcKVC.(int32)		// 比较vectorclock数组中internalAddress对应的那一个键值即可，并且不会进行修改，而Put操作是更新整个vectorclock的数组。
		// return util.IsUpper(kvs.vectorclock, vcFromClient)
	}
	util.DPrintf("here is Start() in Causal: log command option is false")
	return false
}

func (kvs *KVServer) GetInCausal(ctx context.Context, in *kvrpc.GetInCausalRequest) (*kvrpc.GetInCausalResponse, error) {
	util.DPrintf("GetInCausal %s", in.Key)
	getInCausalResponse := new(kvrpc.GetInCausalResponse)
	op := config.Log{
		Option: "Get",
		Key:    in.Key,
		Value:  "",
	}
	ok := kvs.startInCausal(op, in.Vectorclock, in.Timestamp)
	if ok {
		/* vt, _ := kvs.db.Load(in.Key)
		if vt == nil {
			getInCausalResponse.Value = ""
			getInCausalResponse.Success = false
			return getInCausalResponse, nil
		}
		valueTimestamp := vt.(*ValueTimestamp)
		// compare timestamp
		if valueTimestamp.timestamp > in.Timestamp {
			getInCausalResponse.Value = ""
			getInCausalResponse.Success = false
		} */
		// only update the client's vectorclock if the value is newer
		getInCausalResponse.Vectorclock = util.BecomeMap(kvs.vectorclock)
		// getInCausalResponse.Value = valueTimestamp.value
		getInCausalResponse.Value = string(kvs.persister.Get(in.Key))
		// val, err := kvs.memdb.Get(kvs.ctx, in.Key).Result()
		// if err != nil {
		// 	util.EPrintf(err.Error())
		// 	getInCausalResponse.Value = ""
		// 	getInCausalResponse.Success = false
		// 	return getInCausalResponse, nil
		// }
		// getInCausalResponse.Value = string(val)
		getInCausalResponse.Success = true
	} else {
		getInCausalResponse.Value = ""
		getInCausalResponse.Success = false
	}
	return getInCausalResponse, nil
}

func (kvs *KVServer) PutInCausal(ctx context.Context, in *kvrpc.PutInCausalRequest) (*kvrpc.PutInCausalResponse, error) {
	util.DPrintf("PutInCausal %s %s", in.Key, in.Value)
	putInCausalResponse := new(kvrpc.PutInCausalResponse)
	op := config.Log{
		Option: "Put",
		Key:    in.Key,
		Value:  in.Value,
	} 
	ok := kvs.startInCausal(op, in.Vectorclock, in.Timestamp)
	if ok {
		putInCausalResponse.Success = true
	} else {
		util.DPrintf("PutInCausal: StartInCausal Failed key=%s value=%s, Because vcFromClient < kvs.vectorclock", in.Key, in.Value)
		putInCausalResponse.Success = false
	}
	putInCausalResponse.Vectorclock = util.BecomeMap(kvs.vectorclock)
	return putInCausalResponse, nil
}

// this method is used to execute the command from client with causal consistency
func (kvs *KVServer) startInWritelessCausal(command interface{}, vcFromClientArg map[string]int32, timestampFromClient int64) bool {
	vcFromClient := util.BecomeSyncMap(vcFromClientArg)
	// 类型断言，检查并使用接口变量实际类型的一种方式
	newLog := command.(config.Log)
	util.DPrintf("Log in Start(): %v ", newLog)
	// util.DPrintf("vcFromClient in Start(): %v", vcFromClient)
	if newLog.Option == "Put" {
		isUpper := util.IsUpper(kvs.vectorclock, vcFromClient)
		if isUpper {
			val, _ := kvs.vectorclock.Load(kvs.internalAddress)
			kvs.vectorclock.Store(kvs.internalAddress, val.(int32)+1)
		} else {
			// vcFromClient is bigger than kvs.vectorclock
			kvs.MergeVC(vcFromClient)
			val, _ := kvs.vectorclock.Load(kvs.internalAddress)
			kvs.vectorclock.Store(kvs.internalAddress, val.(int32)+1)
		}
		putCounts_int := util.LoadInt(kvs.putCountsInProxy, newLog.Key)
		predictCounts_int := util.LoadInt(kvs.predictPutCounts, newLog.Key)
		if putCounts_int >= predictCounts_int {		// 需要同步
			util.DPrintf("Sync History Puts by Prediction, predictPutCounts: %v, putCountsInProxy: %v", predictCounts_int, putCounts_int)
			// init MapLattice for sending to other nodes
			ml := lattices.HybridLattice{
				Key: newLog.Key,
				Vl: lattices.ValueLattice{
					Log:         newLog,
					VectorClock: util.BecomeMap(kvs.vectorclock),
				},
			}
			data, _ := json.Marshal(ml)
			args := &causalrpc.AppendEntriesInCausalRequest{
				MapLattice: data,
				// Version:    oldVersion + 1,
				Version: 1,
			}
			// async sending to other nodes
			for i := 0; i < len(kvs.peers); i++ {
				if kvs.peers[i] != kvs.internalAddress {
					go kvs.sendAppendEntriesInCausal(kvs.peers[i], args)
				}
			}
			// 同步之后，重置代理中记录的该key对应put次数为0
			kvs.putCountsInProxy.Store(newLog.Key, 0)
		}
		// update value in the db and persist
		kvs.logs = append(kvs.logs, newLog)
		// kvs.db.Store(newLog.Key, &ValueTimestamp{value: newLog.Value, timestamp: time.Now().UnixMilli(), version: oldVersion + 1})
		kvs.persister.Put(newLog.Key, newLog.Value)
		// err := kvs.memdb.Set(kvs.ctx, newLog.Key, newLog.Value, 0).Err()
		// if err != nil {
		// 	panic(err)
		// }
		return true
	} else if newLog.Option == "Get" {
		vcKVS, _ := kvs.vectorclock.Load(kvs.internalAddress)
		vcKVC, _ := vcFromClient.Load(kvs.internalAddress)
		if vcKVS.(int32) >= vcKVC.(int32) {
			// 该Get请求有效，并加一
			getCounts := util.LoadInt(kvs.getCountsInTotal, newLog.Key)
			kvs.getCountsInTotal.Store(newLog.Key, getCounts+1)
			// 取出读的次数和代理中记录的值
			totalCounts := util.LoadInt(kvs.putCountsInTotal, newLog.Key)
			proxyCounts := util.LoadInt(kvs.putCountsInProxy, newLog.Key)
			// update predictPutCounts
			// ？？？？？？取平均以更新预测的put操作的阈值？？？？？？
			kvs.predictPutCounts.Store(newLog.Key, (totalCounts+proxyCounts)/(getCounts+1))
			// 不管get操作前有没有同步，都把putCountsInProxy加进队列，重新预测下一个阈值
			if proxyCounts != 0 {	// 说明在此get操作前一次没有被重置为0，也就是没有同步，同时也表明预测失败了
				util.DPrintf("Sync History Puts by Get")
				// 同步该key之前的put
				syncLog := config.Log{
					Option: "Put",
					Key:    newLog.Key,
					Value:  string(kvs.persister.Get(newLog.Key)),
				}
				ml := lattices.HybridLattice{
					Key: newLog.Key,
					Vl: lattices.ValueLattice{
						Log:         syncLog,
						VectorClock: util.BecomeMap(kvs.vectorclock),
					},
				}
				data, _ := json.Marshal(ml)
				syncReq := &causalrpc.AppendEntriesInCausalRequest{
					MapLattice: data,
					// Version:    oldVersion + 1,
					Version: 1,
				}
				for i := 0; i < len(kvs.peers); i++ {
					if kvs.peers[i] != kvs.internalAddress {
						go kvs.sendAppendEntriesInCausal(kvs.peers[i], syncReq)
					}
				}
				// 预测失败，在get时候触发上一次put操作的同步，由收到该get操作的server负责发起同步。
				// 并重置代理中记录的值，按理来说应该将此次put操作的次数记录在队列中，然后用机器学习对他进行预测，然后再更新预测的值。
				kvs.putCountsInProxy.Store(newLog.Key, 0)
				// kvs.putCountsByNodes.Store(newLog.Key, nil)
			}
			return true
		}
		return false
		// return util.IsUpper(kvs.vectorclock, vcFromClient)
	}
	util.DPrintf("here is Start() in Causal: log command option is false")
	return false
}

/* Writeless-Causal Consistency*/
func (kvs *KVServer) GetInWritelessCausal(ctx context.Context, in *kvrpc.GetInWritelessCausalRequest) (*kvrpc.GetInWritelessCausalResponse, error) {
	/*
		更新计数，比较预测值判断是否需要同步，更新预测值
	*/
	op := config.Log{
		Option: "Get",
		Key:    in.Key,
		Value:  "",
	}
	util.DPrintf("GetInWritelessCausal %s", in.Key)
	ok := kvs.startInWritelessCausal(op, in.Vectorclock, in.Timestamp)
	getInWritelessCausalResponse := new(kvrpc.GetInWritelessCausalResponse)
	if ok {
		getInWritelessCausalResponse.Vectorclock = util.BecomeMap(kvs.vectorclock)
		getInWritelessCausalResponse.Value = string(kvs.persister.Get(in.Key))
		getInWritelessCausalResponse.Success = true
	} else {
		getInWritelessCausalResponse.Value = ""
		getInWritelessCausalResponse.Success = false
	}
	return getInWritelessCausalResponse, nil
}

func (kvs *KVServer) PutInWritelessCausal(ctx context.Context, in *kvrpc.PutInWritelessCausalRequest) (*kvrpc.PutInWritelessCausalResponse, error) {
	util.DPrintf("PutInWritelessCausal %s", in.Key)
	putInWritelessCausalResponse := new(kvrpc.PutInWritelessCausalResponse)
	/*
		更新计数， 比较预测值判断是否需要同步
	*/
	op := config.Log{
		Option: "Put",
		Key:    in.Key,
		Value:  in.Value,
	}
	proxyCounts := util.LoadInt(kvs.putCountsInProxy, in.Key)
	kvs.putCountsInProxy.Store(in.Key, proxyCounts+1)
	totalCounts := util.LoadInt(kvs.putCountsInTotal, in.Key)
	kvs.putCountsInTotal.Store(in.Key, totalCounts+1)
	// kvs.putCountsByNodes[in.Key] = append(kvs.putCountsByNodes[in.Key], kvs.internalAddress)
	ok := kvs.startInWritelessCausal(op, in.Vectorclock, in.Timestamp)
	if ok {
		putInWritelessCausalResponse.Success = true
	} else {
		util.DPrintf("PutInCausal: StartInCausal Failed key=%s value=%s, Because vcFromClient < kvs.vectorclock", in.Key, in.Value)
		putInWritelessCausalResponse.Success = false
	}
	putInWritelessCausalResponse.Vectorclock = util.BecomeMap(kvs.vectorclock)
	return putInWritelessCausalResponse, nil
}

func (kvs *KVServer) AppendEntriesInCausal(ctx context.Context, in *causalrpc.AppendEntriesInCausalRequest) (*causalrpc.AppendEntriesInCausalResponse, error) {
	util.DPrintf("AppendEntriesInCausal %v", in)
	appendEntriesInCausalResponse := &causalrpc.AppendEntriesInCausalResponse{}
	var mlFromOther lattices.HybridLattice
	json.Unmarshal(in.MapLattice, &mlFromOther)
	vcFromOther := util.BecomeSyncMap(mlFromOther.Vl.VectorClock)
	ok := util.IsUpper(kvs.vectorclock, vcFromOther)
	if !ok {
		// Append the log to the local log
		kvs.logs = append(kvs.logs, mlFromOther.Vl.Log)
		// kvs.db.Store(mlFromOther.Key, &ValueTimestamp{value: mlFromOther.Vl.Log.Value, timestamp: time.Now().UnixMilli(), version: in.Version})
		kvs.persister.Put(mlFromOther.Key, mlFromOther.Vl.Log.Value)
		kvs.MergeVC(vcFromOther)
		appendEntriesInCausalResponse.Success = true
	} else {
		// Reject the log, Because of vectorclock
		appendEntriesInCausalResponse.Success = false
	}
	return appendEntriesInCausalResponse, nil
}

func (kvs *KVServer) RegisterKVServer(address string) {		// 传入的是客户端与服务器之间的代理服务器的地址
	util.DPrintf("RegisterKVServer: %s", address)	// 打印格式化后Debug信息
	for {
		// 利用标准库net创建的一个TCP服务器监听器，lis是一个net.Listener类型的对象，为创建的TCP监听器。
		// 监听address地址上的连接请求
		lis, err := net.Listen("tcp", address)
		if err != nil {
			util.FPrintf("failed to listen: %v", err)
		}
		// gRPC 是一个开源的高性能远程过程调用（RPC）框架，提供了在客户端和服务器之间进行跨平台、多语言的通信能力。
		// 创建并返回一个新的gRPC服务器对象，通过该对象可以注册服务和启动服务器，以便接受客户端的gRPC调用。
		grpcServer := grpc.NewServer()
		// 向gRPC服务器注册服务（包含服务定义和服务实现），gRPC服务器可以根据客户端的请求调用相应的服务方法，并返回结果。
		kvrpc.RegisterKVServer(grpcServer, kvs)
		// 将gRPC服务器注册为支持反射的服务器，可以使得客户端通过查询服务器的服务定义来了解其能力，由于安全性和性能等原因，正式部署时可能不建议启用反射功能
		reflection.Register(grpcServer)
		// 启动gRPC服务器并监听指定的代理服务器的网络地址
		if err := grpcServer.Serve(lis); err != nil {
			// 开始监听时发生了错误
			util.FPrintf("failed to serve: %v", err)
		}
	}
}
// 整个过程将以一个无限循环的方式持续进行，即使出现错误，也会继续尝试监听并提供服务。这种设计常见于网络服务器，目的是保持服务器的稳定性和可靠性
func (kvs *KVServer) RegisterCausalServer(address string) { // 传入的地址是internalAddress，节点间交流用的地址（用于类似日志同步等）
	util.DPrintf("RegisterCausalServer: %s", address)
	for {  // 创建一个TCP监听器，并在指定的地址（）上监听传入的连接。如果监听失败，则会打印错误信息。
		lis, err := net.Listen("tcp", address)
		if err != nil {
			util.FPrintf("failed to listen: %v", err)
		}
		grpcServer := grpc.NewServer()	// 创建一个gRPC服务器
		causalrpc.RegisterCAUSALServer(grpcServer, kvs)
		reflection.Register(grpcServer)	// 并在反射服务中进行了注册
		if err := grpcServer.Serve(lis); err != nil {	// 调用Serve方法来启动gRPC服务器，监听传入的连接，并处理相应的请求
			util.FPrintf("failed to serve: %v", err)
		}
	}
}

// s0 --> other servers
func (kvs *KVServer) sendAppendEntriesInCausal(address string, args *causalrpc.AppendEntriesInCausalRequest) (*causalrpc.AppendEntriesInCausalResponse, bool) {
	util.DPrintf("here is sendAppendEntriesInCausal() ---------> ", address)
	// 随机等待，模拟延迟
	time.Sleep(time.Millisecond * time.Duration(kvs.latency+rand.Intn(25)))
	// conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		util.EPrintf("sendAppendEntriesInCausal did not connect: %v", err)
	}
	defer conn.Close()
	client := causalrpc.NewCAUSALClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)		// 定时5秒
	defer cancel()

	reply, err := client.AppendEntriesInCausal(ctx, args)
	if err != nil {
		util.EPrintf("sendAppendEntriesInCausal could not greet: ", err, address)
		return reply, false
	}
	return reply, true
}

func (kvs *KVServer) MergeVC(vc sync.Map) {
	vc.Range(func(k, v interface{}) bool {
		val, ok := kvs.vectorclock.Load(k)
		if !ok {	// 这是什么情况，客户端有的vectorclock对应到服务器中没有，应该是同步客户端的信息
			kvs.vectorclock.Store(k, v)
		} else {
			if v.(int32) > val.(int32) {
				kvs.vectorclock.Store(k, v)
			}
		}
		return true
	})
}
	// 返回了一个指向KVServer类型对象的指针
func MakeKVServer(address string, internalAddress string, peers []string) *KVServer {
	util.IPrintf("Make KVServer %s... ", config.Address)	// 打印格式化后的信息，其中的地址是客户端和服务器之间的代理（目前不知道为什么需要代理）
	kvs := new(KVServer)	// 返回一个指向新分配的、零值初始化的KVServer类型的指针
	kvs.persister = new(persister.Persister)	// 实例化对数据库进行读写操作的接口对象
	kvs.persister.Init("db")	// 初始化，即获取对应路径的一个数据库实例，对其进行操作。
	kvs.address = address
	kvs.internalAddress = internalAddress
	kvs.peers = peers
	// init vectorclock: { "192.168.10.120:30881":0, "192.168.10.121:30881":0, ... }
	for i := 0; i < len(peers); i++ {	// 遍历输入结点的各个地址
		kvs.vectorclock.Store(peers[i], int32(0))	// 将每个地址以键值对的形式存入map映射中，初始值为0
	}
	// init memdb(redis)
	// redis client is a connection pool, support goroutine
	kvs.memdb = redis.NewClient(&redis.Options{		// 使用 Redis 客户端库创建一个与 Redis 数据库的连接。
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB  指定要使用的 Redis 数据库索引
	})
	kvs.ctx = context.Background()		// 创建一个起始的context上下文，不断透传下去，根context。
	kvs.ctx.Err()
	// 初始化map
	// kvs.putCountsByNodes = make(map[string][]string)
	// kvs.putCountsInProxy = make(map[string]int)
	// kvs.putCountsInTotal = make(map[string]int)
	// kvs.getCountsInTotal = make(map[string]int)
	// kvs.predictPutCounts = make(map[string]int)
	return kvs 
}

// 初始化TCP Server
func (kvs *KVServer) RegisterTCPServer(address string) {
	util.DPrintf("RegisterTCPServer: %s", address)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println("Error Native TCP listening", err.Error())
		return // 终止程序
	}
	// 监听并接受来自客户端的连接
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting", err.Error())
			return // 终止程序
		}
		// 处理连接
		go kvs.disributeRPC(conn)
	}
}

func (kvs *KVServer) disributeRPC(conn net.Conn) {
	for {
		buf := make([]byte, 512)
		// 从conn中读取数据到buf中，length变量存储读取的字节数
		length, err := conn.Read(buf)
		if err != nil {
			// fmt.Println("Error reading", err.Error())
			return //终止程序
		}
		fmt.Printf("%v\n", string(buf[:length]))
		var message TCPReq
		// 将buf中的数据解析到message中，即将接收到的原始JSON格式数据转换成Go程序中可以直接操作的结构体格式。
		json.Unmarshal(buf[:length], &message)
		fmt.Printf("message.consistency: %v", string(message.Consistency))
		consistencyLevel := message.Consistency
		switch consistencyLevel {
		case "GetInWritelessCausal":
			key := message.Key
			vc := message.VectorClock
			ts := time.Now().UnixMicro()
			util.DPrintf("GetInWritelessCausal: %s", key)
			op := config.Log{
				Option: "Get",
				Key:    key,
				Value:  "",
			}
			ok := kvs.startInWritelessCausal(op, vc, ts)
			var tcpResp TCPResp
			if ok {
				tcpResp.VectorClock = util.BecomeMap(kvs.vectorclock)
				tcpResp.Value = string(kvs.persister.Get(key))
				tcpResp.Success = true
				tcpResp.Key = key
			} else {
				tcpResp.Value = ""
				tcpResp.Success = false
			}
			res, _ := json.Marshal(tcpResp)
			conn.Write([]byte(res))
		case "PutInWritelessCausal":
			key := message.Key
			value := message.Value
			vc := message.VectorClock
			ts := time.Now().UnixMicro()
			util.DPrintf("PutInWritelessCausal: key:%s, val:%s, vc:%s, ts:%v", key, value, vc, ts)
			// conn.Write([]byte("OK"))
			op := config.Log{
				Option: message.Operation,
				Key:    key,
				Value:  value,
			}
			// 更新计数， 比较预测值判断是否需要同步
			proxyCounts := util.LoadInt(kvs.putCountsInProxy, key)
			kvs.putCountsInProxy.Store(key, proxyCounts+1)
			totalCounts := util.LoadInt(kvs.putCountsInTotal, key)
			kvs.putCountsInTotal.Store(key, totalCounts+1)
			// 以WritelessCausal一致性级别执行该请求
			ok := kvs.startInWritelessCausal(op, vc, ts)
			var tcpResp TCPResp
			tcpResp.Operation = op.Option
			tcpResp.Key = op.Key
			tcpResp.Value = op.Value
			if ok {
				tcpResp.Success = true
			} else {
				util.DPrintf("PutInWritelessCausal: StartInWritelessCausal Failed key=%s value=%s, Because vcFromClient < kvs.vectorclock", key, value)
				tcpResp.Success = false
			}
			tcpResp.VectorClock = util.BecomeMap(kvs.vectorclock)
			res, _ := json.Marshal(tcpResp)
			conn.Write([]byte(res))
		case "GetInCausal":
			key := message.Key
			util.DPrintf("GetInCausal: %s", key)
		case "PutInCausal":
			key := message.Key
			util.DPrintf("PutInCausal: %s", key)
		}
	}
}

func  main() {
	// peers inputed by command line
	// 使用flag包来定义一个命令行参数internalAddress_arg，并通过string函数指定了参数的类型为字符串，用于接受用户在命令行输入的地址信息。
	// 第一个参数是参数的名称。
	// 中间的参数是默认值，用户在命令行中没有指定参数时，将会使用空字符串来代替。
	// 第三个参数则是当用户使用命令行参数“--help”查看帮助时，会显示该描述信息，用于说明该参数的用途和作用。
	var internalAddress_arg = flag.String("internalAddress", "", "Input Your address")	// 返回的是一个指向string类型的指针
	var address_arg = flag.String("address", "", "Input Your address")
	var peers_arg = flag.String("peers", "", "Input Your Peers")
	var tcpAddress_arg = flag.String("tcpAddress", "", "Input Your TCP address")
	// 解析命令行参数并将其存储到上面对应的变量中
	flag.Parse()
	internalAddress := *internalAddress_arg		// 取出指针所指向的值，存入internalAddress变量
	tcpAddress := *tcpAddress_arg
	address := *address_arg
	peers := strings.Split(*peers_arg, ",")		// 将逗号作为分隔符传递给strings.Split函数，以便将peers_arg字符串分割成多个子字符串，并存储在peers的切片中
	kvs := MakeKVServer(address, internalAddress, peers)
	go kvs.RegisterKVServer(kvs.address)
	go kvs.RegisterCausalServer(kvs.internalAddress)
	go kvs.RegisterTCPServer(tcpAddress)
	// log.Println(http.ListenAndServe(":6060", nil))
	// server run for 20min
	time.Sleep(time.Second * 1200)
}