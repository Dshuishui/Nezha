package main

import (
	"context"
	"strconv"
	// "encoding/json"
	"flag"
	"fmt"
	"sync/atomic"
	// "math/rand"
	"encoding/binary"
	"net"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"time"

	// "github.com/JasonLou99/Hybrid_KV_Store/config"
	"github.com/JasonLou99/Hybrid_KV_Store/raft"
	// "github.com/JasonLou99/Hybrid_KV_Store/persister"
	"github.com/JasonLou99/Hybrid_KV_Store/rpc/kvrpc"
	// "github.com/JasonLou99/Hybrid_KV_Store/rpc/raftrpc"
	"github.com/JasonLou99/Hybrid_KV_Store/util"

	"google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
	"github.com/JasonLou99/Hybrid_KV_Store/pool"
	"github.com/syndtr/goleveldb/leveldb"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	internalAddress_arg = flag.String("internalAddress", "", "Input Your address") // 返回的是一个指向string类型的指针
	address_arg         = flag.String("address", "", "Input Your address")
	peers_arg           = flag.String("peers", "", "Input Your Peers")
	gap_arg 			= flag.String("gap", "", "Input Your gap")
)

const (
	OP_TYPE_PUT = "Put"
	OP_TYPE_GET = "Get"
)

type KVServer struct {
	mu              sync.Mutex
	peers           []string
	address         string
	internalAddress string    // internal address for communication between nodes
	lastPutTime     time.Time // lastPutTime记录最后一次PUT请求的时间
	valuelog        *ValueLog
	// pools           []pool.Pool // 用于日志同步的连接池

	me        int
	raft      *raft.Raft
	persister *raft.Persister    // 对数据库进行读写操作的接口
	applyCh   chan raft.ApplyMsg // 用于与Raft层面传输数据的通道
	dead      int32              // set by Kill()
	reqMap    map[int]*OpContext // log index -> 请求上下文
	seqMap    map[int64]int64    // 客户端id -> 客户端seq

	lastAppliedIndex int // 已持久化存储的日志index
	kvrpc.UnimplementedKVServer
}

// ValueLog represents the Value Log file for storing values.
type ValueLog struct {
	file         *os.File
	leveldb      *leveldb.DB
	valueLogPath string
}

type Op struct {
	Index    int    // 写入raft log时的index
	Term     int    // 写入raft log时的term
	Type     string // Put、Get
	Key      string
	Value    string
	SeqId    int64
	ClientId int64
}

// 等待Raft提交期间的Op上下文, 用于唤醒阻塞的RPC
type OpContext struct {
	op        raft.DetailCod
	committed chan byte

	wrongLeader bool // 因为index位置log的term不一致, 说明leader换过了
	ignored     bool // 因为req id过期, 表示已经执行过，该日志需要被跳过

	// Get操作的结果
	keyExist bool
	value    string
}

var wg = sync.WaitGroup{}

func newOpContext(op raft.DetailCod) (opCtx *OpContext) {
	opCtx = &OpContext{
		op:        op,
		committed: make(chan byte),
	}
	return
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.raft.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kvs *KVServer) StartGet(args *kvrpc.GetInRaftRequest) (reply *kvrpc.GetInRaftResponse) {
	reply.Err = raft.OK

	op := raft.DetailCod{
		OpType:   OP_TYPE_GET,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	op.Index, op.Term, isLeader = kvs.raft.Start(op) // 读操作不需要写入raft日志，即不需要作为日志追加进去
	if !isLeader {
		reply.Err = raft.ErrWrongLeader
		return reply
	}

	opCtx := newOpContext(op)

	func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kvs.reqMap[int(op.Index)] = opCtx
	}()

	// RPC结束前清理上下文
	defer func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if one, ok := kvs.reqMap[int(op.Index)]; ok {
			if one == opCtx {
				delete(kvs.reqMap, int(op.Index))
			}
		}
	}()

	timer := time.NewTimer(8000 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-opCtx.committed: // 如果提交了
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = raft.ErrWrongLeader
		} else if !opCtx.keyExist { // key不存在
			reply.Err = raft.ErrNoKey
		} else {
			reply.Value = opCtx.value // 返回值
		}
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		reply.Err = raft.ErrWrongLeader
	}
	return reply
}

func (kvs *KVServer) GetInRaft(ctx context.Context, in *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	reply := kvs.StartGet(in)
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	}else if reply.Err == raft.ErrNoKey{
		// 返回客户端没有该key即可，这里先不做操作
		fmt.Println("server端没有client查询的key")
	}
	return reply, nil
}

func (kvs *KVServer) PutInRaft(ctx context.Context, in *kvrpc.PutInRaftRequest) (*kvrpc.PutInRaftResponse, error) {
	// fmt.Println("走到了server端的put函数")
	reply := kvs.StartPut(in)
	if reply.Err == raft.ErrWrongLeader {
		reply.LeaderId = kvs.raft.GetLeaderId()
	}
	return reply, nil
}

func (kvs *KVServer) StartPut(args *kvrpc.PutInRaftRequest) *kvrpc.PutInRaftResponse {
	reply := &kvrpc.PutInRaftResponse{Err: raft.OK, LeaderId: 0}
	op := raft.DetailCod{
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
	}

	// 写入raft层
	var isLeader bool
	op.Index, op.Term, isLeader = kvs.raft.Start(op)
	if !isLeader {
		// fmt.Println("不是leader，返回")
		reply.Err = raft.ErrWrongLeader
		return reply // 如果收到客户端put请求的不是leader，需要将leader的id返回给客户端的reply中
	}

	opCtx := newOpContext(op)

	func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		// 保存RPC上下文，等待提交回调，可能会因为Leader变更覆盖同样Index，不过前一个RPC会超时退出并令客户端重试
		kvs.reqMap[int(op.Index)] = opCtx
	}()

	// 超时后，结束apply请求的RPC，清理该请求index的上下文
	defer func() {
		kvs.mu.Lock()
		defer kvs.mu.Unlock()
		if one, ok := kvs.reqMap[int(op.Index)]; ok {
			if one == opCtx {
				delete(kvs.reqMap, int(op.Index))
			}
		}
	}()

	timer := time.NewTimer(5000 * time.Millisecond)
	defer timer.Stop()
	select {
	// 通道关闭或者有数据传入都会执行以下的分支
	case <-opCtx.committed: // ApplyLoop函数执行完后，会关闭committed通道，再根据相关的值设置请求reply的结果
		if opCtx.wrongLeader { // 同样index位置的term不一样了, 说明leader变了，需要client向新leader重新写入
			reply.Err = raft.ErrWrongLeader
			// fmt.Println("走了哪个操作1")
			// fmt.Println("设置reply为WrongLeader")
		} else if opCtx.ignored {
			// fmt.Println("走了哪个操作2")
			// 说明req id过期了，该请求被忽略，对MIT这个lab来说只需要告知客户端OK跳过即可
			reply.Err = raft.OK
		}
	case <-timer.C: // 如果2秒都没提交成功，让client重试
		// fmt.Println("Put请求执行超时了，超过了2s，重新让client发送执行")
		reply.Err = raft.ErrWrongLeader
	}
	return reply
}

func (kvs *KVServer) RegisterKVServer(ctx context.Context, address string) { // 传入的是客户端与服务器之间的代理服务器的地址
	defer wg.Done()
	util.DPrintf("RegisterKVServer: %s", address) // 打印格式化后Debug信息
	for {
		lis, err := net.Listen("tcp", address)
		if err != nil {
			util.FPrintf("failed to listen: %v", err)
		}
		grpcServer := grpc.NewServer( // 设置自定义的grpc连接
			grpc.InitialWindowSize(pool.InitialWindowSize),
			grpc.InitialConnWindowSize(pool.InitialConnWindowSize),
			grpc.MaxSendMsgSize(pool.MaxSendMsgSize),
			grpc.MaxRecvMsgSize(pool.MaxRecvMsgSize),
			grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
				PermitWithoutStream: true,
				MinTime:             10 * time.Second, // 这里设置与client的keepalive探测的最小时间间隔。
			}),
			grpc.KeepaliveParams(keepalive.ServerParameters{
				Time:                  pool.KeepAliveTime,
				Timeout:               pool.KeepAliveTimeout,
				MaxConnectionAgeGrace: 30 * time.Second,
			}),
		)
		kvrpc.RegisterKVServer(grpcServer, kvs)
		reflection.Register(grpcServer)

		// 在一个新的协程中启动超时检测，如果一段时间内没有put请求发过来，则终止程序，关闭服务器，以节省资源。
		go func() {
			<-ctx.Done()
			grpcServer.GracefulStop()
			fmt.Println("Server stopped due to context cancellation-kvserver.")
		}()

		// 在grpcServer.Serve(lis)之后的代码默认情况下是不会执行的，因为Serve方法会阻塞当前goroutine直到服务器停止。然而，如果Serve因为某些错误而返回，后面的代码就会执行。
		if err := grpcServer.Serve(lis); err != nil {
			// 开始监听时发生了错误
			util.FPrintf("failed to serve: %v", err)
		}
		fmt.Println("跳出kvserver的for循环")
		break
	}
}

// NewValueLog creates a new Value Log.
func NewValueLog(valueLogPath string, leveldbPath string) (*ValueLog, error) {
	vLog := &ValueLog{valueLogPath: valueLogPath}
	var err error
	vLog.file, err = os.OpenFile(valueLogPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("打开valuelog文件有问题")
		return nil, err
	}
	vLog.leveldb, err = leveldb.OpenFile(leveldbPath, nil)
	if err != nil {
		fmt.Println("打开leveldb文件有问题")
		return nil, err
	}
	return vLog, nil
}

// Put stores the key-value pair in the Value Log and updates LevelDB.
func (vl *ValueLog) Put_Pure(key []byte, value []byte) error {
	// Calculate the position where the value will be written.
	position, err := vl.file.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}

	// Write <idnex, keysize, valuesize, key, value, currentTerm, votedFor, log[]> to the Value Log.
	// 固定整数的长度，即四个字节
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	// extraSize := uint32(8) // 八个字节存储currentTerm和votedFor
	// structSize := uint32(structBuf.Len())
	buf := make([]byte, 8+keySize+valueSize)
	binary.BigEndian.PutUint32(buf[0:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valueSize)
	copy(buf[8:8+keySize], key)
	copy(buf[8+keySize:], value)
	if _, err := vl.file.Write(buf); err != nil {
		return err
	}

	// Update LevelDB with <key, position>.
	// 相当于把地址（指向keysize开始处）压缩一下
	positionBytes := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(positionBytes, position)
	return vl.leveldb.Put(key, positionBytes, nil)
}

func (vl *ValueLog) Put(key []byte, value []byte) error {
	keySize := uint32(len(key))
	valueSize := uint32(len(value))
	buf := make([]byte, 8+keySize+valueSize)
	binary.BigEndian.PutUint32(buf[0:4], keySize)
	binary.BigEndian.PutUint32(buf[4:8], valueSize)
	copy(buf[8:8+keySize], key)
	copy(buf[8+keySize:], value)
	if _, err := vl.file.Write(buf); err != nil {
		return err
	}
	return nil
}

// Get retrieves the value for a given key from the Value Log.
func (vl *ValueLog) Get(key []byte) ([]byte, error) {
	// Retrieve the position from LevelDB.
	positionBytes, err := vl.leveldb.Get(key, nil)
	if err != nil {
		fmt.Println("get不到数据")
		return nil, err
	}
	position, _ := binary.Varint(positionBytes)

	// Seek to the position in the Value Log.
	_, err = vl.file.Seek(position, os.SEEK_SET)
	if err != nil {
		fmt.Println("get时，seek文件的位置有问题")
		return nil, err
	}

	// Read the key size and value size.
	var keySize, valueSize uint32
	sizeBuf := make([]byte, 8)
	if _, err := vl.file.Read(sizeBuf); err != nil {
		fmt.Println("get时，读取key 和 value size时有问题")
		return nil, err
	}
	keySize = binary.BigEndian.Uint32(sizeBuf[0:4])
	valueSize = binary.BigEndian.Uint32(sizeBuf[4:8])

	// Skip over the key bytes.
	// 因为上面已经读取了keysize和valuesize，所以文件的偏移量自动往后移动了8个字节
	if _, err := vl.file.Seek(int64(keySize), os.SEEK_CUR); err != nil {
		fmt.Println("get时，跳过key时有问题")
		return nil, err
	}

	// Read the value bytes.
	value := make([]byte, valueSize)
	if _, err := vl.file.Read(value); err != nil {
		fmt.Println("get是，根据value的偏移位置，拿取value值时有问题")
		return nil, err
	}

	return value, nil
}

// 返回了一个指向KVServer类型对象的指针
func MakeKVServer(address string, internalAddress string, peers []string) *KVServer {
	kvs := new(KVServer)                // 返回一个指向新分配的、零值初始化的KVServer类型的指针
	kvs.persister = new(raft.Persister) // 实例化对数据库进行读写操作的接口对象
	kvs.address = address
	kvs.internalAddress = internalAddress
	kvs.peers = peers
	kvs.lastPutTime = time.Now()
	// Initialize ValueLog and LevelDB (Paths would be specified here).
	// 在这个.代表的是打开的工作区或文件夹的根目录，即FlexSync。指向的是VSCode左侧侧边栏（Explorer栏）中展示的最顶层文件夹。
	// valuelog, err := NewValueLog("./kvstore/kvserver/valueLog_WiscKey.log", "./kvstore/kvserver/db_key_addr")
	// if err != nil {
	// 	fmt.Println("生成valuelog和leveldb文件有问题")
	// 	panic(err)
	// }
	// 这里不直接用kvs.valuelog接受上述NewValueLog函数的返回值，是因为需要先接受该函数的返回值，检查是否有错误发生，如果没有错误，才能将其值赋值给其他值。
	// kvs.valuelog = valuelog
	return kvs
}

// 拿到当前的server在server组中的下标，也用作后续Raft中的一系列与角色有关的Id
func FindIndexInPeers(arr []string, target string) int {
	for index, value := range arr {
		if value == target {
			return index
		}
	}
	return -1 // 如果未找到，返回-1
}

func (kvs *KVServer) applyLoop() {
	for !kvs.killed() {
		select {
		case msg := <-kvs.applyCh:
			// 如果是安装快照
			if msg.CommandValid {
				cmd := msg.Command
				index := msg.CommandIndex
				cmdTerm := msg.CommandTerm
				offset  := msg.Offset

				func() {
					kvs.mu.Lock()
					defer kvs.mu.Unlock()
					// 更新已经应用到的日志
					kvs.lastAppliedIndex = index
					// fmt.Println("进入到applyLoop")
					// 操作日志
					op := cmd.(raft.DetailCod) // 操作在server端的PutAppend函数中已经调用Raft的Start函数，将请求以Op的形式存入日志。

					if op.OpType == "TermLog" { // 需要进行类型断言才能访问结构体的字段，如果是leader开始第一个Term时发起的空指令，则不用执行。
						return
					}

					opCtx, existOp := kvs.reqMap[index]          // 检查当前index对应的等待put的请求是否超时，即是否还在等待被apply
					prevSeq, existSeq := kvs.seqMap[op.ClientId] // 上一次该客户端发来的请求的序号
					kvs.seqMap[op.ClientId] = op.SeqId           // 更新服务器端，客户端请求的序列号

					if existOp { // 存在等待结果的apply日志的RPC, 那么判断状态是否与写入时一致，可能之前接受过该日志，但是身份不是leader了，该index对应的请求日志被别的leader同步日志时覆盖了。
						// 虽然没超时，但是如果已经和刚开始写入的请求不一致了，那也不行。
						if opCtx.op.Term != int32(cmdTerm) { //这里要用msg里面的CommandTerm而不是cmd里面的Term，因为当拿去到的是空指令时，其cmd里面的Term是0，会重复发生错误
							// fmt.Printf("这里有问题吗,opCtx.op.Term:%v,op.Term:%v\n",opCtx.op.Term,op.Term)
							opCtx.wrongLeader = true
						}
					}

					// 只处理ID单调递增的客户端写请求
					if op.OpType == OP_TYPE_PUT {
						if !existSeq || op.SeqId > prevSeq { // 如果是客户端第一次发请求，或者发生递增的请求ID，即比上次发来请求的序号大，那么接受它的变更
							// kvs.kvStore[op.Key] = op.Value		// ----------------------------------------------
							if op.SeqId % 10000 == 0 {
								fmt.Println("底层执行了Put请求，以及重置put操作时间")
							}
							kvs.lastPutTime = time.Now() // 更新put操作时间

							// 将整数编码为字节流并存入 LevelDB
							// indexKey := make([]byte, 4)                            // 假设整数是 int32 类型
							// binary.BigEndian.PutUint32(indexKey, uint32(op.Index)) // 这里注意是把op.Index放进去还是对应日志的entry.Command.Index，两者应该都一样
							// kvs.persister.Put(op.Key, indexKey)                    // <key,idnex>,其中index是string类型
							// addrs := kvs.raft.GetOffsets()		// 拿到raft层的offsets，这个可以优化用通道传输
							// addr := addrs[op.Index]
							positionBytes := make([]byte, binary.MaxVarintLen64)		// 相当于把地址（指向keysize开始处）压缩一下
							binary.PutVarint(positionBytes, offset)
							kvs.persister.Put(op.Key,positionBytes)		

							// kvs.persister.Put(op.Key, []byte(op.Value))
						} else if existOp { // 虽然该请求的处理还未超时，但是已经处理过了。
							opCtx.ignored = true
						}
					} else { // OP_TYPE_GET
						if existOp { // 如果是GET请求，只要没超时，都可以进行幂等处理
							// opCtx.value, opCtx.keyExist = kvs.kvStore[op.Key]	// --------------------------------------------
							// value := kvs.persister.Get(op.Key)		leveldb拿取value

							// 从 LevelDB 中获取键对应的值，并解码为整数
							positionBytes := kvs.persister.Get(op.Key)
							position, _ := binary.Varint(positionBytes) // 将字节流解码为整数，拿到key对应的index
							if positionBytes == nil {	//  说明leveldb中没有该key
								opCtx.keyExist = false
								opCtx.value = ""
							}else{
								value, err := kvs.raft.ReadValueFromFile("data.log", position)
								if err != nil {
									fmt.Println("拿取value有问题")
									panic(err)
								}
								opCtx.value = value
							}
						}
					}

					// 唤醒挂起的RPC
					if existOp { // 如果等待apply的请求还没超时
						close(opCtx.committed)
					}
				}()
			}
		}
	}
}

func main() {
	// peers inputed by command line
	flag.Parse()
	gap,_ := strconv.Atoi(*gap_arg)
	internalAddress := *internalAddress_arg // 取出指针所指向的值，存入internalAddress变量
	address := *address_arg
	peers := strings.Split(*peers_arg, ",") // 将逗号作为分隔符传递给strings.Split函数，以便将peers_arg字符串分割成多个子字符串，并存储在peers的切片中
	kvs := MakeKVServer(address, internalAddress, peers)

	// Raft层
	kvs.applyCh = make(chan raft.ApplyMsg, 3) // 至少1个容量，启动后初始化snapshot用
	kvs.me = FindIndexInPeers(peers, internalAddress)
	// persisterRaft := &raft.Persister{} // 初始化对Raft进行持久化操作的指针
	kvs.reqMap = make(map[int]*OpContext)
	kvs.seqMap = make(map[int64]int64)
	kvs.lastAppliedIndex = 0
	kvs.persister.Init("./kvstore/kvserver/db_key_index")	// 初始化存储<key,index>的leveldb文件

	go kvs.applyLoop()

	ctx, cancel := context.WithCancel(context.Background())
	go kvs.RegisterKVServer(ctx, kvs.address)
	go func() {
		timeout := 38 * time.Second
		for {
			time.Sleep(timeout)
			if (time.Since(kvs.lastPutTime) > timeout)&&(time.Since(kvs.raft.LastAppendTime) > timeout) {
				cancel() // 超时后取消上下文
				fmt.Println("38秒没有请求，停止服务器")
				wg.Done()

				kvs.raft.Kill() // 关闭Raft层
				return          // 退出main函数
			}
		}
	}()
	wg.Add(1 + 1)
	kvs.raft = raft.Make(kvs.peers, kvs.me, kvs.persister, kvs.applyCh, ctx) // 开启Raft
	kvs.raft.Gap = gap

	wg.Wait()
}
