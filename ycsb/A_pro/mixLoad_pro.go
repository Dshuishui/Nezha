package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/pool"
	"gitee.com/dong-shuishui/FlexSync/raft"
	"gitee.com/dong-shuishui/FlexSync/rpc/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/util"
)

var (
	ser        = flag.String("servers", "", "the Server, Client Connects to")
	cnums      = flag.Int("cnums", 1, "Client Threads Number")
	dnums      = flag.Int("dnums", 100000, "total operation number")
	vsize      = flag.Int("vsize", 64, "value size in bytes")
	writeRatio = flag.Float64("wratio", 0.5, "write ratio (0-1.0)")

	rounds = flag.Int("rounds", 1, "how many rounds to run")
	out    = flag.String("out", "mixLoad_results.txt", "output txt filename (in same dir as this .go file)")
)

type KVClient struct {
	Kvservers []string
	mu        sync.Mutex
	clientId  int64
	seqId     int64
	leaderId  int
	pools     []pool.Pool
}

type WorkloadStats struct {
	totalReads      int64
	totalWrites     int64
	throughput      float64
	avgReadLatency  time.Duration
	avgWriteLatency time.Duration
	successOps      int64
	valueSize       int
	maxThreadDur    time.Duration
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	return bigx.Int64()
}

func generateUniqueRandomInts(min, max int) []int {
	nums := make([]int, max-min+1)
	for i := range nums {
		nums[i] = min + i
	}
	rand.Shuffle(len(nums), func(i, j int) { nums[i], nums[j] = nums[j], nums[i] })
	return nums
}

// ========== RPC ==========

func (kvc *KVClient) changeToLeader(id int) int {
	kvc.mu.Lock()
	defer kvc.mu.Unlock()
	kvc.leaderId = id
	return kvc.leaderId
}

func (kvc *KVClient) InitPool() {
	DesignOptions := pool.Options{
		Dial:                 pool.Dial,
		MaxIdle:              150,
		MaxActive:            300,
		MaxConcurrentStreams: 800,
		Reuse:                true,
	}

	for i := 0; i < len(kvc.Kvservers); i++ {
		peer := []string{kvc.Kvservers[i]}
		p, err := pool.New(peer, DesignOptions)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		kvc.pools = append(kvc.pools, p)
	}
}

func (kvc *KVClient) Get(key string) (string, bool, error) {
	args := &kvrpc.GetInRaftRequest{
		Key:      key,
		ClientId: kvc.clientId,
		SeqId:    atomic.AddInt64(&kvc.seqId, 1),
	}
	targetId := kvc.leaderId

	for {
		reply, err := kvc.SendGetInRaft(targetId, args)
		if err != nil {
			return "", false, err
		}

		if reply.Err == raft.OK {
			return reply.Value, true, nil
		} else if reply.Err == raft.ErrNoKey {
			return reply.Value, false, nil
		} else if reply.Err == raft.ErrWrongLeader {
			targetId = int(reply.LeaderId)
			time.Sleep(1 * time.Millisecond)
		}
	}
}

func (kvc *KVClient) SendGetInRaft(targetId int, request *kvrpc.GetInRaftRequest) (*kvrpc.GetInRaftResponse, error) {
	p := kvc.pools[targetId]
	conn, err := p.Get()
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()

	return client.GetInRaft(ctx, request)
}

func (kvc *KVClient) PutInRaft(key, value string) (*kvrpc.PutInRaftResponse, error) {
	request := &kvrpc.PutInRaftRequest{
		Key:      key,
		Value:    value,
		Op:       "Put",
		ClientId: kvc.clientId,
		SeqId:    atomic.AddInt64(&kvc.seqId, 1),
	}

	for {
		p := kvc.pools[kvc.leaderId]
		conn, err := p.Get()
		if err != nil {
			util.EPrintf("failed to get conn: %v", err)
			// 这里直接返回更合理，避免空转
			return nil, err
		}

		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		reply, callErr := client.PutInRaft(ctx, request)
		cancel()
		conn.Close()

		if callErr != nil {
			return nil, callErr
		}

		if reply.Err == raft.OK {
			return reply, nil
		} else if reply.Err == raft.ErrWrongLeader {
			kvc.changeToLeader(int(reply.LeaderId))
			continue
		} else if reply.Err == "defeat" {
			return reply, nil
		}
	}
}

// ========== workload ==========

type threadAgg struct {
	successOps int64
	reads      int64
	writes     int64
	readSum    time.Duration
	writeSum   time.Duration
	valueSize  int
	threadDur  time.Duration
}

func (kvc *KVClient) mixedWorkload(writeRatio float64, value string) *WorkloadStats {
	stats := &WorkloadStats{}
	opsPerThread := *dnums / *cnums

	// 你原来用 baga=6250000，这里保留
	var baga = 6250000
	allKeys := generateUniqueRandomInts(0, baga)

	results := make(chan threadAgg, *cnums)
	wg := sync.WaitGroup{}
	wg.Add(*cnums)

	for tid := 0; tid < *cnums; tid++ {
		go func(threadID int) {
			defer wg.Done()

			start := threadID * opsPerThread
			end := (threadID + 1) * opsPerThread
			if threadID == *cnums-1 {
				end = *dnums
			}
			localOps := end - start
			if localOps <= 0 {
				results <- threadAgg{}
				return
			}

			// 精确写比例 + shuffle
			writeCount := int(float64(localOps) * writeRatio)
			operations := make([]bool, localOps)
			for i := 0; i < writeCount; i++ {
				operations[i] = true
			}
			rand.Shuffle(len(operations), func(i, j int) { operations[i], operations[j] = operations[j], operations[i] })

			agg := threadAgg{}
			threadStart := time.Now()

			for i := 0; i < localOps; i++ {
				isWrite := operations[i]
				keyInt := allKeys[(start+i)%len(allKeys)]
				key := strconv.Itoa(keyInt)

				opStart := time.Now()

				if isWrite {
					reply, err := kvc.PutInRaft(key, value)
					lat := time.Since(opStart)
					ok := err == nil && reply != nil && reply.Err != "defeat"

					if ok {
						agg.successOps++
						agg.writes++
						agg.writeSum += lat
						agg.valueSize = len(value)
					}
				} else {
					// 读仍然随机 key（沿用你原逻辑）
					randKey := rand.Intn(baga)
					randStrKey := strconv.Itoa(randKey)

					v, exists, err := kvc.Get(randStrKey)
					lat := time.Since(opStart)
					ok := err == nil && exists && v != "ErrNoKey"

					if ok {
						agg.successOps++
						agg.reads++
						agg.readSum += lat
						agg.valueSize = len([]byte(v))
					}
				}
			}

			agg.threadDur = time.Since(threadStart)
			results <- agg
		}(tid)
	}

	wg.Wait()
	close(results)

	var (
		totalOps    int64
		totalReads  int64
		totalWrites int64

		readSum  time.Duration
		writeSum time.Duration

		maxDur  time.Duration
		valueSz int
	)

	for r := range results {
		totalOps += r.successOps
		totalReads += r.reads
		totalWrites += r.writes
		readSum += r.readSum
		writeSum += r.writeSum
		if r.threadDur > maxDur {
			maxDur = r.threadDur
		}
		if r.valueSize > 0 {
			valueSz = r.valueSize
		}
	}

	stats.successOps = totalOps
	stats.totalReads = totalReads
	stats.totalWrites = totalWrites
	stats.valueSize = valueSz
	stats.maxThreadDur = maxDur

	if totalReads > 0 {
		stats.avgReadLatency = readSum / time.Duration(totalReads)
	}
	if totalWrites > 0 {
		stats.avgWriteLatency = writeSum / time.Duration(totalWrites)
	}
	// 吞吐按你原来：成功操作数 * valueSize / maxDuration
	if maxDur > 0 && valueSz > 0 {
		stats.throughput = (float64(totalOps) * float64(valueSz) / 1000000.0) / maxDur.Seconds()
	}

	return stats
}

// ========== output ==========

func thisFileDir() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		// 退化：当前工作目录
		cwd, _ := os.Getwd()
		return cwd
	}
	return filepath.Dir(filename)
}

func writeLine(f *os.File, s string) {
	_, _ = f.WriteString(s)
	if len(s) == 0 || s[len(s)-1] != '\n' {
		_, _ = f.WriteString("\n")
	}
}

func main() {
	flag.Parse()

	if strings.TrimSpace(*ser) == "" {
		fmt.Println("ERROR: -servers is required, e.g. -servers 192.168.1.240:3088,192.168.1.241:3088")
		return
	}

	servers := strings.Split(*ser, ",")
	value := util.GenerateLargeValue(*vsize)

	outPath := filepath.Join(thisFileDir(), *out)
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("ERROR: open output file failed: %v\n", err)
		return
	}
	defer f.Close()

	header := fmt.Sprintf(
		"\n====================\nStart: %s\nArgs: servers=%s cnums=%d dnums=%d vsize=%d wratio=%.4f rounds=%d\nOutput: %s\n====================\n",
		time.Now().Format("2006-01-02 15:04:05"),
		*ser, *cnums, *dnums, *vsize, *writeRatio, *rounds, outPath,
	)
	fmt.Print(header)
	writeLine(f, header)

	for round := 1; round <= *rounds; round++ {
		kvc := new(KVClient)
		kvc.Kvservers = servers
		kvc.clientId = nrand()
		kvc.seqId = 0
		kvc.leaderId = 0
		kvc.pools = nil

		fmt.Printf("\n[Round %d/%d] InitPool...\n", round, *rounds)
		writeLine(f, fmt.Sprintf("[Round %d/%d] %s", round, *rounds, time.Now().Format("2006-01-02 15:04:05")))

		kvc.InitPool()

		fmt.Printf("Starting mixed workload test with %.1f%% writes\n", *writeRatio*100)
		fmt.Printf("Total operations: %d, Threads: %d, Write value size: %d bytes\n", *dnums, *cnums, *vsize)

		startTime := time.Now()
		stats := kvc.mixedWorkload(*writeRatio, value)
		elapsed := time.Since(startTime)

		// stdout
		fmt.Printf("读取多少数据：%v---%v---%v\n", stats.successOps, stats.maxThreadDur, stats.valueSize)
		fmt.Printf("\nTest Results (Round %d):\n", round)
		fmt.Printf("Total time: %v\n", elapsed)
		fmt.Printf("Read operations: %d (%.1f%%)\n", stats.totalReads, float64(stats.totalReads)*100/float64(*dnums))
		fmt.Printf("Write operations: %d (%.1f%%)\n", stats.totalWrites, float64(stats.totalWrites)*100/float64(*dnums))
		fmt.Printf("Average read latency: %v\n", stats.avgReadLatency)
		fmt.Printf("Average write latency: %v\n", stats.avgWriteLatency)
		fmt.Printf("Total throughput: %.2f MB/s\n", stats.throughput)

		// file (append)
		writeLine(f, fmt.Sprintf("Round %d Results:", round))
		writeLine(f, fmt.Sprintf("  Total time: %v", elapsed))
		writeLine(f, fmt.Sprintf("  Success ops: %d", stats.successOps))
		writeLine(f, fmt.Sprintf("  Read ops: %d (%.1f%%)", stats.totalReads, float64(stats.totalReads)*100/float64(*dnums)))
		writeLine(f, fmt.Sprintf("  Write ops: %d (%.1f%%)", stats.totalWrites, float64(stats.totalWrites)*100/float64(*dnums)))
		writeLine(f, fmt.Sprintf("  Avg read latency: %v", stats.avgReadLatency))
		writeLine(f, fmt.Sprintf("  Avg write latency: %v", stats.avgWriteLatency))
		writeLine(f, fmt.Sprintf("  Throughput: %.2f MB/s", stats.throughput))
		writeLine(f, fmt.Sprintf("  Max thread duration: %v", stats.maxThreadDur))
		writeLine(f, "----")

		// cleanup (每轮完整关闭)
		for _, p := range kvc.pools {
			p.Close()
		}
		fmt.Printf("[Round %d/%d] Done.\n", round, *rounds)
	}

	footer := fmt.Sprintf("All rounds done. End: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	fmt.Print("\n" + footer)
	writeLine(f, footer)
}
