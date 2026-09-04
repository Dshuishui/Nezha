// SCAN 值正确性校验。
//
// 现有的 scan benchmark 只统计 len(reply.KeyValuePairs)——返回非空就算成功，
// 值对不对没人查。于是一条读出垃圾 value 的 SCAN 路径可以长期显示"正常"。
//
// 这里写入可从 key 推出的 value，SCAN 回来逐条比对。GET 走一遍同样的校验作为
// 对照：GET 正确而 SCAN 错误，说明问题出在 SCAN 独有的解析路径上，而不是写入。
package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	crand "crypto/rand"
	"math/big"
)

var (
	ser    = flag.String("servers", "", "server addresses")
	dnums  = flag.Int("dnums", 2000, "number of keys to write")
	vsize  = flag.Int("vsize", 64, "value size in bytes")
	spanN  = flag.Int("span", 50, "keys per scan range")
	sample = flag.Int("sample", 20, "number of scan ranges to check")
	// 首个联系的服务器在 -servers 里的下标。列表顺序必须与集群 peers 一致，因为
	// ErrWrongLeader 带回的 LeaderId 就是 peers 下标；故障切换后旧 leader 已死，
	// 用它把首次请求指向一个活着的节点，再由重定向找到真 leader。
	leader = flag.Int("leader", 0, "index in -servers to contact first")
)

// expectedValue 从 key 派生 value，长度补齐到 vsize。
// 只要 value 能从 key 算出来，SCAN 返回的每一条都能独立验证。
func expectedValue(key string, size int) string {
	seed := "v" + key + "_"
	var b strings.Builder
	for b.Len() < size {
		b.WriteString(seed)
	}
	return b.String()[:size]
}

type KVClient struct {
	Kvservers []string
	clientId  int64
	seqId     int64
	leaderId  int
	pools     []pool.Pool
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	return bigx.Int64()
}

func (kvc *KVClient) InitPool() {
	opts := pool.Options{Dial: pool.Dial, MaxIdle: 16, MaxActive: 32, MaxConcurrentStreams: 64, Reuse: true}
	for i := range kvc.Kvservers {
		p, err := pool.New([]string{kvc.Kvservers[i]}, opts)
		if err != nil {
			util.EPrintf("failed to new pool: %v", err)
		}
		kvc.pools = append(kvc.pools, p)
	}
}

func (kvc *KVClient) put(key, value string) error {
	req := &kvrpc.PutInRaftRequest{
		Key: key, Value: value, Op: "Put",
		ClientId: kvc.clientId, SeqId: atomic.AddInt64(&kvc.seqId, 1),
	}
	for {
		conn, err := kvc.pools[kvc.leaderId].Get()
		if err != nil {
			return err
		}
		client := kvrpc.NewKVClient(conn.Value())
		ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
		reply, err := client.PutInRaft(ctx, req)
		cancel()
		conn.Close()
		if err != nil {
			return err
		}
		if reply.Err == raft.ErrWrongLeader {
			kvc.leaderId = int(reply.LeaderId)
			continue
		}
		return nil
	}
}

func (kvc *KVClient) get(key string) (string, error) {
	req := &kvrpc.GetInRaftRequest{Key: key, ClientId: kvc.clientId, SeqId: atomic.AddInt64(&kvc.seqId, 1)}
	conn, err := kvc.pools[kvc.leaderId].Get()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reply, err := client.GetInRaft(ctx, req)
	if err != nil {
		return "", err
	}
	return reply.Value, nil
}

func (kvc *KVClient) scan(k1, k2 string) (map[string]string, string, error) {
	req := &kvrpc.ScanRangeRequest{StartKey: k1, EndKey: k2}
	conn, err := kvc.pools[kvc.leaderId].Get()
	if err != nil {
		return nil, "", err
	}
	defer conn.Close()
	client := kvrpc.NewKVClient(conn.Value())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	reply, err := client.ScanRangeInRaft(ctx, req)
	if err != nil {
		return nil, "", err
	}
	return reply.KeyValuePairs, reply.Err, nil
}

// trunc 让错误样例在终端里可读：垃圾 value 往往含不可打印字节。
func trunc(s string) string {
	if len(s) > 40 {
		s = s[:40] + "..."
	}
	return strconv.QuoteToASCII(s)
}

func main() {
	flag.Parse()
	kvc := &KVClient{Kvservers: strings.Split(*ser, ","), clientId: nrand(), leaderId: *leader}
	kvc.InitPool()

	fmt.Printf("写入 %d 条 (value=%dB)...\n", *dnums, *vsize)
	for i := 0; i < *dnums; i++ {
		k := strconv.Itoa(i)
		if err := kvc.put(k, expectedValue(k, *vsize)); err != nil {
			fmt.Printf("put %s 失败: %v\n", k, err)
			return
		}
	}

	// GET 对照：确认写入本身没问题，把问题范围收窄到读路径
	var getOK, getBad int
	for i := 0; i < *dnums && i < 200; i++ {
		k := strconv.Itoa(i)
		v, err := kvc.get(k)
		if err != nil {
			continue
		}
		if v == expectedValue(k, *vsize) {
			getOK++
		} else {
			getBad++
			if getBad <= 3 {
				fmt.Printf("  GET 值错 key=%s 期望=%s 实际=%s\n", k, trunc(expectedValue(k, *vsize)), trunc(v))
			}
		}
	}
	fmt.Printf("GET 校验: 正确 %d, 错误 %d\n", getOK, getBad)

	var total, ok, bad int
	var scanErr int
	for s := 0; s < *sample; s++ {
		lo := s * *spanN
		hi := lo + *spanN - 1
		if lo >= *dnums {
			break
		}
		pairs, rerr, err := kvc.scan(strconv.Itoa(lo), strconv.Itoa(hi))
		if err != nil || rerr != raft.OK {
			scanErr++
			fmt.Printf("  SCAN [%d,%d] 失败: err=%v reply.Err=%q\n", lo, hi, err, rerr)
			continue
		}
		for k, v := range pairs {
			total++
			if v == expectedValue(k, *vsize) {
				ok++
			} else {
				bad++
				if bad <= 5 {
					fmt.Printf("  SCAN 值错 key=%s 期望=%s 实际=%s\n", k, trunc(expectedValue(k, *vsize)), trunc(v))
				}
			}
		}
	}
	fmt.Printf("SCAN 校验: 返回 %d 条, 正确 %d, 错误 %d, 范围失败 %d\n", total, ok, bad, scanErr)
	if bad > 0 || scanErr > 0 {
		fmt.Println("VERIFY_FAIL")
	} else if total == 0 {
		fmt.Println("VERIFY_EMPTY")
	} else {
		fmt.Println("VERIFY_OK")
	}
}
