// 只读校验：不写入，只核对已有数据。用于故障切换后检验新 leader 的数据完整性。
package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/internal/pool"
	"gitee.com/dong-shuishui/FlexSync/internal/raft"
	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/util"

	crand "crypto/rand"
	"math/big"
)

var (
	ser    = flag.String("servers", "", "server addresses")
	dnums  = flag.Int("dnums", 20000, "keys previously written")
	vsize  = flag.Int("vsize", 1024, "value size")
	spanN  = flag.Int("span", 50, "keys per scan range")
	sample = flag.Int("sample", 20, "scan ranges to check")
	checkN = flag.Int("check", 200, "point reads to check")
)

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
	m := big.NewInt(int64(1) << 62)
	b, _ := crand.Int(crand.Reader, m)
	return b.Int64()
}

func (kvc *KVClient) InitPool() {
	o := pool.Options{Dial: pool.Dial, MaxIdle: 16, MaxActive: 32, MaxConcurrentStreams: 64, Reuse: true}
	for i := range kvc.Kvservers {
		p, err := pool.New([]string{kvc.Kvservers[i]}, o)
		if err != nil {
			util.EPrintf("pool: %v", err)
		}
		kvc.pools = append(kvc.pools, p)
	}
}

func (kvc *KVClient) get(key string) (string, error) {
	req := &kvrpc.GetInRaftRequest{Key: key, ClientId: kvc.clientId, SeqId: atomic.AddInt64(&kvc.seqId, 1)}
	conn, err := kvc.pools[kvc.leaderId].Get()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r, err := kvrpc.NewKVClient(conn.Value()).GetInRaft(ctx, req)
	if err != nil {
		return "", err
	}
	return r.Value, nil
}

func (kvc *KVClient) scan(k1, k2 string) (map[string]string, string, error) {
	conn, err := kvc.pools[kvc.leaderId].Get()
	if err != nil {
		return nil, "", err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := kvrpc.NewKVClient(conn.Value()).ScanRangeInRaft(ctx, &kvrpc.ScanRangeRequest{StartKey: k1, EndKey: k2})
	if err != nil {
		return nil, "", err
	}
	return r.KeyValuePairs, r.Err, nil
}

func trunc(s string) string {
	if len(s) > 40 {
		s = s[:40] + "..."
	}
	return strconv.QuoteToASCII(s)
}

func main() {
	flag.Parse()
	kvc := &KVClient{Kvservers: strings.Split(*ser, ","), clientId: nrand()}
	kvc.InitPool()

	var ok, bad, missing int
	for i := 0; i < *checkN && i < *dnums; i++ {
		k := strconv.Itoa(i)
		v, err := kvc.get(k)
		if err != nil {
			missing++
			continue
		}
		if v == expectedValue(k, *vsize) {
			ok++
		} else {
			bad++
			if bad <= 3 {
				fmt.Printf("  GET 值错 key=%s 期望=%s 实际=%s\n", k, trunc(expectedValue(k, *vsize)), trunc(v))
			}
		}
	}
	fmt.Printf("GET 校验: 正确 %d, 错误 %d, 取不到 %d\n", ok, bad, missing)

	var st, sok, sbad, serr int
	for s := 0; s < *sample; s++ {
		lo := s * *spanN
		hi := lo + *spanN - 1
		if lo >= *dnums {
			break
		}
		pairs, rerr, err := kvc.scan(strconv.Itoa(lo), strconv.Itoa(hi))
		if err != nil || rerr != raft.OK {
			serr++
			continue
		}
		for k, v := range pairs {
			st++
			if v == expectedValue(k, *vsize) {
				sok++
			} else {
				sbad++
			}
		}
	}
	fmt.Printf("SCAN 校验: 返回 %d 条, 正确 %d, 错误 %d, 范围失败 %d\n", st, sok, sbad, serr)
	if bad > 0 || sbad > 0 || missing > 0 || serr > 0 {
		fmt.Println("FAILOVER_VERIFY_FAIL")
	} else if ok == 0 || st == 0 {
		fmt.Println("FAILOVER_VERIFY_EMPTY")
	} else {
		fmt.Println("FAILOVER_VERIFY_OK")
	}
}
