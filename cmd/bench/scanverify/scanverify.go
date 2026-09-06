// SCAN 值正确性校验。
//
// 现有的 scan benchmark 只统计 len(reply.KeyValuePairs)——返回非空就算成功，
// 值对不对没人查。于是一条读出垃圾 value 的 SCAN 路径可以长期显示"正常"。
//
// 这里写入可从 key 推出的 value，SCAN 回来逐条比对。GET 走一遍同样的校验作为
// 对照：GET 正确而 SCAN 错误，说明问题出在 SCAN 独有的解析路径上，而不是写入。
package main

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/client"
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

// trunc 让错误样例在终端里可读：垃圾 value 往往含不可打印字节。
func trunc(s string) string {
	if len(s) > 40 {
		s = s[:40] + "..."
	}
	return strconv.QuoteToASCII(s)
}

func main() {
	flag.Parse()
	kvc, err := client.New(strings.Split(*ser, ","), client.Options{Leader: *leader, PoolMaxIdle: 16, PoolMaxActive: 32, PoolMaxConcurrentStreams: 64, PutTimeout: 70 * time.Second, GetTimeout: 10 * time.Second, ScanTimeout: 30 * time.Second})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kvc.Close()

	fmt.Printf("写入 %d 条 (value=%dB)...\n", *dnums, *vsize)
	for i := 0; i < *dnums; i++ {
		k := strconv.Itoa(i)
		if reply, err := kvc.Put(k, expectedValue(k, *vsize)); err != nil || reply.Err != kvrpc.OK {
			fmt.Printf("put %s 失败: %v %s\n", k, err, reply.GetErr())
			return
		}
	}

	// GET 对照：确认写入本身没问题，把问题范围收窄到读路径
	var getOK, getBad int
	for i := 0; i < *dnums && i < 200; i++ {
		k := strconv.Itoa(i)
		v, _, err := kvc.Get(k)
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
		reply, err := kvc.Scan(strconv.Itoa(lo), strconv.Itoa(hi))
		if err != nil || reply.Err != kvrpc.OK {
			scanErr++
			fmt.Printf("  SCAN [%d,%d] 失败: err=%v reply.Err=%q\n", lo, hi, err, reply.GetErr())
			continue
		}
		for k, v := range reply.KeyValuePairs {
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
