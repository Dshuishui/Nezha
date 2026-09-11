// 只读校验：不写入，只核对已有数据。用于故障切换后检验新 leader 的数据完整性。
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

func trunc(s string) string {
	if len(s) > 40 {
		s = s[:40] + "..."
	}
	return strconv.QuoteToASCII(s)
}

func main() {
	flag.Parse()
	// Reads go to the first server as given, never redirected: the point is to inspect
	// that node's own state (a follower's, after a failover).
	kvc, err := client.New(strings.Split(*ser, ","), client.Options{PoolMaxIdle: 16, PoolMaxActive: 32, PoolMaxConcurrentStreams: 64, GetTimeout: 10 * time.Second, ScanTimeout: 30 * time.Second})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer kvc.Close()

	// wrongLeader 单独计数，不混进 missing。GetFrom 故意不跟随重定向（它要看的就是**这一个**
	// 节点的本地状态），所以打在还没当选的节点上会全部拿到 ErrWrongLeader。此前这类回应被
	// 计成"取不到"，于是"节点刚重启、还没选出 leader"看起来和"数据丢了"一模一样——
	// 实测 300 个 GET 全部"取不到"，排查了一轮才发现节点只是还没当选。
	var ok, bad, missing, wrongLeader int
	for i := 0; i < *checkN && i < *dnums; i++ {
		k := strconv.Itoa(i)
		reply, err := kvc.GetFrom(0, k)
		if err == nil && reply.Err == kvrpc.ErrWrongLeader {
			wrongLeader++
			continue
		}
		if err != nil || reply.Err != kvrpc.OK {
			missing++
			continue
		}
		v := reply.Value
		if v == expectedValue(k, *vsize) {
			ok++
		} else {
			bad++
			if bad <= 3 {
				fmt.Printf("  GET 值错 key=%s 期望=%s 实际=%s\n", k, trunc(expectedValue(k, *vsize)), trunc(v))
			}
		}
	}
	fmt.Printf("GET 校验: 正确 %d, 错误 %d, 取不到 %d, 非leader %d\n", ok, bad, missing, wrongLeader)

	var st, sok, sbad, serr, swrong int
	for s := 0; s < *sample; s++ {
		lo := s * *spanN
		hi := lo + *spanN - 1
		if lo >= *dnums {
			break
		}
		reply, err := kvc.ScanFrom(0, strconv.Itoa(lo), strconv.Itoa(hi))
		if err == nil && reply.Err == kvrpc.ErrWrongLeader {
			swrong++
			continue
		}
		if err != nil || reply.Err != kvrpc.OK {
			serr++
			continue
		}
		for k, v := range reply.KeyValuePairs {
			st++
			if v == expectedValue(k, *vsize) {
				sok++
			} else {
				sbad++
			}
		}
	}
	fmt.Printf("SCAN 校验: 返回 %d 条, 正确 %d, 错误 %d, 范围失败 %d, 非leader %d\n", st, sok, sbad, serr, swrong)
	if wrongLeader > 0 || swrong > 0 {
		// 单独的一条：这说明目标节点还没当选，不是数据有问题。要么等它当选，
		// 要么用 -leaderCheck=false 启动节点以读取 follower 的本地状态。
		fmt.Printf("目标节点未持有 leader 身份：GET %d 次、SCAN %d 次被 ErrWrongLeader 挡回\n",
			wrongLeader, swrong)
	}
	if bad > 0 || sbad > 0 || missing > 0 || serr > 0 || wrongLeader > 0 || swrong > 0 {
		fmt.Println("FAILOVER_VERIFY_FAIL")
	} else if ok == 0 || st == 0 {
		fmt.Println("FAILOVER_VERIFY_EMPTY")
	} else {
		fmt.Println("FAILOVER_VERIFY_OK")
	}
}
