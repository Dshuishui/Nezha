// leader 崩溃会丢掉多少**已确认**的写。
//
// 这是 -commitQuorum leader（异步复制档）的定义性指标：那个档位让 leader 在
// 没有任何 follower 拿到这条日志时就回 OK，所以 leader 一崩，这些已经告诉客户端
// "成功了"的写就没了。**只说"可能会丢"是不够的，要给数字。**
//
// 为什么不能用现成的工具：randwrite_goroutine 只报成功**条数**，不记**哪些 key**
// 成功过；而判据必须区分两件事——
//
//	已确认却丢了   服务端回过 OK，崩溃后读不到 → 这才是数据丢失
//	没写成功       崩的那一刻正在途中，从没回过 OK → 不算丢
//
// 把后者算进去会把数字虚高好几倍，而两者在盘上长得一模一样。
//
// 用法分两步，中间由脚本 kill -9 掉 leader：
//
//	crashloss -mode write  -servers ... -dnums N -cnums C -acked /tmp/acked.txt
//	（脚本在此期间 kill -9 leader，写入方一见失败就停下并把已确认的 key 落盘）
//	crashloss -mode verify -servers ... -acked /tmp/acked.txt
//
// **必须同时跑 -commitQuorum majority 作对照**：那一档的丢失次数必须是 0，
// 因为那正是 Raft 的保证。对照不为 0 说明有比异步档重要得多的问题。
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gitee.com/dong-shuishui/FlexSync/api/kvrpc"
	"gitee.com/dong-shuishui/FlexSync/internal/client"
)

var (
	mode   = flag.String("mode", "write", "write | verify")
	ser    = flag.String("servers", "", "server addresses, comma separated")
	dnums  = flag.Int("dnums", 5000000, "写入阶段最多写多少条（通常在写完之前就被 kill 打断）")
	cnums  = flag.Int("cnums", 100, "并发写入的客户端数")
	vsize  = flag.Int("vsize", 1024, "value 字节数")
	acked  = flag.String("acked", "/tmp/crashloss-acked.txt", "已确认 key 的落盘位置")
	leader = flag.Int("leader", 0, "先联系 -servers 里的第几个；崩溃之后要指向一个活着的节点")
)

// expectedValue 从 key 派生 value：这样校验阶段不仅能查"在不在"，还能查"对不对"。
// 一个存在但内容错的 key 与一个丢失的 key 是两种不同的故障。
func expectedValue(key string, size int) string {
	seed := "v" + key + "_"
	var b strings.Builder
	for b.Len() < size {
		b.WriteString(seed)
	}
	return b.String()[:size]
}

func main() {
	flag.Parse()
	servers := strings.Split(*ser, ",")
	if *ser == "" || len(servers) == 0 {
		fmt.Println("需要 -servers")
		os.Exit(2)
	}
	switch *mode {
	case "write":
		doWrite(servers)
	case "verify":
		doVerify(servers)
	default:
		fmt.Printf("-mode 只认 write | verify，收到 %q\n", *mode)
		os.Exit(2)
	}
}

func doWrite(servers []string) {
	// PutTimeout 给 10 秒而不是别处常用的 70 秒：leader 被 kill -9 之后要**尽快**
	// 拿到失败、把切口落下。等得太久，新 leader 已经选出来、写入恢复成功，
	// 样本里就混进了"新 leader 确认的写"，而那些不该算进这次的丢失里。
	kvc, err := client.New(servers, client.Options{
		Leader: *leader, PoolMaxIdle: 16, PoolMaxActive: 32,
		PoolMaxConcurrentStreams: 64, PutTimeout: 10 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer kvc.Close()

	var next atomic.Int64 // 下一个要写的 key
	var stop atomic.Bool  // 有人失败就全体收手
	var okCount, failCount atomic.Int64
	mu := sync.Mutex{}
	ackedKeys := make([]int, 0, 1<<20)

	start := time.Now()
	var wg sync.WaitGroup
	for c := 0; c < *cnums; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]int, 0, 4096)
			for !stop.Load() {
				i := int(next.Add(1) - 1)
				if i >= *dnums {
					break
				}
				k := strconv.Itoa(i)
				reply, err := kvc.Put(k, expectedValue(k, *vsize))
				if err != nil || reply.GetErr() != kvrpc.OK {
					// **第一个失败就收手。** 崩溃之后新 leader 选出来，写入会恢复成功，
					// 那些写是好的、不该混进这次的样本里。在这里切一刀，样本就只含
					// "旧 leader 确认过的"，判据才干净。
					failCount.Add(1)
					stop.Store(true)
					break
				}
				local = append(local, i)
				okCount.Add(1)
			}
			mu.Lock()
			ackedKeys = append(ackedKeys, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	sort.Ints(ackedKeys)
	f, err := os.Create(*acked)
	if err != nil {
		fmt.Printf("写不了 %s: %v\n", *acked, err)
		os.Exit(1)
	}
	w := bufio.NewWriter(f)
	for _, k := range ackedKeys {
		fmt.Fprintln(w, k)
	}
	w.Flush()
	f.Close()

	fmt.Printf("[WRITE] acked=%d failed=%d elapsed=%.3fs rate=%.0f ops/s acked_file=%s\n",
		okCount.Load(), failCount.Load(), elapsed.Seconds(),
		float64(okCount.Load())/elapsed.Seconds(), *acked)
}

func doVerify(servers []string) {
	f, err := os.Open(*acked)
	if err != nil {
		fmt.Printf("读不了 %s: %v\n", *acked, err)
		os.Exit(1)
	}
	var keys []int
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if n, e := strconv.Atoi(strings.TrimSpace(sc.Text())); e == nil {
			keys = append(keys, n)
		}
	}
	f.Close()
	if len(keys) == 0 {
		fmt.Println("[VERIFY] 已确认 key 列表是空的——写入阶段没拿到任何 OK，这一轮说明不了任何事")
		os.Exit(1)
	}

	// -leader 要指向一个**活着**的节点：崩溃之后旧 leader 已死，首次请求打过去
	// 只会连不上；由它做起点，再靠重定向找到新 leader。
	kvc, err := client.New(servers, client.Options{
		Leader: *leader, PoolMaxIdle: 16, PoolMaxActive: 32,
		PoolMaxConcurrentStreams: 64, GetTimeout: 10 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer kvc.Close()

	var present, missing, wrong, readErr int
	var samples []string
	for _, i := range keys {
		k := strconv.Itoa(i)
		v, found, err := kvc.Get(k)
		switch {
		case err != nil:
			// 读不了与读不到是两件事，混起来会把一次读故障报成数据丢失。
			readErr++
		case !found:
			// **用 found，不要拿 value 去跟 "ErrNoKey" 这个字符串比。**
			// 后者是仓库里记着的一个已知坑：一个内容恰好等于 "ErrNoKey" 的 value
			// 会被判成缺键。存储层对"存在"的判据是 Exists()，客户端这一层是 found。
			missing++
			if len(samples) < 5 {
				samples = append(samples, k)
			}
		case v != expectedValue(k, *vsize):
			wrong++
		default:
			present++
		}
	}
	total := len(keys)
	fmt.Printf("[VERIFY] acked=%d present=%d missing=%d wrong=%d read_err=%d lost_pct=%.4f%%\n",
		total, present, missing, wrong, readErr, 100*float64(missing)/float64(total))
	if len(samples) > 0 {
		fmt.Printf("[VERIFY] 丢失样例: %s\n", strings.Join(samples, " "))
	}
}
