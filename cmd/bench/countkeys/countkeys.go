// countkeys 直接遍历节点的 RocksDB，数出实际持久化了多少个 key。
//
// 用途：判定写入端报告的 goodPut 缺口是不是真的丢数据。
//
// 每轮写入都有约 0.01% 的请求被计为失败（例如 999915/1000000）。唯一来源是
// StartPut 里的超时分支：
//
//	case <-timer.C:  reply.Err = "defeat"
//
// 即 60 秒内没等到 apply 回调。但日志此时早已落盘，apply 也可能只是晚到——
// "客户端没收到确认"和"数据没写进去"是两回事，靠客户端的计数分不出来。
//
// 从服务端数才有答案：RocksDB 里存的是 key -> valuelog 偏移，所以它的 key 数
// 就是真正完成持久化的写入数。
//
//	实际 key 数 == 请求总数    -> 数据完好，goodPut 只是漏记确认
//	实际 key 数 == goodPut     -> 确实丢了那些写入
//
// 必须在节点停止后运行：RocksDB 是独占打开的。
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/linxGnu/grocksdb"

	"gitee.com/dong-shuishui/FlexSync/internal/raft"
)

var (
	dbPath = flag.String("db", "", "RocksDB 目录（节点 data 目录下的那个）")
	sample = flag.Int("sample", 5, "打印前若干个 key 作为抽样核对")
)

func main() {
	flag.Parse()
	if *dbPath == "" {
		fmt.Fprintln(os.Stderr, "用法: countkeys -db <rocksdb 目录>")
		os.Exit(2)
	}

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(false)
	db, err := grocksdb.OpenDbForReadOnly(opts, *dbPath, false)
	if err != nil {
		fmt.Fprintf(os.Stderr, "打开 RocksDB 失败（节点是否仍在运行？）: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false) // 全量扫描，不必污染 block cache
	defer ro.Destroy()

	it := db.NewIterator(ro)
	defer it.Close()

	// 恢复用的 applied index 与用户数据同库，靠首字节 0x00 区分（raft.IsMetaKey）。
	// 把它算进 KEYCOUNT 会让计数恒比写入条数多 1，而 verify-goodput.sh 做的是**精确
	// 相等**比较——于是"数据完好"和"确实丢了"两个分支都不成立，判定一直落在兜底分支上。
	var count, meta int
	var samples []string
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		if raft.IsMetaKey(k.Data()) {
			meta++
			k.Free()
			it.Value().Free()
			continue
		}
		count++
		if len(samples) < *sample {
			samples = append(samples, string(k.Data()))
		}
		k.Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "迭代出错: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("KEYCOUNT %d\n", count)
	if meta > 0 {
		fmt.Printf("（另有 %d 行存储层元数据，未计入）\n", meta)
	}
	if len(samples) > 0 {
		fmt.Printf("样例 key: %s\n", strings.Join(samples, " "))
	}
}
