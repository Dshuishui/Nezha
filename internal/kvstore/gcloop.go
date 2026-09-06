package kvstore

import (
	"context"
	"fmt"
	"os"
	"time"
)

// gcLoop checks the value log every five seconds and starts a GC round when it exceeds
// the threshold: the first round rewrites the log into a sorted file, the second merges
// into it. Rounds are capped at two and never overlap.
func (kvs *KVServer) gcLoop(ctx context.Context) {
	// defer kvs.filePool.Close() // 程序退出时关闭池中的所有文件描述符
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		// 检查文件是否存在并且大小是否超过4GB
		fileInfo, err := os.Stat(kvs.currentLog)
		if err != nil {
			if os.IsNotExist(err) {
				// fmt.Printf("文件 %s 不存在，跳过垃圾回收\n", kvs.currentLog)
				continue
			}
			fmt.Printf("检查文件 %s 时出错: %v\n", kvs.currentLog, err)
			continue
		}

		if !kvs.gcEnabled {
			// Nezha-NoGC：只做 KV 分离，不回收 valuelog。
			continue
		}
		if !kvs.kvSeparation {
			// 基线（standard Raft+RocksDB）没有 valuelog，也就没有垃圾要回收。
			// 让它走 GC 会当场出错：RocksDB 里存的是裸 value，GC 却按偏移记录
			// 解析（unknown record tag: 0x76 —— 那是 value 的首字符）。
			// 更要命的是 GC 在搬运之前就把 persister 换成了新的空库，
			// 于是失败之后所有 GET 都返回 NOKEY。
			continue
		}

		fileSizeGB := float64(fileInfo.Size()) / (1024 * 1024 * 1024)
		if fileSizeGB <= kvs.gcThresholdGB {
			// fmt.Printf("文件 %s 大小为 %.2f GB，未达到垃圾回收阈值\n", kvs.currentLog, fileSizeGB)
			continue
		}
		if kvs.numGC >= 2 {
			// fmt.Printf("已经进行了 %d 轮垃圾回收，停止进一步的垃圾回收\n", kvs.numGC)
			continue
		}
		if kvs.gcInProgress {
			continue // the previous round (possibly a post-restart redo) has not finished
		}
		// 第一轮GC
		if kvs.FirstGC {
			fmt.Printf("文件 %s 大小为 %.2f GB，开始垃圾回收\n", kvs.currentLog, fileSizeGB)
			startTime := time.Now()
			err = kvs.FirstGarbageCollection()
			if err != nil {
				// 失败就停在这里：状态不推进、旧文件不删。此前的做法是照样推进 numGC
				// 并且 os.Remove(kvs.oldLog)——可数据还没完整搬进排序文件，删掉源文件
				// 就是永久丢数据。下一轮 5 秒检查会重试。
				fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
				continue
			}
			if kvs.firstSortedFileIndex == nil {
				fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
				continue
			}
			kvs.finishFirstGC(startTime)
		} else if kvs.lastGCFinish {
			if kvs.lastSortedFileIndex == nil {
				fmt.Println("缺少上一轮排序文件索引，跳过本轮迭代 GC")
				continue
			}
			kvs.lastGCFinish = false // make sure last gc process is finished
			startTime := time.Now()
			err = kvs.AnotherGarbageCollection()
			if err != nil {
				fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
				kvs.lastGCFinish = true
				continue
			}
			if kvs.anothersortedFileIndex == nil {
				fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
				kvs.lastGCFinish = true
				continue
			}
			kvs.finishAnotherGC(startTime)
		}

	}
}
