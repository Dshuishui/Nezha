package kvstore

import (
	"context"
	"fmt"
	"os"
	"time"
)

// gcLoop 每 5 秒查一次 valuelog，达到触发条件就开一轮：第一轮把整个日志重写成按 key 区间
// 分区的有序文件，之后每一轮把尾部日志**吸收**进它实际覆盖到的那些分区。轮数不设上限，
// 相邻两轮不重叠。
func (kvs *KVServer) gcLoop(ctx context.Context) {
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

		// 触发：第一轮看绝对大小（还没有分区，没有可比的分母），之后看**尾部相对分区总量的比例**。
		//
		// 原先两轮之后就永久停止（numGC >= 2），valuelog 从此无限增长、空间放大无界。
		// 而如果只是简单去掉那个上限、继续按绝对阈值触发，代价会变成 O(n²)：第 k 轮重写
		// k×阈值 的活数据，却只吸收了一个阈值的新数据。重写多少不是问题，**"重写多少"与
		// "因此吸收了多少新数据"不成比例才是问题**。
		//
		// 按比例触发把两者绑在一起：每重写一次 O(n) 之前必然已吸收 O(n) 新数据，
		// 摊销后每字节 O(1)。绝对阈值降级为下限，防止数据量很小时反复做无意义的小吸收。
		tailBytes := fileInfo.Size()
		floor := int64(kvs.gcThresholdGB * 1073741824)
		need := floor
		if !kvs.FirstGC {
			if r := int64(float64(kvs.lastPartitions.TotalSize()) * kvs.absorbRatio); r > need {
				need = r
			}
		}
		if tailBytes < need {
			continue
		}
		if kvs.gcInProgress {
			continue // the previous round (possibly a post-restart redo) has not finished
		}
		// 第一轮GC
		if kvs.FirstGC {
			fmt.Printf("文件 %s 大小 %.1fMB 达到阈值 %.1fMB，开始第一轮 GC\n",
				kvs.currentLog, float64(tailBytes)/1048576, float64(need)/1048576)
			startTime := time.Now()
			err = kvs.FirstGarbageCollection()
			if err != nil {
				// 失败就停在这里：状态不推进、旧文件不删。此前的做法是照样推进 numGC
				// 并且 os.Remove(kvs.oldLog)——可数据还没完整搬进排序文件，删掉源文件
				// 就是永久丢数据。下一轮 5 秒检查会重试。
				fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
				continue
			}
			if kvs.firstPartitions == nil {
				fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
				continue
			}
			kvs.finishFirstGC(startTime)
		} else if kvs.lastGCFinish {
			if kvs.lastPartitions == nil {
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
			if kvs.anotherPartitions == nil {
				fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
				kvs.lastGCFinish = true
				continue
			}
			kvs.finishAnotherGC(startTime)
		}

	}
}
