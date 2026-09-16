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
		// 判定要用到的可变状态一次性在锁下取下来。
		//
		// 这些字段原先是裸读的，而在快照出现之前那是安全的：写它们的只有 GC 自己这个
		// goroutine。装快照把一个**并发写者**加了进来（gRPC 的 InstallSSTable 处理
		// goroutine 在 kvs.mu 下改 currentLog / persister / 分区组），于是裸读变成了
		// 真竞态——`RACE=1` 跑 slow-follower.sh 抓到的正是这一条：
		// gcLoop 的 os.Stat(kvs.currentLog) 撞上 installSnapshot 的 kvs.currentLog = …。
		// string 是两个字（指针 + 长度），撕裂读会让 os.Stat 去 stat 一个拼接出来的路径。
		//
		// 一次取下来还有第二个好处：整个判定看到的是**同一个时刻**的状态，
		// 而不是分几次读到的、可能已经互相矛盾的几个字段。
		kvs.mu.Lock()
		curLog := kvs.currentLog
		firstGC, lastFinish, inProgress := kvs.FirstGC, kvs.lastGCFinish, kvs.gcInProgress
		partsTotal := kvs.lastPartitions.TotalSize()
		kvs.mu.Unlock()

		// 检查文件是否存在并且大小是否超过4GB
		fileInfo, err := os.Stat(curLog)
		if err != nil {
			if os.IsNotExist(err) {
				// fmt.Printf("文件 %s 不存在，跳过垃圾回收\n", curLog)
				continue
			}
			fmt.Printf("检查文件 %s 时出错: %v\n", curLog, err)
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
		if !firstGC {
			if r := int64(float64(partsTotal) * kvs.absorbRatio); r > need {
				need = r
			}
		}
		if tailBytes < need {
			continue
		}
		if inProgress {
			continue // the previous round (possibly a post-restart redo) has not finished
		}
		// 与装快照互斥。装快照会把 persister、当前日志、分区组三样一起换掉，GC 一轮里
		// 三样都要用，撞上就是拿着一半新一半旧的状态搬数据。两边都是"看见对方在跑就退回"：
		// 谁也不等谁，因为 GC 一轮可以长到数分钟，等下去会把读一起挡住。
		kvs.mu.Lock()
		if kvs.installing {
			kvs.mu.Unlock()
			continue
		}
		kvs.gcActive = true
		kvs.mu.Unlock()
		// 包一层匿名函数只为了 defer：gcActive 必须在**每一条**退出路径上清掉，
		// 而下面有五处提前返回。漏掉任何一处，装快照就会永久被这个标志挡住，
		// 而且是静默的——那个副本从此只是个摆设。所以里面的 continue 都改成 return。
		func() {
			defer func() {
				kvs.mu.Lock()
				kvs.gcActive = false
				kvs.mu.Unlock()
			}()
			// 第一轮GC。用上面在锁下取的 firstGC / lastFinish，而不是再裸读一次：
			// 再读一次既是竞态，也可能与刚才做判定时看到的状态不一致。
			if firstGC {
				fmt.Printf("文件 %s 大小 %.1fMB 达到阈值 %.1fMB，开始第一轮 GC\n",
					curLog, float64(tailBytes)/1048576, float64(need)/1048576)
				startTime := time.Now()
				err = kvs.FirstGarbageCollection()
				if err != nil {
					// 失败就停在这里：状态不推进、旧文件不删。此前的做法是照样推进 numGC
					// 并且 os.Remove(kvs.oldLog)——可数据还没完整搬进排序文件，删掉源文件
					// 就是永久丢数据。下一轮 5 秒检查会重试。
					fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
					return
				}
				if kvs.firstPartitions == nil {
					fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
					return
				}
				kvs.finishFirstGC(startTime)
			} else if lastFinish {
				if kvs.lastPartitions == nil {
					fmt.Println("缺少上一轮排序文件索引，跳过本轮迭代 GC")
					return
				}
				kvs.lastGCFinish = false // make sure last gc process is finished
				startTime := time.Now()
				err = kvs.AnotherGarbageCollection()
				if err != nil {
					fmt.Println("垃圾回收出现了错误，本轮不推进状态、不删除旧文件: ", err)
					kvs.lastGCFinish = true
					return
				}
				if kvs.anotherPartitions == nil {
					fmt.Println("垃圾回收返回成功但未建立排序文件索引，本轮不推进状态")
					kvs.lastGCFinish = true
					return
				}
				kvs.finishAnotherGC(startTime)
			}
		}()
	}
}
