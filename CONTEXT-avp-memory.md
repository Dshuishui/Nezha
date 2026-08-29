# CONTEXT — AVP 小值优化 + 内存瓶颈治理

> 会话上下文快照，供新的 Claude 实例无缝接手。最后更新：2026-08-30

---

## 当前目标

Nezha（KVS-Raft 分布式 KV 存储）的 ICDE 2026 论文已录用。`avp-inline-small-value` 分支是面向 **TKDE 期刊扩展版**的新工作，核心是小值（64B/256B）场景的优化。

**本轮的主线**：Nezha 的内存随 **key 数量**（而非数据量）线性增长，100GB 的 64B 小值 = 16 亿 key，光索引就要几百 GB，导致小值场景**根本无法在普通机器上测试**。目标是把内存与数据集解耦。

> 注意：ICDE 论文实验用 1KB–256KB value，AVP 阈值 512B，**论文实验里 AVP 根本不触发**，所以本分支改动不影响已发表的 460.2%/12.5%/72.6% 三个数字，无回退风险。

---

## 仓库与环境

| 项 | 值 |
|---|---|
| 本地路径 | `/Users/cong/Documents/Github/Nezha` |
| 当前分支 | `avp-inline-small-value` |
| 远端 | `github` → github.com/Dshuishui/Nezha（默认分支已设为 `multiGC`）<br>`origin` → gitee.com/dong-shuishui/Nezha |
| baseline | `multiGC` 分支（= ICDE 版本） |
| 云服务器 | ssh 别名 **`tengxunOneYear`**（118.25.192.117，主机名 VM-0-8-ubuntu），项目在 `~/Github/Nezha` |

**重要环境细节：**

- 本地 Mac 的 RocksDB 是 10.2.1，与 `grocksdb v1.8.12`（对应 8.9.1）API 不兼容，**本地无法编译/跑测试**，一切验证都在云服务器上做
- ssh 到服务器需要 `dangerouslyDisableSandbox: true`（沙箱拦出站 SSH）
- `git push` 需要 `-c http.proxy="$HTTPS_PROXY" -c https.proxy="$HTTPS_PROXY"` 覆盖 git 里写死的 `127.0.0.1:7897`
- 服务器有时拉不动 GitHub，可靠做法是本地 `git format-patch` + `scp` + 服务器 `git am`
- 服务器上跑长任务必须 `setsid ... < /dev/null &`，否则 ssh 断开会带走进程
- **不要 `pkill -f xxx.sh`**，会匹配到自己那条 ssh 命令行把自己杀掉

服务器编译环境：
```bash
export PATH=$PATH:/usr/local/go/bin
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L/usr/lib/x86_64-linux-gnu -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu
```

---

## 已完成的四项改动

分支 HEAD：`e390bd3`（已推 GitHub + Gitee）

### 1. `rf.log` 物理压缩 — commit `3122463`

**问题**：`rf.log []*raftrpc.LogEntry` 从不截断。实测每条 **216 B**（置 NULL 后的地板价，protobuf 三层结构 `[]*LogEntry` 槽位 + `LogEntry` + `DetailCod` 各自独立分配），带 64B value 时 280 B。100GB/64B → **337 GB**。

原有的 `memoryControlLoop` 只把已应用条目的 `Value` 置为 `"NULL"`：16KB value 省 99%，但 **64B value 只省 23%**——小值场景 value 根本不是大头。而且它的 goroutine 启动一直是注释掉的，**从没运行过**。

**改法**：加 `lastIncludedIndex`/`lastIncludedTerm` 快照基址，`index2LogPos` 从 `index-1` 改为 `index-lastIncludedIndex-1`，新增 `compactLog` 定期物理截断。

**关键点**：必须 `make`+`copy`；`rf.log = rf.log[pos:]` 只移动切片头指针，底层数组仍被引用，一个字节都不释放。

压缩点取 `min(lastApplied - 5000, min(matchIndex))`，保证不删掉任何 follower 未复制的条目。8 处 `index2LogPos` 调用点都加了压缩区间保护（新增 `termAt()`，落在压缩区返回 -1 而非负下标 panic）。

**已验证（内存曲线，64B value，单节点）：**

| 写入量 | 改前 `2bf2e15` 峰值 RSS | 改后峰值 RSS |
|---|---|---|
| 25 万 | 226 MB | 118 MB |
| 50 万 | 372 MB | 124 MB |
| 100 万 | 667 MB | 146 MB |
| 200 万 | **1165 MB** | **167 MB** |

改前斜率约 480 B/条（线性，且结束 RSS ≈ 峰值，完全不回落）；改后约 28 B/条，**降 17 倍**。延迟 15.1–16.1 ms、吞吐 0.11–0.16 MB/s 两组无系统性差异，**零性能代价**。

> 曾经怀疑 `rf.Offsets` 有同样问题——**是错的**。实测 1000 万次 append + `s = s[1:]` 后堆仅 0.05 MB，Go 的 `append` 扩容时只拷贝存活部分，自愈。不需要修。

### 2. `InlineValues` → 字节预算有界 LRU — commit `3e75249`

**问题**：无界 map 持有 GC 时遇到的所有小值，100GB/64B 需约 **190 GB**。

**隐藏更深的问题**：原实现小值内联时**不往 `Entries` 写 offset**（`GC_opt.go` 的 else 分支），所以 `InlineValues` 不是缓存而是小值的**唯一索引**——直接改有界会丢数据。

**改法**：
1. offset 对所有 key 都记录，sortedFile 成为唯一权威来源，`InlineValues` 降级为纯加速层
2. 无界 map → `InlineCache`（字节预算 LRU，`-inlineCacheMB` 默认 256）
3. GET 未命中读盘后**回填**小值，Zipf 热点自然升温
4. **SCAN 删掉全表遍历**——原来是 O(缓存条目数)、与查询范围无关，小值场景下会让窄范围 scan 退化；改为直接走 sortedFile 顺序读
5. 顺带把硬编码的 GC 阈值 4000GB 提成 `-gcThresholdGB`（默认不变），否则 AVP 内联路径在任何实测里都跑不到

### 3. `Entries` 稠密 map → 稀疏块索引 — commit `34ef625`

**问题**：`map[string]int64` 每 key 约 55 B，100GB/64B（10 亿 key）→ **58 GB**。

**依据**：sortedFile 由 GC 用 RocksDB 迭代器 `SeekToFirst()/Next()` 顺序写，key 定长 padding，**字典序即键序，文件本身可二分**。

**改法**：新增 `kvstore/FlexSync/sparseindex.go`，每约 64KB（`-indexBlockKB`）记一个块起点。点查二分定位块 + 块内顺序扫描；范围查询起点也从「key 逐个 +1 线性试探」改成二分（原做法随键空间稀疏程度恶化）。`CreateSortedFileIndex` 改为重建稀疏索引，不重建内联缓存（纯加速层，冷启动为空即可）。

单测实测：**10 万 key → 144 个索引项，压缩 694 倍**。

### 4. 删除 `Offsets` 哨兵 — commit `aeac5de` ⚠️ 这是修我自己引入的 bug

**现象**：GC 后 GET 崩溃，`panic: 去新的rocksdb中拿取key对应的index有问题: key mismatch in new file`。

**根因**：`raft/raft.go` 的 `Make()` 里有

```go
rf.Offsets = append(rf.Offsets, 0) // 初始化时添加一个0，使得后续对index的访问和raft的对其，从1开始
```

这个**哨兵 0** 的设计意图是让 index=1 的 TermLog 消费掉它，使后续 Put 偏移量对齐。原始代码里 TermLog 确实会 pop 队列，一切正常。

**上一个 session 我修 Bug 2 时让 TermLog 不再 pop**（判断"TermLog 没有 valuelog 条目，不该占 Offsets 槽"——这半句是对的），但没注意到它同时承担着消费哨兵的职责。哨兵于是永久卡在队首：

```
初始              Offsets=[哨兵0]
index=1 TermLog   不 pop         → Offsets=[哨兵0]
key"1" 写盘       offset=0       → Offsets=[哨兵0, 0]
index=2 key"1"    取哨兵 0        ✅ 巧合正确（哨兵值恰好是 0）
index=3 key"4"    取 0（key"1"的）❌ 真身在 94 → 此后永久偏一格
```

**取证证据**：
- `newKeyIndex_1_2` 里 **65320/65321 个 key（100.0%）偏 -94**（恰好一条 entry：20B 头 + 10B key + 64B value），唯一例外持有一个上一代日志的陈旧大偏移
- 仪器输出 `[OFF-APPEND #1] ... 追加后队列长度=2` —— **第一次追加时队列里就已经有 1 个元素**
- `len(Offsets)` 恒定比 `lastIndex - lastApplied` 多 1，两次换文件时都成立

**为什么以前 40GB/100GB 大值测试没暴露**：GC 前读走别的路径；GC 后才用 RocksDB offset → currentLog 直读。大 value 时 GC 后残留在 currentLog 的 key 很少，Zipf 热点大多落在 sortedFile（那条路径是对的），且 `anotherGCGet` 有 sortedFile 兜底。小值 + 低阈值下两轮 GC 后仍有 6.5 万 key 在 currentLog，每次读几乎必然撞上。

**修法**：删哨兵，保留 TermLog 不 pop。哨兵只能抵消**一个** TermLog，而每次重新选主都会产生新的 TermLog——第二次选主后照样会吃掉真实偏移量再次腐坏。现在的不变式：`Offsets[0]` 恒对应日志下标 `shotOffset+1`。

---

## 内存账（100GB / 64B value，约 10 亿 key）

| 结构 | 改前 | 改后 |
|---|---|---|
| `rf.log` | 337 GB | ~1.4 MB（5000 条窗口） |
| `Entries` → `Sparse` | 58 GB | ~64 MB（64KB 块） |
| `InlineValues` | 190 GB | 固定预算（默认 256 MB） |

**几百 GB → 几百 MB。** 2GB 空闲内存的服务器，磁盘够的话现在应该能跑 100GB 小值测试。

---

## 验证状态

**单测 16/16 全过**（服务器上跑）：

```bash
go test ./raft/ -v            # 7 个：compact_test.go 4 + offsets_test.go 3
go test ./kvstore/FlexSync/ -v # 9 个：inlinecache_test.go 4 + sparseindex_test.go 5
```

**e2e 对照实验通过**（`scripts/test-inline-cache-e2e.sh`，GC关闭 vs GC开启）：

```
对照组 A（GC 关闭，读走 valuelog）   GoodPut = 13879 / 20000
实验组 B（GC 开启，读走稀疏索引）     GoodPut = 13829 / 20000
差异 50，容差 277（2%）             ✅ 通过
```

实验组是在**内联缓存仅 1MB、GC 跑了 3 轮**的严苛条件下——绝大多数读必然缓存未命中、必须退回 sortedFile 二分查找。

> 注：`zipf_read` 从 **1 亿**的键空间采样而只写了 20 万 key，理论命中率 `ln(200000)/ln(1e8) ≈ 66%`，所以 69% 是正常的。**绝对命中率不能作为断言**，必须用对照实验。

---

## 进行中的实验

**AVP 分支 vs multiGC baseline 完整对比**，服务器上跑，脚本 `/tmp/cmp.sh`，日志 `/tmp/cmp2.log`，结果 `/tmp/avpcmp_{after,before}.csv`。

配置：50 万条 × 64B，内联缓存 64MB，索引块 64KB，GC 阈值按写入量自动算（0.0146 GB）确保必然触发。

**已出的实验组数据**：

```
PUT : elapse 12m56s, throughput 0.0412 MB/S, avg latency 65.08 ms, goodPut 499843
GC  : 已完成，读路径走 sortedFile（稀疏索引生效）
GET : Throughput 0.2821 MB/S, GoodPut 14810/20000, Average Latency 4.50 ms
SCAN: 进行中
```

⚠️ **PUT 数字不可用于前后对比**：GC 阈值压到 0.0146 GB 后 GC 在写入过程中反复触发、跟写入抢 I/O，导致 PUT 从 3m12s 变成 12m56s。这是测试配置所致，不是代码回归。GET/SCAN 有意义（两组受同样影响，可比）。

**检查进度**：
```bash
ssh tengxunOneYear 'pgrep -c cmp.sh; tail -20 /tmp/cmp2.log; cat /tmp/avpcmp_*.csv'
```

---

## 待办

1. **等对比实验跑完**，拿到 GET/SCAN 的前后对比 —— 这是 TKDE 必须要有的数字：**稀疏索引每次点查多读一个 64KB 块的实际代价**。目前只有理论估计（一次 seek + 一次顺序读），审稿人一定会问用多少读性能换了多少内存。
2. 若要在 2GB 机器上跑 100GB 小值，需实测确认（目前只有外推）。
3. TKDE 论文实验：每项改进都要与原版 Nezha 做对照，跑改前/改后两轮同参数 benchmark。建议做「内存 vs 写入量」曲线图（改前线性、改后平线），比单点数字有说服力。

---

## 新增的脚本

| 脚本 | 用途 |
|---|---|
| `scripts/bench-memory-curve.sh` | 内存随写入量曲线，验证内存与数据集解耦 |
| `scripts/test-raftlog-memory.sh` | 单点 RSS 对比（峰值 vs 压缩后） |
| `scripts/test-inline-cache-e2e.sh` | GC关闭 vs GC开启 对照，验证 GC 后读路径不丢数据 |
| `scripts/bench-avp-compare.sh` | 完整对比：内存 + PUT/GET/SCAN，自动探测版本支持的 flag |

**踩过的坑**（写脚本时注意）：
- 三个 benchmark 输出格式互不相同：randwrite `throughput:0.16MB/S`、zipf_read `Throughput: 0.33 MB/S`、scan_pro `throught:0.44MB/S`（原代码拼错）。解析必须忽略大小写、容忍空格、同时匹配 `throughput|throught`
- GC 阈值必须按写入量推算，定死的值容易差一点触发不了；**GC 没触发要直接判失败**，否则产出的是无意义数据
- 每轮测试要用 `mktemp -d` 新数据目录；GC 产生的文件（`RaftState_sorted_*`、`newRaftState_*`、`newKeyIndex_*`）都在 dataDir 内

---

## Git 提交记录（本轮）

```
e390bd3 test: fix benchmark output parsing and auto-size the GC threshold
6e1ca53 test: turn the e2e into a GC-off vs GC-on controlled comparison
aeac5de fix(raft): drop the Offsets sentinel that desynchronised every offset   ← 根因修复
ec3f991 test: add full before/after comparison benchmark (memory + PUT/GET/SCAN)
a4a3d86 test(index): unit tests for the sparse block index
34ef625 perf(index): replace dense key->offset map with a sparse block index
4050405 test(avp): add end-to-end test for bounded inline cache
d04a4a0 test(avp): add InlineCache unit tests and a configurable GC threshold
3e75249 perf(avp): bound the inline small-value cache by a memory budget
9637cd2 test: add memory-vs-write-volume curve benchmark
3122463 perf(raft): physically compact rf.log instead of nulling values
```
