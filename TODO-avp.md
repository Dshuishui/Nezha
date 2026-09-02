# TKDE 扩展版 · 待办

> 记录当前未完成的事项与已知边界。跑完一项就勾掉，并把结论写进对应条目。

## 正在跑（compact 后从这里接上）

- **AVP 主实验** `/root/autodl-tmp/avpmain.log`，36 次运行约 3~4 小时
  完成标志 `AVPMAIN_DONE`。脚本 `/root/avp-main.sh`。
  2×2（AVP 开关 × GC 开关）× value 64/256/1024B × 3 轮。
  **三条判据必须同时成立**：
  1. 64B/256B 两档 AVP 组 GET 延迟更低且 `avp命中率 > 0`
  2. `nogc_avp vs nogc` 与 `gc_avp vs gc` 同向（GC 前后都成立）
  3. **1024B 阴性对照命中率必须为 0 且无显著差异**——它超过内联阈值 512B，
     若那档也"变快"，说明测的是别的东西（缓存状态/运行顺序/机器波动），
     前两档的正收益不可信，整组作废

- 三方正确性验证已通过（baseline/nezha/nezha_avp 全部 VERIFY_OK）。

## 🖥 两台实验机器

| | AutoDL | winbox-wsl（自有） |
|---|---|---|
| 连接 | `ssh -p 57200 root@connect.bjb1.seetacloud.com` | `ssh winbox-wsl` |
| 规格 | 377GB / 32核 / 50GB | 24GB / 12核 / **953GB** |
| 磁盘 | **共享存储** —— 写入绝对值不可写进论文 | **独占 ext4** |
| 环境 | `/root/env.sh`，RocksDB 8.x，代码在 `/root/autodl-tmp/work/Nezha` | `~/env.sh`，RocksDB **8.9.1-2**(apt)，Go **1.24.4**(apt golang-1.24-go)，代码在 `~/work/Nezha` |
| 长任务 | `setsid nohup ... < /dev/null &` | **`~/bin/winjob run <名字> '<命令>'`**（Mac 侧命令，README 要求 >30s 任务必须用） |

**winbox-wsl 的出站网络几乎全被 reset**：go.dev、阿里、中科大、goproxy.cn、
github.com、gitee.com 全不通，**只有 apt 源可达**。因此：
- Go 用 apt 装（`golang-1.24-go`），不能下 tarball
- 依赖用本地 Mac 的 `~/go/pkg/mod` **rsync** 到 `~/work/gomod`
- 代码同步只能 rsync（含 `.git`，否则结果里的 commit hash 为空），不能 clone/pull

机器手册：`~/Documents/MyWin10/README.md`（macOS TCC 可能挡住读取，需在
系统设置→隐私与安全性→文件和文件夹里给终端授权）。

**未做但必须做**：新旧机器同配置对照。现有 36 格数据全部产自 AutoDL，
换机器后绝对值必变（尤其写入），不做对照就无法判断差异来自机器还是来自改动。

## 📄 ICDE 论文的实验坐标系（读数据前必看）

论文 PDF 在仓库根目录（已 gitignore）。实验章节的七个对比配置：

    Original      Raft + RocksDB 传统架构        = 我们的 -kvSeparation=false
    PASV [28]     去掉存储引擎 WAL 的 LSM 方案     未接入
    TiKV [31]     企业级，架构类似 Original        未接入
    Dwisckey      WiscKey 的分布式实现            未接入
    LSM-Raft [30] 传输 compacted SSTable         未接入
    Nezha-NoGC    只做 KV 分离的基础版            = 默认配置 + 不触发 GC
    Nezha         完整版，GC 与 Raft 日志耦合      = 默认配置 + 触发 GC

**实验条件与我们现在差好几个数量级**：

    项目      论文                        我们
    集群      3 节点 3 副本，10GbE         单节点无副本
    value    1 KB ~ 256 KB               64 B ~ 1024 B
    数据量    100 GB，GC 阈值 40 GB        50~100 MB
    硬件      Xeon E5-2603v3/64GB/2TB SSD AutoDL 共享存储

关键数字备核：Nezha vs Original PUT **+460.2%**（Nezha-NoGC +464.7%）、
PASV vs Original +26.5%、Nezha vs Dwisckey scan **+208.9%**。

**已定方向（B 路线）**：论文主战场是 1KB~256KB 大 value，AVP 优化的是 <512B
小值——论文没覆盖这个区间，正是 TKDE 扩展的新地。先在小 value 坐标系证明
AVP 有效（Nezha-NoGC vs Nezha × AVP 开关），外部系统随后按用户安排接入。
代价：我们的数字接不到论文主表上，需另补一张论文坐标系的对照表说明大 value
下没有回退。

## ✅ 已修：标记字节的连带损伤（三处，全部静默失败）

加 `TagOffset` 标记字节时只改了写入端和 `Get_opt`，读取端散落各处的手写解析
全部漏改——按 `[0:8]` 取偏移，把标记字节当成最低位，算出"真实偏移左移 8 位
再截断"：看着像合法偏移，seek 过去落在文件任意位置。

    GC_opt.go:125          第一轮 GC 主循环   → 读到 EOF，GC 静默失败
    AnotherGC_opt.go:175   第二轮 GC
    AnotherGC_opt.go:329   第二轮 GC
    FlexSync.go            SCAN 的 ReadValueFromNewFile → SCAN 静默返回空
    persister.go           parseValueInt64 要求恰好 8 字节 → 必然失败

更危险的是 GC 失败后的处置：错误只打印不返回，紧接着 `lastGCFinish = true`、
`lastSortedFileIndex = nil`、**`os.Remove(kvs.oldLog)`**——数据没搬完就删源文件，
且 nil 索引让第二轮 GC 空指针崩溃。实测确认过崩溃栈。

治本：解码收敛到 `raft.DecodeOffsetRecord` 单一入口；`ReadValueFromNewFile`
改名 `ReadValueFromOffset` 并改收 `int64`，让"忘记剥标记"无法从调用点溜进来；
GC 失败即 continue，不推进状态也不删文件。

验证：20000 条 × 64B，强制触发两轮 GC —— SCAN 1000/1000 正确，节点存活，
两轮 GC 均建立索引并完成。修复前同一配置写到 13613 条即崩溃、SCAN 返回 0 条。

**影响范围（已核实，不必作废任何历史数据）**：标记字节引入于 `62b6068`
(09-01 21:10)，而 `2d39bea`(08-31)、`467763d`(08-31)、`51599d6`(09-01 13:16)
三个实验 commit 全部早于它。其后跑的三组（三方对照、超时验证、group commit
重测）只测 PUT/GET 且 `gcThresholdGB=4000` 不触发 GC，均未受影响。

**但因此留下一个空白**：AVP placement 自引入起从未在 GC 场景下被正确测过。
GET −1.7% 那个结论来自不触发 GC 的配置，走 sortedFile 的表现仍是未知。

## ⚠️ 已知未修：PadKey 会让不同的 key 撞到同一存储位置

**用户已确认暂不处理**（2026-09-02）：论文实验碰不到，先做好测试。
五个用例已钉住行为（`raft/persister_key_test.go`），真正修好时它们会失败提醒。

`KeyLength = 10`。三类 key 会被静默损坏——不是显示上的 key 变形，而是先写入的
value 被后写入的**静默覆盖**：

    "007" vs "7"                    补齐后同为 "0000000007"
    "user_00001_profile" vs
    "user_00001_settings"           超过 10 字符被截断，同为 "user_00001"
    "0000001234"                    长度恰为 10 且多前导零，被误判为"已补齐"
                                    而原样返回，跳过补齐逻辑

已修的只有 `UnpadKey` 的全零边界（key `"0"` 此前会还原成空串，从 SCAN 结果和
sortedFileCache 里彻底消失）。

**论文实验不受影响**：key 由 `strconv.Itoa(i)` 生成，既无前导零也不超过 10 字符，
三种情况一个都不触发。**但把 Nezha 当通用 KV 对外提供服务时会丢数据。**

根治要换填充方案（如定长排序前缀 + 原始 key，既保留 SCAN 需要的有序性又不丢
信息），会改动存储格式并波及 SCAN、GC、sortedFileCache 所有读路径，且已有数据
需要重写。

## ✅ 已定案：PUT 吞吐指标不可用于性能对比 + 根因已修

### 现象

同配置只改 `-commitTimeoutS`，20 万请求 × 50 并发：

    超时 60s: 吞吐 0.0739 / 0.1187 / 0.1176   max 60003ms
    超时  5s: 吞吐 0.2207 / 0.2195 / 0.2200   max  5003ms
    p50/p99/p999 两档之间几乎不变（12.0 / 22.3 / 23.5）

三条证据锁死：`max` 紧贴超时值；吞吐方差从 **60% 塌到 0.5%**；而 p50/p99/p999
从头到尾没动过——**系统真实性能不变，吞吐数字却能差 60%**。

另有一条：吞吐绝对值 0.117 → 0.220，**翻倍**。60s 档的 PUT 吞吐不只是噪声大，
是被超时系统性压低了一半。

早先「按失败数干净分簇」的说法**不成立**（60s 档轮次 1 失败 19 反而吞吐最低）。
决定吞吐的不是超时请求总数，而是有几个**串在同一个 goroutine 上**——50 个
goroutine，谁连吃两次超时谁就把墙钟拖长。那个分簇是九轮小样本的巧合。

### 根因（已修，commit `31704f0`）

`StartPut` 里 `raft.Start` 先分配 index 并写日志，**之后**才把 opCtx 注册进
`reqMap`。这中间 applyLoop 可能已经处理完这条 index——它查 reqMap 查不到，
就不会 `close(opCtx.committed)`，客户端于是在一个永不关闭的通道上等到超时。

写入本身不受影响：applyLoop 的存储分支与 `existOp` 无关，照常执行。所以
**数据在，丢的只是确认**。p999=23ms 到 max=60000ms 之间空无一物，正是这个
特征——不是尾延迟衰减，是请求根本没被唤醒。

修法：`lastAppliedIndex` 与 reqMap 注册同在 `kvs.mu` 下，天然串行，拿来当同步点。
注册时若发现 `op.Index <= lastAppliedIndex`，说明 apply 已经过去了，直接返回，
不进 select。同时 `[APPLY-RACE] early_apply_rescued` 计数，修复与证据是同一段代码。

### 后果：需要重测的结论

- 「baseline 写入快 53%」——**判定虚假**，撤下
- 「group commit +140%」——**已重测，真实值 +5.5%**（3 轮，方差 0.7%）
  正确的叙事是「group commit 让 fsync 几乎免费」：fsync 使 p50 从 12.30 →
  14.14ms（+15%），开启攒批后回到 12.08ms。50μs 窗口已足够，200μs 只是把
  批大小从 8.6 撑到 14.7、性能不变。早先记录的 avg_batch=35.55 是超时污染
  下请求堆积的产物。
  附带观察：fsync 那档 `early_apply_rescued` 是 0~2，其余档 28~75——
  raft.Start 变慢反而让 apply 竞态几乎不发生，命中率与其耗时成反比。
- 「句柄复用无效果」——当时也看吞吐，可能被噪声淹没，需重新判定
- GET 那组**不受影响**（无 commit 等待、无超时路径），三方对照 −7.6% → −1.7% 站得住

论文口径：报延迟分布，不报平均值+吞吐。前者掩盖尾部，后者被尾部主导。

## 待验证

- [x] **goodPut 缺口是丢数据还是丢确认** → **丢确认**
  与 60 秒超时是同一个 bug（见上）。applyLoop 的存储分支与 `existOp` 无关，
  数据照常落盘，丢的只是唤醒客户端那一步。`goodPut` 名不副实：它统计的是
  "超时内收到确认的请求"，不是"成功写入的请求"。
  实证待补：`bash scripts/verify-goodput.sh 200000 64`（停机后数 RocksDB key 数）。

## 待清理

- [x] **Dependabot 告警已全部 dismiss**（5 条，2026-09-02）
  实为两个包共 5 条 CVE，均已核实不适用，理由记在每条告警上（`not_used`）：
  - `golang.org/x/net`（2 条 medium）：advisory 要求升到 0.36.0，`go.mod` 里
    **实际已是 v0.49.0**。工作分支与 default branch(`multiGC`) 均已核实。
  - `gopkg.in/yaml.v2`（1 high + 2 medium）：`go.sum` 里**只有 `/go.mod` 那一行、
    没有 `h1:` 源码哈希**——构建时从未下载过它的代码，编译产物里不含这个包。
    Dependabot 扫的是 go.sum 文本，不区分"真的编译进去了"和"只在版本图里出现过"。

- [ ] **实验全部跑完后执行 `go mod tidy`**
  清掉 go.sum 里的陈旧条目，让告警不再复现。**必须等新旧机器对照实验做完**——
  该对照要求两台机器跑完全相同的代码，依赖一变对照就失去意义。
  另注意 winbox-wsl 出站网络几乎全被 reset（见下），tidy 若需下载会当场失败，
  应在本地 Mac 上执行后再 rsync 过去。

## 待建设

- [ ] **前端展示**
  建议**不要自研通用监控**：已有的 `[AVP-STATS]`/`[PUT-BREAKDOWN]`/`[RAFT-WRITE]`
  都是 atomic 计数器，加一个 Prometheus 格式的 HTTP endpoint（约 50 行）即可接
  Grafana，TiKV/etcd/ClickHouse 走的都是这条路，工作量比自研 React 前端小一个数量级。
  **值得自研的只有一处**：AVP 的 value placement 可视化——哪些 value 走内联、
  哪些走 valuelog、命中率如何随负载变化。Grafana 画不出这个，而它恰是论文 demo
  最该展示的东西。

- [ ] **更现代的测试工作流**（YCSB 只是起点）
  YCSB 是 2010 年的合成负载（均匀/Zipf + 固定 value 大小）。更值得做的：
  - `db_bench --benchmarks=mixgraph`：RocksDB 官方，基于 Facebook 生产负载建模，
    key/value 大小分布与访问局部性取自真实系统
  - **真实 trace replay**：Twitter 开源了 54 个生产缓存集群的完整 trace
  - **待核实**：若这些 trace 确实以小 value 为主，就直接证明了"小 value 是现实中的
    主流场景"，AVP 优化的不是假想问题。这比任何合成负载都有说服力，但必须核实，
    不能凭印象写进论文。

## 🎛 被测系统的选择（`-system`）

论文比较的六个配置现在各由一个参数选中，节点启动时打印生效配置，
结果文件因此能追溯到系统，不必回查当时的脚本：

    -system=original     value 完整写 RocksDB；value 落盘 3 次
                         （Raft 日志、存储引擎 WAL、SSTable）
    -system=pasv         original 去掉存储引擎的 WAL
    -system=dwisckey     KV 分离，但每条 value 在 Raft 日志之外再落一次盘；
                         读路径与 nezha-nogc 完全相同；不做 GC
    -system=lsm-raft     差异全在 follower 侧，单节点等价 original（启动时提示）
    -system=nezha-nogc   Raft 日志兼任 valuelog，value 只落盘 1 次；不跑 GC
    -system=nezha        在此之上加与 Raft 日志耦合的 GC

AVP 正交叠加：`-system=nezha -inlinePlacement`。

**必须开 `-syncWAL`**：不 fsync 时两次写入都只是 memcpy 到 page cache，
"Nezha 比 Dwisckey 少一次持久化"这个核心论点根本测不出来。
实测 20000 条 × 64B：dwisckey 20.4ms → 37.4ms（+83%），差异全部来自那次多余的 fsync。

**此前的隐患**：GC 跑不跑是靠把 gcThresholdGB 设成 4000（高到永不触发）来控制的，
阈值按数据量算错就会静默变成另一个被测系统，而结果里看不出任何异常。
现已分离为独立的 gcEnabled 字段。

### 尚未实现

- **TiKV**：外部系统，需单独部署。测试时记录副本数、`sync-log`、RocksDB 参数，
  否则与我们的数字不可比。
- **lsm-raft 的真实实现**：现在只是占位（等价 original + 提示）。它要传输
  compacted SSTable 而非细粒度日志条目、并在 follower 侧做 KV 分离，
  必须多节点才有意义。

### 待查

- **PASV 与 original 几乎无差异**（0.0615 vs 0.0623 MB/S）。论文实测 PASV 比
  Original 快 26.5%，我们测不出来。可能是写入瓶颈不在 RocksDB——实测 PUT 的
  98% 时间花在等 apply，RocksDB 写入只占 0.03ms，关掉 WAL 省下的那点被淹没了。
  需要确认 DisableWAL 是否真的生效（比如对比 RocksDB 目录里有无 .log 文件）。

## 待补实验

### 优先级 1：论文主表（进行中）

- [ ] **三方 × 三项 × 三档 value 的完整对比**（脚本 `/root/maintable.sh` 已就绪）
  - 模式：`baseline`(-kvSeparation=false，即 standard Raft+RocksDB) / `nezha` /
    `nezha_avp`(-inlinePlacement)
  - 负载：PUT / GET / SCAN；value：64B / 256B / 1024B；3 轮取中位数
  - **触发 GC**（阈值=写入量/3），走 sortedFile 路径——那才是论文声称
    SCAN +72.6% 的场景，不触发 GC 测的是另一条代码路径
  - 数据量按 value 反比取（50万/30万/10万），让三档写入总量都在 50~100MB，
    否则大 value 那档耗时长得多且 GC 行为不可比
  - 报 p50/p99 而非只报吞吐（PUT 吞吐曾被 apply 竞态污染，见上文）
  - 目的：复现并更新 README 里的 PUT +460.2% / GET +12.5% / SCAN +72.6%

### 优先级 2：外部系统对比（TKDE 审稿人大概率会问）

- [ ] **与真实开源系统对比，而不只是自己关掉一个开关**
  当前"baseline"是同一套代码 `-kvSeparation=false`，优点是对照干净（只差被测
  变量），缺点是审稿人会问"跟真正的产品比如何"。ICDE 版可以只做前者，TKDE
  扩展版通常要求后者。
  候选（按契合度与工作量排序）：
  - **BadgerDB**：同为 Go、同为 WiscKey 路线的 KV 分离，是 Nezha 最直接的对手，
    接入成本最低（同语言、嵌入式）。**首选**。
  - **RocksDB / Titan**：Titan 是 TiKV 的 KV 分离插件，工业界对标物。
    `benchmark/testRocksDB*` 已有 grocksdb 的用法可复用。
  - **etcd**：同为 Raft + 强一致 KV，但它不做 KV 分离且面向小数据，
    对比点在"Raft 集成方式"而非"存储布局"。
  - **TiKV**：最完整的对标，但部署与调参成本最高，性价比最低。
  注意公平性：这些系统多为单机或不同的一致性配置，必须写清对比条件
  （副本数、fsync 设置、是否走共识），否则数字没有可比性。

### 优先级 3：其余

- [ ] **更高的 inlineThreshold**：256B 时 AVP 收益仍在上升（吞吐 +29.6%），
  说明 512B 定低了。扫 1KB / 2KB / 4KB，找收益开始下降的拐点。
- [ ] **AVP placement + GC 的组合从未被正确测过**（见上文标记字节那节）
  GET −1.7% 的结论来自不触发 GC 的配置。主表实验会顺带覆盖这一项。
- [ ] **内存受限对照**：AVP 的主场是"数据集装不下内存"。当前 64GB 机器上
  128MB 数据全在 page cache，测出的是收益下限。用 cgroup 压到 2~4GB 再测。
- [ ] **重复测量**：历史数据多为单点。GET 噪声较小（100 轮 × 20000 请求），
  但 SCAN 噪声实测可达 20%，个位数差异不可用。关键结论需 3 次取中位数。
- [ ] **接入 go-ycsb**（用户已确认：PUT/GET/SCAN 三项测完后做，并与自制 benchmark 对比效果）
  论文 Fig.8 本身就是 YCSB 结果，扩展版要更新那张图绕不开。论文 Table II 的配置：

      Load  Insert           Insert Only
      A     Update  Point    50%write 50%read
      B     Update  Point     5%write 95%read
      C       /     Point    Read Only
      D     Insert  Point     5%write 95%read
      E     Insert  Range     5%write 95%scan
      F     RMW     Point    50%write 50%read

  100GB 预载、每 workload 100 万请求、value **16KB**。
  现有 `ycsb/` 只有 A/D/E/F（**缺 B/C**）且为自制。

  **今天暴露的 benchmark 问题里，YCSB 能挡掉四个**：keyspace 写死 1 亿（读打在
  不存在的 key 上）、gapkey 写死 400 万（每轮扫全库）、zipf_read 没有 -vsize
  却被传入（秒退产生空列）、各 benchmark 输出格式与单位不统一。共性都是"自制
  工具的隐含约定散落各处、无人统一"。

  **但 YCSB 挡不住正确性问题**：今天最严重的三个 bug（apply 竞态、标记字节漏剥、
  GC 失败删源文件）全是 `benchmark/scanverify`（逐条校验值）抓出来的，YCSB 默认
  不校验返回值。两者不可互相替代——YCSB 管口径，scanverify 管有没有意义。
  **每组实验前先跑 scanverify 作正确性闸门**，这条要固化进流程。

  做法：实现 `Read/Insert/Update/Scan/Delete` 五个接口（约 150 行）接 go-ycsb，
  与自制 benchmark 跑同一配置对比，确认两者口径一致后再用 YCSB 出新主表。
  **保留原 benchmark**用于与 ICDE 版对照。
- [ ] **句柄复用重新判定**：当时判"无效果"是基于 PUT 吞吐，而该指标已证实被
  超时污染。需用 p50/p99 重测。

## 方法论教训

- **性能实验必须独占机器。** 为了"不让机器闲着"，把并发扫描和主队列的阶段分解
  同时跑在一台机器上，两者共享 CPU 与磁盘。成功率类数据不受影响（一条写入成不成功
  与隔壁跑什么无关），但**延迟与吞吐全部偏高，不能作为基线**。省下的那点时间不值得
  赔上数据可信度——受影响的两组需在机器空闲时重测。
- **扫描实验要一次只变一个维度。** 首版"规模扫描"把并发锁死在 50、只变总请求数，
  于是 100 请求那档每个客户端只发 2 次、压力持续不到一秒，它 100% 成功说明不了
  任何问题。总量与持续高压时长被绑在一起变，结论无法归因。

## 已知边界（读数据时必须记得）

- **SCAN 测量噪声可达 20%**：由阴性对照量出（AVP 不碰 SCAN 路径，代码已确认），
  故块尺寸实验里那些个位数的 SCAN 差异不能当结论。
- **AVP 只覆盖 sortedFile 一条读路径**：GC 后数据分散在多个 sortedFile 与新旧
  valuelog，读是并发多路查找。走 valuelog 的请求不经过内联缓存，
  **命中率天生够不到 100%**，上限是走 sortedFile 那条路径的请求占比。
  这既解释了实测命中率，也指出了 AVP 下一步能往哪扩。
- **AutoDL 的磁盘是共享存储**：写入性能绝对值不可写进论文（32 核机器上
  200 万条 PUT 反而比 2 核腾讯云慢 30%）。内存与读性能对比不受影响。
- **页大小 4096，与腾讯云一致**：RSS 数字可跨这两台机器比较。

## 下一步（待定方向）

- [ ] **攒批窗口寻优**：已测 0 / 200 / 1000μs，批大小 1.00 / 12.26 / 35.55。
  窗口越大批越大，但 200μs 那档延迟(16.98ms)反而高于 0 和 1000μs(13.59/12.38ms)，
  **不单调，存疑**——可能是单次测量噪声（PUT 实测波动 ±7%）。
  需重复测量，并向上扫 2000 / 5000μs 找拐点。

- [ ] **A. 换独占磁盘的机器重测 fsync**
  AutoDL 是共享存储，实测单次 fsync 仅 0.0711ms，大概率被底层缓冲掩盖。
  独占 NVMe 通常 50~200μs，SATA SSD / 机械盘 0.5~10ms——换机器后持久化占比
  只会更高。当前 13.7% 是**下界**。

- [ ] **B. group commit（"减少持久化次数"的直接延伸）**
  开启 fsync 后 `avg_lock_wait` 从 0.223ms 涨到 **1.606ms（×7.2）**：fsync 在持锁
  状态下进行，一个客户端等磁盘时其余 49 个全堵在 rf.mu 上。攒一批请求共用一次
  fsync，既摊薄磁盘成本又缩短临界区。
  **注意**：无 fsync 时做这个优化毫无意义（本来只有 6μs），这也是之前"句柄复用"
  白做的同一个原因——在开关关闭的配置下优化被关掉的那部分。

## 已完成

- [x] AVP 有效性：命中率 69.23%（对照组严格 0），GET 延迟 −34%、吞吐 +51.8%
- [x] 阈值断崖：512B/1024B 两档 `hits=0` 阴性对照守住，证明前几档收益是真实效应
- [x] `entries_per_scan` 稳定为理论值一半，验证稀疏索引行为模型
- [x] 读的键空间对齐实际写入量（YCSB/db_bench 通行口径）
- [x] 过程数据归档到 `~/nezha-results/<时间戳>_<标签>/`
- [x] **写入路径瓶颈定位**：handler 11.45ms 中，写日志仅 0.006ms（0.05%）、
  raft.Start 共 1.8%、**等 apply 占 98.1%**，residual 0.1% 证明分解无遗漏。
  此前"每条五次系统调用是瓶颈"的假设被证伪——句柄复用优化的那 6μs
  即使降到零也测不出来。
- [x] **小规模写入验证**：100/1000/10000 条**全部 100% 成功**，单并发与 10 并发
  在 10 万条下同样零失败；50 并发失败 12 条、100 并发失败 30 条，但 200 并发
  又回到 0——**不单调，所以"失败率随并发上升"这个说法站不住**，更像偶发尾延迟。
  逻辑 bug 已排除：bug 不会只在特定并发数下出现。

- [x] **group commit**：开启 fsync 后攒批，窗口 1000μs 时平均批大小 **35.55**，
  20 万次写入只做约 5600 次 fsync（**省掉 194374 次**）。
  ~~吞吐 0.1072 → 0.2578 MB/S（+140%）~~ **该数字作废**（超时污染），
  已重测：真实 +5.5%，正确叙事是"group commit 让 fsync 几乎免费"（见上文）。
  以下锁等待数据仍成立——
  因为省下的不只是磁盘时间，**锁等待从 1.6276ms 降到 0.3091ms（−81%）**：
  一个客户端等磁盘时不再堵住其余 49 个。
  这是原始 idea 的第二个层次：架构上"两次持久化→一次"，执行上"每条一次→每批一次"。

- [x] **fsync 缺失是关键前提**：系统从未调用过 fsync，Flush 只把数据交给
  OS page cache（进程崩溃不丢、机器断电全丢）。因此"一次持久化写入"只值 6μs，
  "把两次持久化合成一次"这一架构收益完全测不出来——**被测的开关本身是关着的**。
  加 `-syncWAL` 后实测：单次写日志 0.0060 → **0.0711ms（×11.9）**，
  持久化占端到端 2.0% → **13.7%（×6.9）**，PUT 吞吐 −8.3%。
  **会议版结论不受影响**：460% 归因于避免大 value 进 LSM 的写放大（论文自述），
  与 fsync 无关，且两组配置一致、对照公平。
