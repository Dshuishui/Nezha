# TKDE 扩展版 · 待办

> 记录当前未完成的事项与已知边界。跑完一项就勾掉，并把结论写进对应条目。

## 正在跑（compact 后从这里接上）

- 无。240/241 上没有残留进程。
  节点脚本留在两台机器上可复用：`~/rep-node.sh`（两节点稳态）、`~/three-race-node.sh`
  （三节点，`BIN=race|normal`，`start|kill9|stop|report`）。
- **下一步：崩溃恢复**（`docs/crash-recovery.md`，等用户确认设计）。
- 之后：三台物理机 / 六系统多节点对比 / 读路径 leader 检查（见各节）。

## 🖥 实验机器（2026-09-04 更新）

| | tikv240 | tikv241 | winbox-wsl | AutoDL |
|---|---|---|---|---|
| 规格 | 96核/251G | 96核/251G | 12核/24G | 32核/377G |
| 磁盘 | 761G 独占 | 独占 | 953G 独占 ext4 | 50G **共享** |
| 用户 | `Zg.xin` | **`zx`**（不同！）| — | root |
| 定位 | **主力** | **主力（配对做多节点）** | 备用快验 | **弃用** |

- 手册：`~/Documents/MyWin10/machines/tikv24{0,1}.md`
- 环境：Go 1.24.4 + RocksDB 8.9.1（源码编译到 `~/local`，无 sudo），`source ~/env.sh`
- 代码在 `~/work/Nezha`，用 `rsync -az` 从 Mac 同步
- **四级跳，强依赖 winbox 在线**：Mac → winbox → WSL → 跳板机 → 240/241。
  连不上时先查 `ssh winbox-wsl`
- `winjob` 不支持多级跳，240/241 上的长任务用 `nohup` 或 `tmux`（2.7）
- 两节点选举实测：**241 恒为 leader，240 为 follower**（peers 顺序决定）
- AutoDL 弃用原因：共享存储，写入绝对值不可信
- **跨机器只比相对收益**：winbox 12 核在 100 并发下调度开销显著；
  tikv240 单并发反比 winbox 慢 40%（磁盘 fsync 更慢），96 核的价值只在高并发

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

## 🔍 写路径的两个瓶颈（已定位，2026-09-03）

`nezha-nogc`，64B value，开 `-syncWAL`，实测：

    并发  平均延迟   p50     raft_start  commit_wait  rocksdb  吞吐
    1     15.81ms   11.16    1.05       14.48       0.038   0.0040
    10    15.27ms   10.96    4.84       10.04       0.010   0.0419
    50    43.46ms   42.91   19.29       23.71       0.009   0.0731
    100   86.13ms   85.58   19.51       66.17       0.010   0.0740
    200  172.85ms  172.86   20.12      152.28       0.011   0.0739

### 瓶颈一：低并发下的固定 15ms —— applyLogLoop 的轮询

并发 1 与 10 的延迟几乎相同（15.81 / 15.27ms），是与负载无关的固定延迟。
并发 1 时 `raft_start` 仅 1.05ms 而 `commit_wait` 达 14.48ms——来自
`applyLogLoop` 空闲时的 `time.Sleep(10 * time.Millisecond)`：日志写完后
要等轮询醒来才被 apply。

### 瓶颈二：高并发下的吞吐上限 —— 每条一次 fsync

吞吐自并发 50 起饱和在 0.074 MB/S（三档 0.0731/0.0740/0.0739），
延迟严格线性增长（43→86→173，正好翻倍），是排队论的典型形态。

    0.074 MB/S ÷ 64B ≈ 1212 条/秒 → 每条 0.825ms

而 RocksDB 写入只要 **0.01ms**，差 80 倍。那 0.825ms 是 fsync
（独占 ext4 上 0.5~1ms 属正常）。

### 后果：所有系统的 PUT 差异都被这两个瓶颈掩盖

这解释了为什么 original / pasv / nezha-nogc / nezha / AVP 的 PUT 全都挤在
12~20ms、差异落在噪声内：各系统在写入上的真实差别（少写几次 value、关掉
存储引擎 WAL）只有 0.01~0.03ms 量级，相对 0.825ms 的 fsync 和 10ms 的轮询
完全不可见。**dwisckey 是唯一测出差异的，因为它多的正是一次 fsync**
（20.4ms → 37.4ms），代价大到能穿透这层掩盖。

**推论：不修掉这两个瓶颈，论文主表里 PUT 那一列测不出任何系统间差异。**

### 对四个优化方案的判定

- **批量 apply —— 降级**。RocksDB 只占 0.01ms（0.01%），批量能省的极少。
- **早确认（early ack）—— 首选**。客户端在 Raft 日志 fsync 后即返回，不等 apply，
  可消除 `commit_wait`。**只有 Nezha 能做**：它的 Raft 日志含完整 value，
  崩溃后可从日志重建索引；baseline 的 value 只在 RocksDB，必须等它写完。
  代价是要维护 pending 索引（index → offset），读路径先查它。
  解决延迟，但解决不了 fsync 造成的吞吐上限。
- **group commit —— 解决吞吐上限**。攒批共用一次 fsync。已实现，默认关闭。
- **内联值写时预热 / 自适应阈值** —— 与这两个瓶颈无关，另论。

## 📐 实验口径（已定）

    PUT   并发 100
    GET   并发 100
    SCAN  并发 1

SCAN 单线程是刻意的：一次范围查询本身读取量就大（gapkey 100/1000/10000），
再叠并发只会让各 goroutine 的随机起点互相冲刷缓存，结果不稳定且难以归因。
具体请求量按每次实验的数据规模另定。

**注意机器差异**：winbox-wsl 只有 12 核，100 并发的调度开销会明显高于
AutoDL 的 32 核。实测同配置下 winbox 的 GET 比 AutoDL 慢 140%（50 并发时），
而单并发的 SCAN 反而快 30~55%（独占 ext4 的磁盘优势）。
**跨机器只比相对收益，不比绝对值。**

历史数据的口径：36 格 AVP 主实验与新旧机器对照用的是 PUT/GET 并发 50、
SCAN 并发 1；六系统验证与 PASV 检查用 PUT 并发 20（那两次是功能验证，
不是性能对比）。

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

- [x] **PASV 与 original 无差异** —— 已查清，分两层：
  1. **开关此前完全没生效**（已修 `bcd102f`）：`SetDisableWAL` 在 `persister.Init`
     之后 21 行，而该设置只在建库时读一次。两者的 WAL 曾字节数分毫不差
     （64B 档 1,920,391；16KB 档 49,264,479）。修复后 PASV 的 WAL 为 **0 字节**。
  2. **开关生效后仍无差异**（−2.1% @64B，+0.6% @16KB，均在噪声内）。
     这是可信的负面结果：PUT 有 99.9% 时间在等 apply，RocksDB 的 WAL 写入嵌在
     其中约 0.03ms，省掉它等于在 0.2% 的份额里做优化。

  **论文测出 +26.5%，说明其写路径分布与我们不同**，最可能是 3 节点 3 副本下
  网络共识占了大头。这一差异需在多节点实验时核对——若多节点下仍无差异，
  则说明我们的 apply 路径实现与论文的基线不是一回事。

## 🔧 多节点正确性（2026-09-04，三个 bug 已修）

两节点（240+241）跑通前清掉了三个环环相扣的问题：

    8b6d1a9  去掉 GC 里 +64000 的历史补偿
    fd492c2  GC 读取失败不再静默跳过（这道防护当场抓出了下一个 bug）
    8185987  偏移与文件版本配对，消除 GC 切换窗口的竞态

### 1. `+64000` 补偿（`GC_opt.go`）

follower 读 entry 时无条件加 64000 字节，注释说"follower 偏移统一比 leader 小
一个 vsize"。**根因是 `O_APPEND`**：follower 处理冲突日志走"Seek 回退再覆盖"，
而 POSIX 规定该模式下 Seek 对写入位置无效，覆盖静默变成追加，文件末尾多一条记录。
`O_APPEND` 早些时候已改掉，补偿成了纯负担——而 `64000` 是**当年那次实验的 value
大小**，写死在代码里。value≠64000 时 follower 的 GC 必然 seek 越界。

（旁证：被注释掉的 `index+int64(kvs.valueSize)` 即使启用也是错的，
`kvs.valueSize` 全仓库从未被赋值。）

### 2. GC 读取失败被静默跳过 → 数据永久丢失

两个读取 goroutine 把错误吞掉（一个 `continue` 跳过、一个 `break` 提前收尾），
合并照常完成、整轮报成功，随后 `os.Remove(kvs.oldLog)` 删掉源文件——**没搬过去的
数据就此永久丢失**，而存储引擎里的偏移还指着一个不存在的文件，全程无任何日志。

已改为：读取失败即中止本轮、保住源文件、下个周期重试；后续轮次的失败也不再
`panic`（那既拖垮节点又跳过了重试）。

### 3. 偏移与文件版本来自两个时刻两把锁

    FileVersion  决定写入时记录（AppendEntries / StartPut），rf.mu 下
    offset       实际写入文件时产生（WriteEntryToFile），logMu 下

GC 切换文件只走 `logMu`，拦不住持 `rf.mu` 的写入路径。切换窗口内写入的记录
**偏移属于新文件、版本却记成旧的**，读取时拿新文件偏移去旧文件找，越界 EOF。

实测：follower 报 `failed to read entry at index 12169484`，而其 sorted 文件
12166322 字节——超出 3162 字节，**正好 3 条 entry**，即那个窗口里挤进去的记录数。

修法：`logVersion` 与 `logFile` 一同在 `logMu` 下切换；`offsetVersions` 与
`Offsets` 一起追加/消费/截断；`ApplyMsg.FileVersion` 传上去，applyLoop 改用它。

### 验证到了什么程度

**已验证**：两节点、20000 条 × 1KB、两边均触发 GC —— 两节点 GC 完全对称
（3 轮 / 2 次建索引 / 0 错误），GET 200/200、SCAN 1000/1000。修复前 follower
稳定报 EOF 且卡在第 2 轮。

**已补验（2026-09-04）：三节点故障切换**

    1. node0(240) 当选 leader，写入 20000 条 × 1KB     VERIFY_OK
    2. kill -9 node0
    3. node1(241) 当选新 leader
    4. 从新 leader 读同一批数据                        FAILOVER_VERIFY_OK
       GET 200/200 正确、0 取不到；SCAN 1000/1000 正确、0 范围失败

这正是该 bug 最严重后果的检验点——修复前那些偏移错位的记录，会在这一步暴露成
"读不到"或"值不对"。校验工具：`benchmark/readonly/`（只读，不写入）。

**顺带发现：两节点集群做不了故障切换测试。** 多数派是 2，挂掉一个就凑不齐票，
剩下的节点永远停在 Candidate。这是 Raft 的正确行为——两节点容错能力为 0，
也正是论文用 3 副本的原因。首次尝试时 240 就卡在 `Follower -> Candidate`。

三节点搭法（脚本 `~/three-node.sh`，两台机器凑三个实例）：

    node0  240:3099  internal 30991
    node1  241:3099  internal 30991
    node2  241:3100  internal 30992

对正确性验证等效（Raft 不关心节点在哪台物理机），但**性能测试不能这么跑**，
同机实例会争资源。

**已补验（2026-09-04 下午）：重复测量 + 工具检测**

两节点（leader 240 / follower 241）连续 8 轮，每轮：起集群 → 写入并校验 → 等两边
GC 稳定 → **分别直接读 leader 和 follower** 校验（server 无 leader 检查，所以能读到
follower 自己 GC 重建后的索引）→ 收集日志 → 停。脚本：服务器 `~/rep-node.sh`，
Mac 侧 `driver.sh`（scratchpad）。

    轮  value  条数   构建     写入校验  读leader  读follower  GC(240|241)  日志错误
    1   64B    20000  normal   OK        300/1500  300/1500    3|3          0
    2   1KB    20000  normal   OK        300/1500  300/1500    3|3          0
    3   4KB    10000  normal   OK        300/1500  300/1500    3|3          0
    4   64B    20000  normal   OK        300/1500  300/1500    3|3          0
    5   1KB    20000  normal   OK        300/1500  300/1500    3|3          0
    6   4KB    10000  normal   OK        300/1500  300/1500    3|2 *        0
    7   1KB    8000   -race    OK        300/1500  300/1500    3|3          DATA RACE ×3 / ×10
    8   64B    8000   -race    OK        300/1500  300/1500    3|3          DATA RACE ×9 / ×3

    * follower 少一轮 GC：GC 由 5s 定时检查文件大小触发，两边检查时刻不同，
      follower 一次跨过了两个阈值。数据校验全对，不是错误。

**数据层面 8/8 全对**（GET 300/300、SCAN 1500/1500，两个节点各自读）。第三个 bug
（偏移与文件版本错配）在 3 种 value 大小 × 多轮下没有复现。

**-race 抓到 11 对无同步访问**（数据没坏，但其中两对能坏）→ 已修 `1eb42fa`：
- `doAppendEntries` 组装 AppendEntries 请求时不持 `rf.mu`，裸读 log 切片 /
  nextIndex / commitIndex / lastIncludedIndex。`Start` 的 append 重分配时可读到撕裂的
  切片头（越界或 nil——原代码那句 `rf.log[i] == nil` 检查就是给这个打的补丁）；
  `compactLog` 换数组 + 推进 lastIncludedIndex 是两步，夹在中间会把错误的条目发给
  follower，且 term 一致时一致性检查发现不了。修：锁内快照并拷贝指针切片，编码和
  RPC 在锁外。
- `StartPut` / 选举的 TermLog 在 `Start` 返回后写回 `op.Index/op.Term`，而此时同一
  结构体已被复制 goroutine 拿去 gob 编码。修：`Start` 在发布前填好，调用方只用返回值。
- follower 侧 `rf.numGC`（GC 无锁写、AppendEntries 读）——只喂已废弃的
  `Command.FileVersion`，版本现由 `offsetVersions` 携带，整条路径删除。
- `AppendMonitor` 诊断循环裸读——改为锁内读。

**静态工具（全部在 tikv240 上跑，Mac 没有 RocksDB 头文件）**：
- `go vet`：2 项（persister 不可达代码、丢弃的 cancel）→ 已清 `15b2e16`，现为 0
- `staticcheck`：50 项，全是 U1000 未使用 / SA1019 弃用 API / S1000 风格 /
  SA4004 `for{...break}`，无正确性类
- `errcheck`：62 项，值得看的只有 GC 输出文件只 Flush 不 Sync → 已修 `3aad91d`
  （源日志删除前必须 fsync 排序文件，否则掉电丢整轮数据）
- `go test -race`：通过（只有 persister 的 5 个单测，覆盖有限）

**修后复跑（同日）**：

    第二轮（HEAD 15b2e16）             第三轮（HEAD c0e41cd）
    1KB  race    数据OK  race 1|1        1KB  race    数据OK  race 0|0
    64B  race    数据OK  race 0|1        64B  race    数据OK  race 0|0
    4KB  race    数据OK  race 0|1
    1KB  normal  数据OK  错误 0|0
    64B  normal  数据OK  错误 0|0

第二轮剩下的那一对是 GC 换 `rf.persister` 指针无锁、写路径持 logMu 读（只喂 PadKey，
读到哪个都对，但指针读写本身要同步）→ `c0e41cd` 加 logMu。第三轮两边 0 报告。

合计 **15 个回合数据全对**（每回合两个节点各自 GET 300 + SCAN 1500）。

**已补验（同日傍晚）：-race 故障切换**

三节点 -race 构建，流程：写 20000×1KB → GC 稳定 → `kill -9` leader → 直读两个存活节点
→ 经新 leader 重写 20000×512B → GC 稳定 → 再直读 → 三节点日志查 DATA RACE。
脚本：服务器 `~/three-race-node.sh`，Mac 侧 `failover-race.sh` / `failover-loop.sh`。

    首跑    数据全对，0 race，但 **65 秒没选出新 leader**（node2 卡 Candidate，node1 从未变 Candidate）
    复跑×4  数据全对，0 race，新 leader 2~3 秒（node1 与 node2 各当选过）

首跑的卡死没能带诊断复现，属间歇性；但结构性原因是明确的 → `373e26d`：
- `sendRequestVote` 每次 `grpc.Dial` 新连接且 `WithBlock` 无超时，对死节点永不返回；
- 计票循环只在"全部应答"或"过半"退出，死节点那份应答永远不来、活着的那票又因 2s RPC
  超时丢掉时，candidate 永久阻塞，不重选也不让位。
修：投票走连接池（对不可达节点快速失败）；计票最多等一个选举超时；选举决策打 debug 日志。

顺带发现并修掉一个 GC 数据丢失窗口 `dc62bb6`：切换文件后立即对旧库建 RocksDB 迭代器
（快照），已提交未 apply 的旧版本条目在快照之后才写进旧库，GC 看不见，随后旧文件旧库
一起删。修：切换后等 Raft 层报告旧版本无待 apply 条目再建迭代器。

**原型没有崩溃恢复**（`ReadPersist` 被注释、状态全在内存）→ 用户决定修而不是写 limitation。
设计草案 `docs/crash-recovery.md`，待确认后实现。

**仍未验证**：
- [ ] **三台物理机**。现为 2 台跑 3 实例，性能数据不可用；论文口径是 3 节点 3 副本
- [ ] **-race 覆盖冲突截断 / 旧 leader 重启回归**。依赖崩溃恢复实现后才能测
- [ ] **doAppendEntries 加锁后的吞吐**。锁内只拷贝指针切片，理论上可忽略，但没量过；
      下次跑 PUT 主表时顺带与 `73306ea` 对照一次
- [ ] **GC 等待 apply 的耗时**（`dc62bb6`）。正常应为 0~几 ms；超过 50ms 会打印
      "GC 等待旧版本条目 apply 完毕"，主表实验时留意

## 🔍 读路径不经过 Raft，也没有 leader 检查（2026-09-04 发现，暂不修）

`StartGet` 与 `ScanRangeInRaft` 里的 leader 检查和 ReadIndex 整段被注释掉
（原作者 commit `3178d7b`，2025-01-18，早于 AVP 工作）。`raft.GetReadIndex`
的实现还在（向所有 peer 发一轮心跳、多数派回应才返回 commitIndex），只是没人调用。

现状：
- 任何节点收到 GET/SCAN 都直接读本地 RocksDB + 日志文件返回，不确认自己是不是
  leader，也不等 apply 追上 commit。**读操作零跨节点网络交互。**
- 客户端把读固定发给 `leaderId`（默认 0），server 永不回 `ErrWrongLeader`，
  发给谁就从谁读。我们的测试都手动指向了真 leader，结果才正确——这是测试设计对，
  不是系统保证。
- 所有性能测试 client 与 server 同机，client→server 这一跳走 loopback。

后果：
1. **正确性**：读 follower 会拿到落后的数据，被分区的旧 leader 会返回过期值。
   现在的 GET 不是线性一致读。
2. **可比性**：GET 的延迟/吞吐里少了 ReadIndex 那一轮 RTT。TiKV 是 lease read，
   直接对比 Nezha 占便宜。
3. **论文数字**：按时间推断，论文的 GET 数据同样不带 ReadIndex（无法确证）。

**决定（用户 2026-09-04）：目前先这样测，记入 TODO，后续再补。**

- [ ] 最低限度：恢复 leader 检查，非 leader 回 `ErrWrongLeader`。零网络开销，
      不改性能数字，只堵读 follower 的口子。
- [ ] 公平对比：实现 lease read（leader 心跳多数派成功后的租期内本地读）。
      开销接近现在，也是 TiKV 的做法。
- [ ] 严格模式：ReadIndex 做成可选开关，量化"线性一致读多贵"。现有
      `GetReadIndex` 每次读都起 goroutine 发一轮心跳，直接打开吞吐会崩，
      要改成多个读共享一次心跳的批量版。
- [ ] client 与 server 分机跑一次，量 loopback 与真实网络的差距（只影响绝对数字）。

顺带一提：这个"没有 leader 检查"反而让**直接读 follower 校验其本地状态**成为可能，
重复验证脚本（见上）就靠它检查 follower GC 重建后的索引。

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
