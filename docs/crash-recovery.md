# 崩溃恢复设计（草案，待确认后实现）

> 现状：节点重启等于全新节点。`ReadPersist` 被注释掉，Raft 的 term/votedFor/日志/偏移队列
> 全在内存；RocksDB 与日志文件虽然在磁盘上，但启动时没有任何代码去读它们、把状态接回来。
> 而且重启后的空节点追不上：leader 的 `compactLog` 已把老条目从内存裁掉，又没有 InstallSnapshot。

## 目标

1. 节点进程被 `kill -9` 后原地重启，能从本地磁盘恢复到崩溃前的状态，作为 follower 追上 leader；
   若它崩溃前是 leader，能参与新一轮选举。
2. 恢复过程不依赖其他节点传数据（第一版）。
3. 不改写路径的性能：新增的持久化只发生在**选举**和 **GC 切换**这两个低频时刻。

不做（第一版）：落后到 leader 内存日志之外的追赶（需要 InstallSnapshot，即传 RocksDB + 排序文件）。
leader 的压缩本来就受 `min(matchIndex)` 约束，节点宕机期间 leader 不会裁掉它还没收到的条目，
所以"原地重启"这一场景不需要快照；代价是宕机期间 leader 内存持续增长。

## 需要恢复的状态与来源

| 状态 | 现状 | 恢复来源 | 何时写 |
|---|---|---|---|
| `currentTerm`, `votedFor` | 内存 | 新增 `raft_state.json`，fsync | 每次变更（选举、看到更高 term） |
| `lastIncludedIndex/Term` | 内存 | `raft_state.json` | GC 切换时（见下） |
| `rf.log` 尾部 | 内存 | **扫当前日志文件重建**：每条记录含 Index / Term / Key / Value | 无需额外写 |
| `Offsets` / `offsetVersions`（未 apply 条目）| 内存 | 扫当前日志文件时顺带重建（index > lastApplied 的那些） | 无需额外写 |
| `lastApplied` | 内存 | **RocksDB 内特殊 key `\x00applied`**，与每条 apply 同一个 WriteBatch 写入 | 每次 apply（同批，无额外 fsync） |
| `commitIndex` | 内存 | 取 `lastApplied`；leader 的心跳会把它推上去 | — |
| GC 轮数 `numGC`、当前日志/库/排序文件路径、`FirstGC` 标志 | 内存 | 新增 `kv_state.json`，fsync | GC 切换成功后 |
| 排序文件的稀疏索引与 inlineCache | 内存 | 启动时对排序文件重建（`建立了索引` 已有该函数） | 无需额外写 |
| RocksDB 索引 | 已在磁盘 | 直接打开 `kv_state.json` 指向的库 | 已有 |

`lastApplied` 放进 RocksDB 而不是状态文件，是因为它和"哪些 key 已写进库"必须原子一致：
崩在两者之间就会重放或漏放。同一个 WriteBatch 由 RocksDB 保证原子，且 `-syncWAL` 下的持久性
语义与数据本身完全一样，不多一次 fsync。

## 启动流程

```
1. 读 kv_state.json     → numGC、当前日志路径、当前库路径、排序文件路径、FirstGC
   （文件不存在 = 全新节点，走现在的初始化）
2. 打开当前 RocksDB     → 读 \x00applied 得 lastApplied
3. 读 raft_state.json   → currentTerm、votedFor、lastIncludedIndex/Term
4. 扫当前日志文件（从头到尾，格式即 WriteEntryToFile 写的）：
     每条 → rf.log 追加 LogEntry{Term, Command{Key, Value, Index, Term}}
     若 index > lastApplied → Offsets/offsetVersions 追加（offset, numGC）
     记录文件末尾偏移 → rf.logOffset
   文件第一条的 index - 1 应等于 lastIncludedIndex（校验，不等则报错退出，不猜）
5. 若 FirstGC=false：对排序文件重建稀疏索引（现有函数）
6. commitIndex = lastApplied；role = Follower；正常启动各 loop
```

第 4 步的尾部截断：崩溃可能留下最后一条写了一半的记录（`-syncWAL` 下每条都 fsync，
只可能是最后一条）。读到长度不足或校验失败即截断文件到该记录起点，其余照常。

## GC 切换时的持久化顺序

GC 成功后现在的顺序是：切换 → 迁移 → 建索引 → 删旧日志。恢复需要 `kv_state.json` 在
"删旧日志"之前落盘，否则崩在中间会指向一个不存在的文件：

```
切换文件 → 等旧版本 apply 完（dc62bb6）→ 迁移 → fsync 排序文件（3aad91d）
→ 写 kv_state.json（新路径、numGC）并 fsync → 删旧日志、旧库
```

崩在"写 kv_state.json"之前：重启读到旧状态，旧日志/旧库还在，重做这一轮 GC 即可
（现有代码已允许重试：`switchedPersister` 字段）。崩在之后：新状态完整，旧文件残留，启动时清掉。

`lastIncludedIndex/Term` 也在此时写入 `raft_state.json`：值为旧文件最后一条的 index/term。
这样"当前日志文件第一条 index - 1 == lastIncludedIndex"的不变式在切换后成立。
`compactLog` 只裁内存、不动文件，所以恢复时 `rf.log` 会比崩溃前长（多出已 apply 的部分），
第一次 `compactLog` 会再裁掉，不影响正确性。

## 与现有机制的交互

- **follower 冲突截断**（`AppendEntriesInRaft` 里 `rf.log = rf.log[:logPos]` + Seek 覆盖写）：
  恢复重建的 `rf.log` 与文件内容一致，截断逻辑照常工作。
- **`FileVersion`**：恢复时所有未 apply 条目都在当前文件里，版本一律取 `numGC`，与 `offsetVersions` 语义一致。
- **重启后 leader 的 nextIndex**：leader 对它的 `nextIndex` 是宕机前的值，重启节点的日志尾部完整，
  一致性检查会直接通过，从缺的那条开始补。

## 验证方案

三节点，`-race` 构建：
1. 写入 N 条 → 触发 GC → `kill -9` 一个 follower → 继续写 M 条 → 重启该 follower（不清目录）
   → 等追上 → 直接读它，N+M 条全对。
2. 同上，杀的是 leader：新 leader 产生后继续写，旧 leader 重启回归为 follower，追上后直读全对。
3. 崩在 GC 中途：写入过程中对正在 GC 的节点 `kill -9`（用日志里 "Starting garbage collection" 触发），
   重启后重做 GC，读全对，且没有残留文件。
4. 每个场景至少 3 次。

## 实现工作量估计

- Raft 层：状态文件读写、日志扫描重建、启动挂接 ~150 行
- KV 层：`kv_state.json`、启动分支、GC 顺序调整、`\x00applied` 写入与读取 ~120 行
- 验证脚本：在 `three-race-node.sh` 上加 `restart`（不清目录）~30 行
