# 多节点正确性验证脚本

两台实验机（`tikv240` = node0，`tikv241` = node1/node2）上的三节点/两节点验证。
所有脚本都假定：服务器上代码在 `~/work/Nezha`，环境在 `~/env.sh`，Mac 侧通过 `ssh tikv240` /
`ssh tikv241` 免密可达；两台机器之间没有互信，所以编排全部由 Mac 侧驱动。

| 脚本 | 放哪 | 作用 |
|---|---|---|
| `rep-node.sh` | 两台服务器 `~/` | 两节点稳态验证的节点控制：`start ROLE VS N [normal\|race]` / `stop` / `report` |
| `two-node-rounds.sh` | Mac | 两节点多轮：写入校验 → 等 GC 稳定 → 分别直读 leader 与 follower → 收日志 |
| `three-node.sh` | 两台服务器 `~/` | 三节点节点控制：`start IDX PORT IPORT VS N` / `kill9` / `restart`（不清目录，走恢复）/ `stop` / `report` / `recoverlog`；`BIN=race\|normal`，`GC_PAUSE_MS=` 让 GC 在切换后暂停 |
| `failover.sh` | Mac | 杀 leader → 新 leader → 直读两个存活节点 → 经新 leader 重写 → 再直读 |
| `failover-loop.sh` | Mac | 连跑 N 次 `failover.sh` |
| `recover.sh` | Mac | 崩溃恢复：`MODE=restart` follower/leader 宕机后原地重启追平；`MODE=midgc` GC 中途宕机后重启重做 |

服务器侧脚本部署：`scp three-node.sh tikv240:~/ && ssh tikv240 chmod +x ~/three-node.sh`（241 同）。
Mac 侧脚本里调用的远端名字是 `~/three-race-node.sh` / `~/rep-node.sh`，部署时按需改名或加软链。

直读 follower 之所以可行，是因为读路径目前没有 leader 检查（见 TODO-avp.md）；这恰好让
"follower 本地状态是否正确"可以被直接验证。
