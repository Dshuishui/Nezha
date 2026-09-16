#!/bin/bash
# 崩溃恢复验证（-race 构建，三节点）。
#   MODE=restart（默认）: S1 follower 宕机→写入→重启追平；S2 leader 宕机→新 leader 写入→旧 leader 重启追平
#   MODE=midgc          : S3 node2 在 GC 切换之后、搬运之前被 kill -9，重启后重做 GC
set -u
cd "$(dirname "$0")"
# shellcheck source=scripts/multinode/gate.sh
. ./gate.sh
# 直读某个指定节点的**本地**状态，所以这些节点必须用 -leaderCheck=false 起。
# `-leaderCheck` 自 811ac32 起默认开，follower 上的读一律回 ErrWrongLeader，而客户端
# 的 -servers 只有一个地址时 redirect 无处可去——本脚本的直读步骤因此从 2026-09-13
# 起恒判失败，直到 2026-09-16 才发现。理由与判定见 gate.sh 的 gate_read_ok。
MODE=${MODE:-restart}; LOG=recover-$MODE.log; : > "$LOG"
RDFLAG="EXTRA='-leaderCheck=false'"
# r / rq / host_of / addr_of / peers_str / servers_str 全在 gate.sh，理由见那里。
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
N=20000; VS_A=1024; VS_B=512; VS_C=256
ALL=$(servers_str)
fail=0
# 本脚本的整个立意就是三个节点最后都活着并且追平了，所以三个都要求 alive=yes。
# 只查 races/err_lines 的话，一个重启之后又死掉的节点会静默判过——而那恰恰是
# 崩溃恢复最该抓的失效。
report_all() { local i rep why; for i in 0 1 2; do
  rep=$(r "$(host_of $i)" "~/three-node.sh report $i"); echo "$rep" | sed 's/^/    /' | tee -a "$LOG"
  why=$(gate_report_ok "$rep" yes "node$i") || fail=1
  [ -n "$why" ] && echo "$why" | tee -a "$LOG"
done; }
wait_gc() { local prev="" cur k spec h i g ok nodes=("$@")
  for k in $(seq 1 24); do sleep 15; cur=""; ok=1
    for i in "${nodes[@]}"; do g=$(r "$(host_of $i)" "~/three-node.sh report $i" | grep -oE 'gc_done=[0-9]+'); cur="$cur $g"; [ "${g#gc_done=}" -ge 1 ] 2>/dev/null || ok=0; done
    [ "$cur" = "$prev" ] && [ $ok = 1 ] && { say "GC 稳定:$cur"; return 0; }; prev=$cur; done
  say "GC 未稳定:$cur"; fail=1; }
write_all() { # $1=leader idx to contact first, $2=vsize
  local W; W=$(r tikv240 "source ~/env.sh; /tmp/scanverify -servers $ALL -leader $1 -dnums $N -vsize $2 -span 50 -sample 20" | grep -vE 'new pool success')
  echo "$W" | tail -3 | sed 's/^/    /' | tee -a "$LOG"; echo "$W" | grep -q VERIFY_OK || fail=1; }
read_until_ok() { # $1=idx $2=vsize $3=timeout_s ；恢复后的节点要先追上再读得全对
  local t0=$(date +%s) R
  while :; do
    R=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers $(addr_of $1) -dnums $N -vsize $2 -check 300 -sample 30" | grep -vE 'new pool success')
    if echo "$R" | grep -q FAILOVER_VERIFY_OK; then say "node$1 直读全对（用时 $(( $(date +%s) - t0 ))s）"; echo "$R" | tail -3 | sed 's/^/    /' | tee -a "$LOG"; return 0; fi
    # 目标节点把读挡回来时立刻退出：那是配置问题，等下去也不会变对，白等一个超时。
    if echo "$R" | grep -q "未持有 leader 身份"; then say "node$1 直读被挡回"; gate_read_ok "$R" "node$1 直读" | tee -a "$LOG"; fail=1; return 1; fi
    if [ $(( $(date +%s) - t0 )) -ge $3 ]; then say "node$1 在 ${3}s 内未读全对"; gate_read_ok "$R" "node$1 直读" | tee -a "$LOG"; fail=1; return 1; fi
    sleep 5
  done; }
restart_node() { # 回执必须有：2026-09-16 这一句静默返回空串，节点其实没重启（数据目录里
  # 连 n1.log 都没生成），而脚本接着去读它、空转 41 分钟才判失败，原因还显示成"数据不对"。
  local out; out=$(rq "$(host_of "$1")" "~/three-node.sh restart $1" | tail -1)
  say "${out:-（无回执）}"
  require_out "$out" "restart node$1" | tee -a "$LOG" || { fail=1; return 1; }
  case "$out" in RESTARTED*) ;; *) say "restart node$1 回执不是 RESTARTED"; fail=1; return 1;; esac
  sleep 6; rq "$(host_of "$1")" "~/three-node.sh recoverlog $1" | sed 's/^/    /' | tee -a "$LOG"; }

trap cleanup_nodes EXIT   # 定义在 gate.sh，理由见那里

# 启动前先清掉**我们自己**上一轮的残留。
#
# trap 只能覆盖"脚本自己出错退出"，覆盖不了被 SIGKILL：2026-09-16 有一轮因本机内存不足
# 被系统杀掉，EXIT trap 根本没机会跑，两个节点留在 tikv241 上，下一轮启动就端口冲突。
# 所以清理必须**两头都有**：挂 EXIT，并且启动前再清一遍。
# 只清 node 0/1/2（靠 pid 文件与 `-data ~/work/three-N` 匹配），碰不到别人的实验。
cleanup_nodes

say "拓扑 TOPO=${TOPO}：node0=$(host_of 0) node1=$(host_of 1) node2=$(host_of 2)；驱动跑在 $(hostname 2>/dev/null)（ON_SERVER=${ON_SERVER}）"
say "===== 1. 拉起三节点（-race, mode=${MODE}）====="
P0=""; P2=""; [ "$MODE" = midgc ] && P2="GC_PAUSE_MS=20000"
# 端口与 peers 全部由拓扑派生，不再写死——TOPO=three 时 node2 在 node55 上。
PEERSTR=$(peers_str)
for i in 0 1 2; do
  read -r P IP <<<"$(port_of "$i")"
  PX=""; [ "$i" = 0 ] && PX="$P0"; [ "$i" = 2 ] && PX="$P2"
  OUT=$(rq "$(host_of "$i")" "$PX BIN=race PEERS='$PEERSTR' $RDFLAG ~/three-node.sh start $i $P $IP $VS_A $N" | tail -1)
  say "${OUT:-（无回执）}"
  require_out "$OUT" "start node$i" | tee -a "$LOG" || { fail=1; break; }
  [ "$i" = 0 ] && sleep 3
done
grep -c "STARTED node" "$LOG" | grep -q '^3$' || { say "有节点没起来，终止"; exit 1; }
sleep 12

if [ "$MODE" = midgc ]; then
  say "===== 2. 写入 $N × ${VS_A}B；node2 的 GC 会在切换后暂停 20s ====="
  ( r tikv240 "source ~/env.sh; /tmp/scanverify -servers $ALL -leader 0 -dnums $N -vsize $VS_A -span 50 -sample 20" | grep -vE 'new pool success' | tail -3 | sed 's/^/    [写入] /' | tee -a "$LOG" ) &
  WPID=$!
  say "等待 node2 进入 GC 暂停窗口"
  for k in $(seq 1 60); do
    if r tikv241 "grep -q 'GC-PAUSE' ~/work/three-2/n.log" ; then break; fi; sleep 2
  done
  say "$(r tikv241 "grep -h 'GC-PAUSE\|设置kvs.currentLog' ~/work/three-2/n.log | tail -2")"
  say "===== 3. 在 GC 中途 kill -9 node2 ====="
  K=$(rq tikv241 "~/three-node.sh kill9 2"); say "${K:-（无回执）}"
  require_out "$K" "kill9 node2" | tee -a "$LOG" || fail=1
  wait $WPID
  say "===== 4. 重启 node2，应重做第 1 轮 GC ====="
  restart_node 2
  sleep 5
  read_until_ok 2 $VS_A 120
  say "$(r tikv241 "grep -hE '垃圾回收完成|GC 曾中断|重做' ~/work/three-2/n1.log | head -5")"
  say "===== 5. 继续经 leader 重写 $N × ${VS_B}B，再直读 node2 ====="
  write_all 0 $VS_B
  wait_gc 0 1 2
  read_until_ok 2 $VS_B 90
  say "===== 6. 日志 ====="
  report_all
  r tikv241 "ls -la ~/work/three-2/data/valuelog/ ~/work/three-2/data/*.json; cat ~/work/three-2/data/kv_state.json" | sed 's/^/    /' | tee -a "$LOG"
else
  say "===== 2. 写入 $N × ${VS_A}B 并校验 ====="
  write_all 0 $VS_A
  wait_gc 0 1 2
  say "===== S1-a. kill -9 follower node2 ====="
  K=$(rq tikv241 "~/three-node.sh kill9 2"); say "${K:-（无回执）}"
  require_out "$K" "kill9 node2" | tee -a "$LOG" || fail=1
  say "===== S1-b. node2 缺席期间经 leader 重写 $N × ${VS_B}B ====="
  write_all 0 $VS_B
  say "===== S1-c. 重启 node2，等它追平后直读 ====="
  restart_node 2
  read_until_ok 2 $VS_B 120
  say "===== S2-a. kill -9 leader node0 ====="
  W0=$(leader_wins); W0=${W0:-0}
  K=$(rq tikv240 "~/three-node.sh kill9 0"); say "${K:-（无回执）}"
  require_out "$K" "kill9 node0" | tee -a "$LOG" || fail=1
  wait_new_leader "$W0" 40
  r tikv241 "grep -hE 'Candidate -> Leader' ~/work/three-1/n*.log ~/work/three-2/n*.log | tail -2" | sed 's/^/    /' | tee -a "$LOG"
  say "===== S2-b. 经新 leader 重写 $N × ${VS_C}B ====="
  write_all 1 $VS_C
  say "===== S2-c. 重启旧 leader node0，等它追平后直读 ====="
  restart_node 0
  read_until_ok 0 $VS_C 120
  wait_gc 0 1 2
  say "===== 收尾：三节点日志 ====="
  report_all
  r tikv240 "cat ~/work/three-0/data/kv_state.json ~/work/three-0/data/raft_state.json" | sed 's/^/    [node0 state] /' | tee -a "$LOG"
fi
for i in 0 1 2; do r "$(host_of $i)" "~/three-node.sh stop $i" >/dev/null; done
[ $fail = 0 ] && say "RECOVER_${MODE}_OK" || say "RECOVER_${MODE}_FAIL"
