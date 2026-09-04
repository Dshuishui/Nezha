#!/bin/bash
# 崩溃恢复验证（-race 构建，三节点）。
#   MODE=restart（默认）: S1 follower 宕机→写入→重启追平；S2 leader 宕机→新 leader 写入→旧 leader 重启追平
#   MODE=midgc          : S3 node2 在 GC 切换之后、搬运之前被 kill -9，重启后重做 GC
set -u
cd "$(dirname "$0")"
MODE=${MODE:-restart}; LOG=recover-$MODE.log; : > "$LOG"
r() { ssh -o ConnectTimeout=40 -o ServerAliveInterval=15 "$@" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
N=20000; VS_A=1024; VS_B=512; VS_C=256
ALL="192.168.1.240:3099,192.168.1.241:3099,192.168.1.241:3100"
fail=0
host_of() { case $1 in 0) echo tikv240;; *) echo tikv241;; esac; }
addr_of() { case $1 in 0) echo 192.168.1.240:3099;; 1) echo 192.168.1.241:3099;; 2) echo 192.168.1.241:3100;; esac; }
report_all() { for i in 0 1 2; do rep=$(r "$(host_of $i)" "~/three-node.sh report $i"); echo "$rep" | sed 's/^/    /' | tee -a "$LOG"; echo "$rep" | grep -qE 'races=0 err_lines=0' || fail=1; done; }
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
    if [ $(( $(date +%s) - t0 )) -ge $3 ]; then say "node$1 在 ${3}s 内未读全对"; echo "$R" | tail -3 | sed 's/^/    /' | tee -a "$LOG"; fail=1; return 1; fi
    sleep 5
  done; }
restart_node() { say "$(r "$(host_of $1)" "~/three-node.sh restart $1" | tail -1)"; sleep 6; r "$(host_of $1)" "~/three-node.sh recoverlog $1" | sed 's/^/    /' | tee -a "$LOG"; }

say "===== 1. 拉起三节点（-race, mode=${MODE}）====="
P0=""; P2=""; [ "$MODE" = midgc ] && P2="GC_PAUSE_MS=20000"
say "$(r tikv240 "$P0 BIN=race ~/three-node.sh start 0 3099 30991 $VS_A $N" | tail -1)"; sleep 3
say "$(r tikv241 "BIN=race ~/three-node.sh start 1 3099 30991 $VS_A $N" | tail -1)"
say "$(r tikv241 "$P2 BIN=race ~/three-node.sh start 2 3100 30992 $VS_A $N" | tail -1)"
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
  say "$(r tikv241 '~/three-node.sh kill9 2')"
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
  say "$(r tikv241 '~/three-node.sh kill9 2')"
  say "===== S1-b. node2 缺席期间经 leader 重写 $N × ${VS_B}B ====="
  write_all 0 $VS_B
  say "===== S1-c. 重启 node2，等它追平后直读 ====="
  restart_node 2
  read_until_ok 2 $VS_B 120
  say "===== S2-a. kill -9 leader node0 ====="
  say "$(r tikv240 '~/three-node.sh kill9 0')"; sleep 10
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
