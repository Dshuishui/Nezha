#!/bin/bash
# 三节点故障切换，-race 构建。稳态 race 轮次没走到的路径：选举、上任空指令、
# follower 在新 leader 下继续写入与 GC。node0 死后不重启（原型没有崩溃恢复）。
set -u
cd "$(dirname "$0")"; LOG=failover-race.log; : > "$LOG"
r() { ssh -o ConnectTimeout=40 -o ServerAliveInterval=15 "$@" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
N=20000; VS_A=1024; VS_B=512; BIN=${BIN:-race}; KEEP=${KEEP:-0}
LOG=failover-$BIN.log; : > "$LOG"
ALL="192.168.1.240:3099,192.168.1.241:3099,192.168.1.241:3100"
fail=0
wait_gc() { # 参数 = "host idx" 对；等 gc_done 连续两次不变且各 ≥1
  local nodes=("$@") prev="" cur k spec h i g ok
  for k in $(seq 1 24); do sleep 15; cur=""; ok=1
    for spec in "${nodes[@]}"; do h=${spec% *}; i=${spec#* }
      g=$(r "$h" "~/three-node.sh report $i" | grep -oE 'gc_done=[0-9]+'); cur="$cur $g"
      [ "${g#gc_done=}" -ge 1 ] 2>/dev/null || ok=0
    done
    [ "$cur" = "$prev" ] && [ $ok = 1 ] && { say "GC 稳定:$cur"; return 0; }; prev=$cur
  done
  say "GC 未稳定:$cur"; fail=1; }

say "===== 1. 拉起三节点（-race）====="
say "$(r tikv240 "BIN=$BIN ~/three-node.sh start 0 3099 30991 $VS_A $N" | tail -1)"; sleep 3
say "$(r tikv241 "BIN=$BIN ~/three-node.sh start 1 3099 30991 $VS_A $N" | tail -1)"
say "$(r tikv241 "BIN=$BIN ~/three-node.sh start 2 3100 30992 $VS_A $N" | tail -1)"
grep -c "STARTED node" "$LOG" | grep -q '^3$' || { say "有节点没起来，终止"; exit 1; }
sleep 12
say "===== 2. 写入 $N × ${VS_A}B 并校验（首联 node0）====="
W=$(r tikv240 "source ~/env.sh; /tmp/scanverify -servers $ALL -leader 0 -dnums $N -vsize $VS_A -span 50 -sample 20" | grep -vE 'new pool success'); echo "$W" | tail -3 | sed 's/^/    /' | tee -a "$LOG"
echo "$W" | grep -q VERIFY_OK || fail=1
wait_gc "tikv240 0" "tikv241 1" "tikv241 2"
say "===== 3. kill -9 node0 ====="
say "$(r tikv240 '~/three-node.sh kill9 0')"; sleep 20
r tikv241 "grep -hE 'Candidate|Leader|election|RequestVote' ~/work/three-1/n.log ~/work/three-2/n.log | tail -12" | sed 's/^/    /' | tee -a "$LOG"
say "===== 4. 从 node1、node2 直接读旧数据 ====="
for a in 192.168.1.241:3099 192.168.1.241:3100; do
  R=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers $a -dnums $N -vsize $VS_A -check 300 -sample 30" | grep -vE 'new pool success'); echo "$R" | tail -3 | sed "s/^/    [$a] /" | tee -a "$LOG"; echo "$R" | grep -q FAILOVER_VERIFY_OK || fail=1
done
say "===== 5. 经新 leader 重写 $N × ${VS_B}B（首联 node1，靠重定向找 leader）====="
W=$(r tikv240 "source ~/env.sh; /tmp/scanverify -servers $ALL -leader 1 -dnums $N -vsize $VS_B -span 50 -sample 20" | grep -vE 'new pool success'); echo "$W" | tail -3 | sed 's/^/    /' | tee -a "$LOG"
echo "$W" | grep -q VERIFY_OK || fail=1
wait_gc "tikv241 1" "tikv241 2"
say "===== 6. 从 node1、node2 直接读新数据 ====="
for a in 192.168.1.241:3099 192.168.1.241:3100; do
  R=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers $a -dnums $N -vsize $VS_B -check 300 -sample 30" | grep -vE 'new pool success'); echo "$R" | tail -3 | sed "s/^/    [$a] /" | tee -a "$LOG"; echo "$R" | grep -q FAILOVER_VERIFY_OK || fail=1
done
say "===== 7. 三个节点的日志 ====="
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do h=${spec% *}; i=${spec#* }; rep=$(r "$h" "~/three-node.sh report $i"); echo "$rep" | sed 's/^/    /' | tee -a "$LOG"; echo "$rep" | grep -qE 'races=0 err_lines=0' || fail=1; done
[ "$KEEP" = 1 ] || for spec in "tikv241 1" "tikv241 2"; do h=${spec% *}; i=${spec#* }; r "$h" "~/three-node.sh stop $i" >/dev/null; done
[ $fail = 0 ] && say "FAILOVER_RACE_OK" || say "FAILOVER_RACE_FAIL"
