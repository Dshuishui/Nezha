#!/bin/bash
# 两节点重复验证驱动（在 Mac 上跑，通过 ssh 编排 240/241）。
# 每轮：起两节点 → 写入+校验 → 等 GC 稳定 → 直接从两台各读一遍 → 收集日志 → 停。
# 直接读 follower 是关键：server 侧没有 leader 检查，所以能读到 follower 自己 GC 重建后的索引。
set -u
cd "$(dirname "$0")"
LOG=rep.log; : > "$LOG"
r() { ssh -o ConnectTimeout=40 -o ServerAliveInterval=15 "$@" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

say "编译客户端工具"
r tikv240 'source ~/env.sh; cd ~/work/Nezha; export TMPDIR=~/work/tmp; go build -o /tmp/scanverify ./benchmark/scanverify/ && go build -o /tmp/readonly ./benchmark/readonly/ && echo TOOLS_OK' | tail -1 | tee -a "$LOG"

ROUNDS=(
  "64 20000 normal"
  "1024 20000 normal"
  "4096 10000 normal"
  "64 20000 normal"
  "1024 20000 normal"
  "4096 10000 normal"
  "1024 8000 race"
  "64 8000 race"
)
SUMMARY=()
i=0
for spec in "${ROUNDS[@]}"; do
  i=$((i+1)); set -- $spec; VS=$1; N=$2; BIN=$3
  say "===== ROUND $i: value=${VS}B n=$N bin=$BIN ====="
  s1=$(r tikv240 "~/rep-node.sh start leader $VS $N $BIN" | tail -1); say "240: $s1"
  sleep 3
  s2=$(r tikv241 "~/rep-node.sh start follower $VS $N $BIN" | tail -1); say "241: $s2"
  case "$s1$s2" in *FAIL*) say "启动失败，跳过本轮"; r tikv240 '~/rep-node.sh stop leader'; r tikv241 '~/rep-node.sh stop follower'; SUMMARY+=("R$i ${VS}B/$N/$BIN START_FAIL"); continue;; esac
  sleep 10
  T0=$(date +%s)
  W=$(r tikv240 "source ~/env.sh; /tmp/scanverify -servers 192.168.1.240:3099,192.168.1.241:3099 -dnums $N -vsize $VS -span 50 -sample 20" | grep -vE 'new pool success')
  echo "$W" | tail -4 | sed 's/^/    /' | tee -a "$LOG"
  WR=$(echo "$W" | grep -oE 'VERIFY_(OK|FAIL|EMPTY)' | tail -1); WR=${WR:-NO_RESULT}
  say "写入+校验: $WR 用时 $(( $(date +%s) - T0 ))s"
  # 等 GC 稳定：两边 gc_done 连续两次轮询不变且 ≥1（阈值是总量的 1/3，两边都该至少完成一轮）
  prev=""; stable=0
  for k in $(seq 1 24); do
    sleep 15
    g0=$(r tikv240 '~/rep-node.sh report leader' | grep -oE 'gc_done=[0-9]+' ); g1=$(r tikv241 '~/rep-node.sh report follower' | grep -oE 'gc_done=[0-9]+')
    cur="$g0/$g1"
    if [ "$cur" = "$prev" ] && [ "${g0#gc_done=}" -ge 1 ] && [ "${g1#gc_done=}" -ge 1 ]; then stable=1; break; fi
    prev=$cur
  done
  say "GC 状态 240:$g0 241:$g1 stable=$stable"
  R0=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers 192.168.1.240:3099 -dnums $N -vsize $VS -check 300 -sample 30" | grep -vE 'new pool success')
  echo "$R0" | tail -3 | sed 's/^/    [读240] /' | tee -a "$LOG"
  R1=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers 192.168.1.241:3099 -dnums $N -vsize $VS -check 300 -sample 30" | grep -vE 'new pool success')
  echo "$R1" | tail -3 | sed 's/^/    [读241] /' | tee -a "$LOG"
  RR0=$(echo "$R0" | grep -c FAILOVER_VERIFY_OK); RR1=$(echo "$R1" | grep -c FAILOVER_VERIFY_OK)
  rep0=$(r tikv240 '~/rep-node.sh report leader'); rep1=$(r tikv241 '~/rep-node.sh report follower')
  echo "$rep0" | sed 's/^/    240 /' | tee -a "$LOG"; echo "$rep1" | sed 's/^/    241 /' | tee -a "$LOG"
  r tikv240 '~/rep-node.sh stop leader' >/dev/null; r tikv241 '~/rep-node.sh stop follower' >/dev/null
  e0=$(echo "$rep0" | grep -oE 'err_lines=[0-9]+'); e1=$(echo "$rep1" | grep -oE 'err_lines=[0-9]+')
  verdict=OK
  [ "$WR" = VERIFY_OK ] && [ "$RR0" = 1 ] && [ "$RR1" = 1 ] && [ "$e0" = err_lines=0 ] && [ "$e1" = err_lines=0 ] && [ $stable = 1 ] || verdict=FAIL
  SUMMARY+=("R$i ${VS}B/$N/$BIN write=$WR read240=$RR0 read241=$RR1 gc=$g0|$g1 $e0|$e1 => $verdict")
  say "ROUND $i => $verdict"
  sleep 3
done
say "########## SUMMARY ##########"
printf '%s\n' "${SUMMARY[@]}" | tee -a "$LOG"
say "REP_DONE"
