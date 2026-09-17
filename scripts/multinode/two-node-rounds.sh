#!/bin/bash
# 两节点重复验证驱动（在 Mac 上跑，通过 ssh 编排 240/241）。
# 每轮：起两节点 → 写入+校验 → 等 GC 稳定 → 直接从两台各读一遍 → 收集日志 → 停。
# 直接读 follower 是关键：server 侧没有 leader 检查，所以能读到 follower 自己 GC 重建后的索引。
set -u
cd "$(dirname "$0")"
# shellcheck source=scripts/multinode/gate.sh
. "$(dirname "$0")/gate.sh"
# 直读某个指定节点的**本地**状态，所以这些节点必须用 -leaderCheck=false 起。
# `-leaderCheck` 自 811ac32 起默认开，follower 上的读一律回 ErrWrongLeader，而客户端
# 的 -servers 只有一个地址时 redirect 无处可去——本脚本的直读步骤因此从 2026-09-13
# 起恒判失败，直到 2026-09-16 才发现。理由与判定见 gate.sh 的 gate_read_ok。
RDFLAG="EXTRA='-leaderCheck=false'"

# 这个脚本里的地址还是写死的两台拓扑，所以 TOPO=three 下**拒绝运行**而不是静默跑错。
# （recover.sh 与 slow-follower.sh 已接入 gate.sh 的集中拓扑，可以用 TOPO=three。）
[ "$TOPO" = two ] || { echo "$0 暂不支持 TOPO=${TOPO}：脚本内地址仍写死两台拓扑" >&2; exit 1; }

# 中途死掉必须停掉自己起的节点。happy path（第 70 行）和 START_FAIL 分支都会停，
# 但两者之间任何一次退出——set -u 撞到未定义变量、ssh 超时、Ctrl-C——都会把两个节点
# 留在 240/241 上。那两台是**共用**的，留下的进程会占住端口与磁盘，而下一次运行只会
# 报"启动失败"，指不到这里。trap 收不到 SIGKILL，所以它不是万能的，但覆盖其余情形。
cleanup_rep_nodes() {
  rq tikv240 '~/rep-node.sh stop leader' >/dev/null 2>&1 || true
  rq tikv241 '~/rep-node.sh stop follower' >/dev/null 2>&1 || true
}
trap cleanup_rep_nodes EXIT

LOG=rep.log; : > "$LOG"
# r / rq 在 gate.sh（带总超时、本机直跑、别名->用户名@IP），理由见那里。
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

say "编译客户端工具"
r tikv240 'source ~/env.sh; cd ~/work/Nezha; export TMPDIR=~/work/tmp; go build -o /tmp/scanverify ./cmd/bench/scanverify/ && go build -o /tmp/readonly ./cmd/bench/readonly/ && echo TOOLS_OK' | tail -1 | tee -a "$LOG"

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
  s1=$(r tikv240 "$RDFLAG ~/rep-node.sh start leader $VS $N $BIN" | tail -1); say "240: $s1"
  sleep 3
  s2=$(r tikv241 "$RDFLAG ~/rep-node.sh start follower $VS $N $BIN" | tail -1); say "241: $s2"
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
  # 读不对时把原因打出来：是这个脚本忘了 -leaderCheck=false，还是真的数据不对。
  gate_read_ok "$R0" "直读 240" | tee -a "$LOG"; gate_read_ok "$R1" "直读 241" | tee -a "$LOG"
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
