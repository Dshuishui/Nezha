#!/bin/bash
# 快照安装与 GC 争用同一份状态时，会不会有一方被永久饿死。
#
# 两者是**双向拒绝**而不是加锁（见 gcloop.go 的 `if kvs.installing` 与 snapshot.go 的
# `if kvs.gcActive || kvs.gcInProgress`）。这么设计的理由是正当的：一轮 GC 可以长到数分钟，
# 让安装排队等它会把读一起挡住。但拒绝式互斥有个前提——**双方都必须有机会跑成**。
#
# 要找的失效是这个：GC 连续不断地跑时 gcActive/gcInProgress 几乎恒为真，于是
#   1. follower 的每次安装都被退回（leader 按 snapshotRetryPause 重试）；
#   2. 而 leader 的内存日志被字节预算截断了，follower 也**没法靠补日志追上**；
#   3. 于是它永久落后，而三节点提交只等中位数，集群看起来一切正常。
# 这就是 docs/snapshot-replication.md 里说的 limbo 换了个形式：不是忘了上报，
# 而是上报了、重试了，但对端永远在忙。
#
# 所以这里刻意把 GC 压成"几乎一直在跑"：GC 阈值取到很小、absorbRatio 取到很小，
# 再持续写入。然后按住一个 follower 让它落到压缩点之前，放开，看它到底能不能装上。
#
# 判据不是"有没有被退回"——被退回是设计行为、必然会发生。判据是**最终有没有装上**，
# 以及退回了多少次才装上（次数写进输出，供以后回归对比）。
set -u
cd "$(dirname "$0")"
# shellcheck source=scripts/multinode/gate.sh
. ./gate.sh
require_driver_host || exit 1   # 只能在 tikv240 上跑，理由见 gate.sh
[ "$TOPO" = two ] || [ "$TOPO" = three ] || { echo "TOPO 只认 two|three" >&2; exit 1; }

LOG=snapshot-vs-gc.log; : > "$LOG"
fail=0
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
die() { say "SNAPGC_FAIL: $*"; exit 1; }

VSIZE=${VSIZE:-256}
WARMUP=${WARMUP:-60000}     # 先写够，让分区形成、GC 进入稳定的连续状态
STALLED=${STALLED:-120000}  # 按住期间再写这么多，把受害者推到压缩点之前
HOLD=${HOLD:-40}
BUDGET_MB=${BUDGET_MB:-16}  # 预算调小，确保受害者真的落到压缩点之前
# GC 阈值与吸收比例都取小，让 GC 几乎一直在跑——这是本测的核心条件。
GCGB=${GCGB:-0.004}
ABSORB=${ABSORB:-0.05}
INSTALL_WAIT=${INSTALL_WAIT:-180}   # 放开之后最多等多久才算永久饿死
BIN=normal; [ "${RACE:-0}" = 1 ] && BIN=race

ALL=$(servers_str)
CLIENT_HOST=${CLIENT_HOST:-tikv240}
trap cleanup_nodes EXIT

say "拓扑 TOPO=${TOPO}：node0=$(host_of 0) node1=$(host_of 1) node2=$(host_of 2)"
say "条件：GC 阈值 ${GCGB}GB、absorbRatio ${ABSORB}（让 GC 几乎连续跑）、日志预算 ${BUDGET_MB}MB"

say "===== 0. 各机器上不能有别人的实验 ====="
for h in $(for i in 0 1 2; do host_of "$i"; done | sort -u); do
  own=$(rq "$h" "pgrep 'nezha' 2>/dev/null | head -5 | xargs -r ps -o user=,pid=,etime=,args= -p 2>/dev/null | cut -c1-120")
  [ -z "$own" ] || { echo "$own" | sed 's/^/    /'; die "$h 上有进程在跑，先停手（不是自己的就去问机器的其他使用者）"; }
  say "  $h 干净"
done
cleanup_nodes

say "===== 1. 拉起三节点（GC 开）====="
PEERSTR=$(peers_str)
for i in 0 1 2; do
  read -r P IP <<<"$(port_of "$i")"
  OUT=$(rq "$(host_of "$i")" "BIN=$BIN SYSTEM=nezha PEERS='$PEERSTR' \
    EXTRA='-raftLogBudgetMB $BUDGET_MB -gcThresholdGB $GCGB -absorbRatio $ABSORB -partitionTargetMB 2 -leaderCheck=false' \
    ~/three-node.sh start $i $P $IP $VSIZE $WARMUP" | tail -1)
  say "${OUT:-（无回执）}"
  # 不要写成 `require_out ... | tee ... || die`：`||` 作用在**整个管道**上，而管道的
  # 退出码是最后一条命令（tee）的，恒为 0，于是守卫永远不触发。我加 require_out 正是
  # 为了让静默失败变响，第一版却用一根管子把它废掉了（2026-09-17 实测：start node0
  # 没有回执、[闸门] 那行打出来了、脚本照样往下跑）。先赋值再判。
  w=$(require_out "$OUT" "start node$i"); rc=$?; [ -n "$w" ] && echo "$w" | tee -a "$LOG"
  [ $rc = 0 ] || die "start node$i 没有回执"
done
sleep 12

say "===== 2. 找出 leader，挑跨机的受害者 ====="
LEADER=-1; BEST=-1
for i in 0 1 2; do
  rep=$(rq "$(host_of "$i")" "~/three-node.sh report $i")
  won=$(gate_field "$rep" won); term=$(gate_field "$rep" term); alive=$(gate_field "$rep" alive)
  say "  node$i alive=$alive won=$won term=$term"
  [ "${won:-0}" -ge 1 ] 2>/dev/null || continue
  [ "$alive" = yes ] || continue
  [ "${term:-0}" -gt "$BEST" ] 2>/dev/null && { BEST=$term; LEADER=$i; }
done
[ "$LEADER" -ge 0 ] || die "没找到 leader"
VICTIM=-1
for i in 0 1 2; do
  [ "$i" = "$LEADER" ] && continue
  [ "$(host_of "$i")" != "$(host_of "$LEADER")" ] && { VICTIM=$i; break; }
done
[ "$VICTIM" -ge 0 ] || die "挑不出跨机的受害者"
say "  leader=node${LEADER}（$(host_of "$LEADER")）  受害者=node${VICTIM}（$(host_of "$VICTIM")）"

say "===== 3. 写 $WARMUP 条，让 GC 进入连续状态 ====="
W=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/randwrite_goroutine -cnums 16 -dnums $WARMUP -vsize $VSIZE -servers $ALL" | grep -oE 'elapse:[^,]*')
say "  $W"
sleep 20
GC0=$(gate_field "$(rq "$(host_of "$LEADER")" "~/three-node.sh report $LEADER")" gc_done)
say "  leader 已完成 ${GC0:-?} 轮 GC（这个数要在后面继续涨，否则 GC 不是连续的，本测前提不成立）"

say "===== 4. SIGSTOP node${VICTIM}，边写边让 GC 继续跑 ====="
say "$(rq "$(host_of "$VICTIM")" "~/three-node.sh pause $VICTIM")"
W=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/randwrite_goroutine -cnums 16 -dnums $STALLED -vsize $VSIZE -servers $(addr_of "$LEADER")" | grep -oE 'elapse:[^,]*')
say "  $W"
sleep "$HOLD"
M=$(rq "$(host_of "$LEADER")" "~/three-node.sh mem $LEADER"); say "  $M"
TRUNC=$(gate_field "$M" truncates)
[ "${TRUNC:-0}" -ge 1 ] 2>/dev/null || say "  [注意] 没发生截断，受害者可能还没落到压缩点之前，本测可能触发不到安装"

GC1=$(gate_field "$(rq "$(host_of "$LEADER")" "~/three-node.sh report $LEADER")" gc_done)
say "  GC 轮数 ${GC0:-?} → ${GC1:-?}（涨了说明 GC 在连续跑，前提成立）"
if [ "${GC1:-0}" -le "${GC0:-0}" ] 2>/dev/null; then
  say "  [注意] GC 轮数没涨，本测的核心条件（GC 几乎一直在跑）没达成，结论不成立"
  fail=1
fi

say "===== 5. SIGCONT，看安装被退回多少次、最终能不能装上 ====="
say "$(rq "$(host_of "$VICTIM")" "~/three-node.sh resume $VICTIM")"
INSTALLED=0; REFUSED=0
t0=$(date +%s)
while [ $(( $(date +%s) - t0 )) -lt "$INSTALL_WAIT" ]; do
  sleep 5
  MV=$(rq "$(host_of "$VICTIM")" "~/three-node.sh mem $VICTIM")
  [ "$(gate_field "$MV" snap_installed)" != 0 ] && { INSTALLED=1; break; }
  REFUSED=$(rq "$(host_of "$VICTIM")" "cat ~/work/three-$VICTIM/n*.log 2>/dev/null | grep -c 'GC 正在跑，本次安装退回'" | tr -d '\r')
  say "  等了 $(( $(date +%s) - t0 ))s：尚未装上，被 GC 退回 ${REFUSED:-0} 次"
done
REFUSED=$(rq "$(host_of "$VICTIM")" "cat ~/work/three-$VICTIM/n*.log 2>/dev/null | grep -c 'GC 正在跑，本次安装退回'" | tr -d '\r')
if [ "$INSTALLED" = 1 ]; then
  say "  [好] 最终装上了：被 GC 退回 ${REFUSED:-0} 次之后成功——拒绝式互斥没有饿死安装"
  rq "$(host_of "$VICTIM")" "~/three-node.sh snaplog $VICTIM 3" | sed 's/^/      /' | tee -a "$LOG"
else
  say "  [失效] ${INSTALL_WAIT}s 内始终没装上，被 GC 退回 ${REFUSED:-0} 次"
  say "         这就是要找的饿死：GC 一直忙 → 安装永远被退回 → 而日志已按预算截断、"
  say "         受害者也补不了日志，于是永久落后，而集群看起来正常（提交只等中位数）。"
  rq "$(host_of "$VICTIM")" "~/three-node.sh snaplog $VICTIM 5" | sed 's/^/      /' | tee -a "$LOG"
  fail=1
fi

say "===== 6. 正确性：先经 leader 写 key 派生的值，再直读受害者 ====="
V=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/scanverify -servers $ALL -leader $LEADER -dnums 3000 -vsize $VSIZE -span 50 -sample 20" | grep -vE 'new pool success')
echo "$V" | grep -E '校验|VERIFY' | sed 's/^/      /' | tee -a "$LOG"
echo "$V" | grep -q VERIFY_OK || die "经 leader 的校验未通过——本测前提不成立"
if [ "$INSTALLED" = 1 ]; then
  for _ in $(seq 1 24); do
    R=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/readonly -servers $(addr_of "$VICTIM") -dnums 3000 -vsize $VSIZE -check 300 -sample 30" | grep -vE 'new pool success')
    echo "$R" | grep -q FAILOVER_VERIFY_OK && { say "  受害者直读全对"; break; }
    sleep 5
  done
  why=$(gate_read_ok "${R:-}" "直读受害者 node$VICTIM") || fail=1
  [ -n "$why" ] && echo "$why" | tee -a "$LOG"
fi

say "===== 7. 三节点闸门 ====="
for i in 0 1 2; do
  rep=$(rq "$(host_of "$i")" "~/three-node.sh report $i"); say "  $rep"
  why=$(gate_report_ok "$rep" yes "node$i") || fail=1
  [ -n "$why" ] && echo "$why" | tee -a "$LOG"
done

echo | tee -a "$LOG"
{
echo "=============================================="
printf " leader=node%s  受害者=node%s（跨机）\n" "$LEADER" "$VICTIM"
printf " GC 轮数 %s → %s   截断 %s 次\n" "${GC0:-?}" "${GC1:-?}" "${TRUNC:-?}"
printf " 安装：%s   被 GC 退回 %s 次\n" "$([ "$INSTALLED" = 1 ] && echo 成功 || echo 失败)" "${REFUSED:-?}"
echo "=============================================="
} | tee -a "$LOG"
[ $fail = 0 ] && say "SNAPGC_OK" || say "SNAPGC_FAIL"
exit $fail
