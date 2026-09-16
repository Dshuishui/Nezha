#!/bin/bash
# 跨机版：把一个 follower 按住不动，看 leader 的内存还会不会被它钉住。
#
# 与 scripts/test/slow-follower.sh **判据完全相同**，区别只有一个、但是关键的一个：
# 三个节点分布在两台实验机上，而被按住的副本与 leader **不在同一台机器**，所以复制、
# 快照传输、快照安装全程经过真实网络。单机版跑的是 loopback——快照做出来到装上只差
# 一次本机文件读写，网络这一段等于没测。到 2026-09-16 为止，快照路径**从未跨过网卡**。
#
# 拓扑由 gate.sh 的 TOPO 决定（two = node1/node2 同在 241；three = 三台各一个）。
# 两种拓扑下都总能挑到一个与 leader 不同机器的受害者（见 pick_victim）。
#
# 要看到的是两件事（和单机版一样）：
#   1. 被按住期间 leader 的内存日志**保持有界**（改造之前是基线的 30 倍）
#   2. 放开之后 leader 给它**发快照**，它装上并追平，数据仍然正确
#
# 用 SIGSTOP 而不是 kill：进程还在、端口还占着、TCP 还在，只是不干活——这正是
# CockroachDB 所说的 "not recently active" follower，也最贴近"某个节点明显更慢"。
set -u
cd "$(dirname "$0")"
# shellcheck source=scripts/multinode/gate.sh
. ./gate.sh

LOG=slow-follower-multi.log; : > "$LOG"
# r / rq / host_of / addr_of / peers_str / servers_str 全在 gate.sh，理由见那里。
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
die() { say "SLOW_MULTI_FAIL: $*"; stop_all; exit 1; }

# 规模：compactLog 的门槛是 20000 条、保留窗口 5000 条、每 10 秒检查一次，
# 所以"按住期间"必须写够 2 万条以上并跨过几个检查周期。数值与单机版一致，
# 那一组是实测能稳定触发快照路径的。
VSIZE=${VSIZE:-256}
WARMUP=${WARMUP:-40000}     # 三节点都健康时先写这么多，建立基线
STALLED=${STALLED:-150000}  # 按住一个 follower 期间再写这么多
HOLD=${HOLD:-40}            # 按住多少秒（至少跨 3 个 compactLog 检查周期）
# 预算调小到 16MB，让上限在这个规模上可观测：256B value 一条约 472B，16MB 约 3.5 万条。
# 默认的 256MB 在 15 万条 × 256B（约 71MB）下根本碰不到。
BUDGET_MB=${BUDGET_MB:-16}
# 默认 **normal** 而不是 race：本测的主判据之一是 RSS，而竞态检测器本身要吃掉好几倍
# 内存，会把这个数字变成不可解释的。要查竞态就 RACE=1 单独跑一轮，那一轮只看
# races= 字段、不看 RSS。
BIN=normal; [ "${RACE:-0}" = 1 ] && BIN=race

ALL=$(servers_str)
# 跑 bench 客户端的机器，与"哪个节点在哪台机器"无关，理由见 recover.sh 里同名变量。
CLIENT_HOST=${CLIENT_HOST:-tikv240}
# 被按住的进程一定要先 resume 再 stop：SIGSTOP 状态下的进程不处理 SIGTERM，只会攒着，
# 于是 stop 里那句 kill 什么也不做，直到后面的 kill -9 才收掉它。
stop_all() { local i; for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh resume $i" >/dev/null 2>&1 || true
                                        rq "$(host_of "$i")" "~/three-node.sh stop $i"   >/dev/null 2>&1 || true; done; }
trap stop_all EXIT

# mem_field <REPORT/MEM 行> <字段> —— 复用 gate.sh 的按 token 取值，理由见那里
mem_of() { r "$(host_of $1)" "~/three-node.sh mem $1"; }

say "拓扑 TOPO=${TOPO}：node0=$(host_of 0) node1=$(host_of 1) node2=$(host_of 2)；驱动跑在 $(hostname 2>/dev/null)（ON_SERVER=${ON_SERVER}）"
say "===== 0. 校验各机器上没有别人的实验在跑 ====="
for h in $(for i in 0 1 2; do host_of "$i"; done | sort -u); do
  own=$(r "$h" "pgrep 'nezha' 2>/dev/null | head -10 | xargs -r ps -o user=,pid=,etime=,args= -p 2>/dev/null | cut -c1-130")
  if [ -n "$own" ]; then
    say "  $h 上有进程在跑，本脚本会清空 ~/work/three-* 并抢占端口，先停手："
    echo "$own" | sed 's/^/      /' | tee -a "$LOG"
    say "  是自己的：等它跑完或手动停掉。**不是自己的（属主不是你）：不要停，先问机器的其他使用者。**"
    exit 1
  fi
  say "  $h 干净"
done

say "===== 1. 拉起三节点（bin=${BIN}，GC 关掉，日志预算 ${BUDGET_MB}MB）====="
# GC 关掉（-system nezha-nogc）：本测只看 Raft 日志的内存，不要让 GC 的搬运掺进来。
for i in 0 1 2; do
  read -r P IP <<<"$(port_of $i)"
  OUT=$(rq "$(host_of "$i")" "BIN=$BIN SYSTEM=nezha-nogc PEERS='$(peers_str)' EXTRA='-raftLogBudgetMB $BUDGET_MB -leaderCheck=false' ~/three-node.sh start $i $P $IP $VSIZE $WARMUP" | tail -1)
  say "${OUT:-（无回执）}"
  require_out "$OUT" "start node$i" | tee -a "$LOG" || die "start node$i 没有回执"
done
[ "$(grep -c 'STARTED node' "$LOG")" = 3 ] || die "有节点没起来"
sleep 12

say "===== 2. 找出 leader ====="
# 当前角色不能靠"最后一次角色转换"判断：代码里只打 `Follower -> Candidate` 和
# `Candidate -> Leader`，**没有** `Leader -> Follower`，所以一个已经下台的 leader
# 日志里最后一条仍然是它当选那次。所以在"赢过"的节点里取 term 最大的那个。
LEADER=-1; BEST=-1
for i in 0 1 2; do
  rep=$(r "$(host_of $i)" "~/three-node.sh report $i")
  won=$(gate_field "$rep" won); term=$(gate_field "$rep" term); alive=$(gate_field "$rep" alive)
  say "  node$i alive=$alive won=$won term=$term"
  [ "${won:-0}" -ge 1 ] 2>/dev/null || continue
  [ "$alive" = yes ] || continue
  [ "${term:-0}" -gt "$BEST" ] 2>/dev/null && { BEST=$term; LEADER=$i; }
done
[ "$LEADER" -ge 0 ] || die "没找到 leader"
say "  leader = node${LEADER}（$(host_of $LEADER)，term=${BEST}）"

# 受害者必须在**另一台机器**上——这就是这个脚本存在的全部理由。
# 两种拓扑下 node0 都独占 tikv240，所以 leader 无论落在哪个节点都挑得出来。
pick_victim() {
  local i lh; lh=$(host_of "$LEADER")
  for i in 0 1 2; do
    [ "$i" = "$LEADER" ] && continue
    [ "$(host_of $i)" != "$lh" ] && { echo "$i"; return 0; }
  done
  return 1
}
VICTIM=$(pick_victim) || die "挑不出跨机的受害者（拓扑变了？）"
say "  受害者 = node${VICTIM}（$(host_of "$VICTIM")）—— 与 leader 跨机，复制和快照都要过网卡"

say "===== 3. 阶段 1：三节点健康，写 $WARMUP × ${VSIZE}B，建立基线 ====="
W=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/randwrite_goroutine -cnums 16 -dnums $WARMUP -vsize $VSIZE -servers $ALL" | grep -oE 'elapse:[^,]*')
say "  $W"
sleep 25   # 跨过两个 compactLog 检查周期
M=$(mem_of "$LEADER"); say "  $M"
RSS_BASE=$(gate_field "$M" rss_kb); RET_BASE=$(gate_field "$M" retained)
[ "$RET_BASE" != none ] && [ -n "$RET_BASE" ] || die "基线拿不到压缩后的保留条数，判据无法成立"
say "  基线：leader RSS = $((RSS_BASE/1024)) MB，最近一次压缩后保留 $RET_BASE 条"

say "===== 4. 阶段 2：SIGSTOP node${VICTIM}，再写 $STALLED 条 ====="
say "$(r "$(host_of "$VICTIM")" "~/three-node.sh pause $VICTIM")"
grep -q "PAUSED node$VICTIM" "$LOG" || die "SIGSTOP 没成功"
# 只打 leader：被按住的那个已经不回话，把它列进 -servers 只会让客户端在它身上超时。
W=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/randwrite_goroutine -cnums 16 -dnums $STALLED -vsize $VSIZE -servers $(addr_of "$LEADER")" | grep -oE 'elapse:[^,]*')
say "  $W"
say "  再按住 ${HOLD}s，让 compactLog 跑几轮"
sleep "$HOLD"
M=$(mem_of "$LEADER"); say "  $M"
RSS_PIN=$(gate_field "$M" rss_kb); RET_PIN=$(gate_field "$M" retained)
TRUNC=$(gate_field "$M" truncates); PINNED=$(gate_field "$M" pinned)
r "$(host_of "$LEADER")" "~/three-node.sh snaplog $LEADER 4" | sed 's/^/      /' | tee -a "$LOG"
[ "$RET_PIN" != none ] && [ -n "$RET_PIN" ] || die "按住期间拿不到保留条数，判据无法成立"

# **核心判据**：内存日志不再随落后程度增长。
# 改造之前这里是基线的 30 倍（5000 → 150000）；现在被按住的副本越过活跃窗口就落入
# 第二档，压缩照常进行，所以保留条数应当与基线同量级。留 3 倍余量给"刚好跨在窗口
# 边上"的时序。
LIMIT=$((RET_BASE * 3))
if [ "$RET_PIN" -le "$LIMIT" ]; then
  say "  [好] 内存有界：按住期间保留 $RET_PIN 条（基线 ${RET_BASE}，上限判据 ${LIMIT}）"
  say "       改造之前这里是 150000 条，换算到 100GB/64B 落后 10% 约 30GB → OOM"
else
  die "内存仍随落后程度增长：保留 $RET_PIN 条，是基线 $RET_BASE 的 $((RET_PIN/RET_BASE)) 倍"
fi

say "===== 5. 阶段 3：SIGCONT node${VICTIM}，看它能否靠跨机快照追上 ====="
say "$(r "$(host_of "$VICTIM")" "~/three-node.sh resume $VICTIM")"
SENT=0
for _ in $(seq 1 45); do
  sleep 2
  M=$(mem_of "$LEADER")
  [ "$(gate_field "$M" snap_sent)" != 0 ] && { SENT=1; break; }
done
if [ "$SENT" = 1 ]; then
  r "$(host_of "$LEADER")" "~/three-node.sh snaplog $LEADER 3" | sed 's/^/      [发送端] /' | tee -a "$LOG"
  INST=0
  for _ in $(seq 1 60); do
    sleep 2
    MV=$(mem_of "$VICTIM")
    [ "$(gate_field "$MV" snap_installed)" != 0 ] && { INST=1; break; }
  done
  if [ "$INST" = 1 ]; then
    r "$(host_of "$VICTIM")" "~/three-node.sh snaplog $VICTIM 3" | sed 's/^/      [接收端] /' | tee -a "$LOG"
    say "  [好] 跨机快照补齐成功——这是单机版覆盖不到的那一段"
  else
    die "leader 发了快照但对端没装上（跨机传输或安装失败）"
  fi
else
  # 它也可能在落到压缩点之前就追上了（按住期间写入不多、或压缩恰好没跨过它）。
  # 那不是失败，但必须说清楚这一轮**没有覆盖到快照路径**——否则读报告的人会以为覆盖了。
  say "  [注意] 本轮没有触发快照：那个副本在落到压缩点之前就追上了"
  say "         要稳定覆盖快照路径，调大 STALLED 或调小 BUDGET_MB"
fi
M=$(mem_of "$LEADER"); RSS_END=$(gate_field "$M" rss_kb); RET_END=$(gate_field "$M" retained)
say "  恢复之后：$M"

say "===== 6. 正确性：数据必须是对的 ====="
# 先等受害者追平再直读它——刚装完快照的节点还要接上后续的普通复制。
for _ in $(seq 1 30); do
  R=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/readonly -servers $(addr_of "$VICTIM") -dnums 2000 -vsize $VSIZE -check 300 -sample 30" | grep -vE 'new pool success')
  echo "$R" | grep -q FAILOVER_VERIFY_OK && { say "  受害者直读全对"; break; }
  sleep 5
done
echo "${R:-}" | grep -q FAILOVER_VERIFY_OK || say "  [注意] 受害者在 150s 内没读全对（下面的 leader 校验仍然要过）"
V=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/scanverify -servers $ALL -leader $LEADER -dnums 3000 -vsize $VSIZE -span 50 -sample 20" | grep -vE 'new pool success')
echo "$V" | grep -E '校验|VERIFY' | sed 's/^/      /' | tee -a "$LOG"
echo "$V" | grep -q VERIFY_OK || die "数据校验未通过——本测的前提不成立"

say "===== 7. 三节点闸门 ====="
fail=0
for i in 0 1 2; do
  rep=$(r "$(host_of $i)" "~/three-node.sh report $i"); say "  $rep"
  why=$(gate_report_ok "$rep" yes "node$i") || fail=1
  [ -n "$why" ] && echo "$why" | tee -a "$LOG"
done

echo | tee -a "$LOG"
{
echo "=============================================="
printf " leader = node%s (%s)   受害者 = node%s (%s)\n" "$LEADER" "$(host_of "$LEADER")" "$VICTIM" "$(host_of "$VICTIM")"
printf " 内存日志保留条数   基线 %-8s → 按住 %-8s → 恢复后 %s\n" "$RET_BASE" "$RET_PIN" "${RET_END:-?}"
printf " leader RSS (MB)    基线 %-8s → 按住 %-8s → 恢复后 %s\n" "$((RSS_BASE/1024))" "$((RSS_PIN/1024))" "$((RSS_END/1024))"
printf " [LOG-TRUNCATE] %-3s 次   [LOG-PINNED] %-3s 次   发出快照 %s\n" "$TRUNC" "$PINNED" "$SENT"
echo "=============================================="
} | tee -a "$LOG"
[ $fail = 0 ] && say "SLOW_MULTI_OK" || say "SLOW_MULTI_FAIL（闸门不合格）"
exit $fail
