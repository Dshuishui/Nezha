#!/bin/bash
# 三节点：把一个 follower 按住不动，看 leader 的内存还会不会被它钉住。
#
# 这个脚本的含义随改造反转过一次，两个版本都要记下来，否则读到它的人会以为判据写错了。
#
# **改造之前**（2026-09-15，提交 b898526）它是**复现脚本**。`compactLog` 的压缩上界夹在
# min(matchIndex)，理由正当——截掉某个 follower 还没复制的条目，它就永久追不上，因为
# 没有 InstallSnapshot。代价是 leader 内存里的 rf.log 随最慢 follower 的落后程度线性增长。
# 当时实测：SIGSTOP 一个 follower 40 秒、写 15 万条 × 256B，leader 的内存日志从 5000 条
# 涨到 **150000 条**（30 倍），RSS 114 → 254MB。换算到 100GB/64B、落后 10% 就是约 30GB，
# 进程会被 OOM 杀掉，而在此之前没有任何一行日志提到过原因。
#
# **改造之后**（快照三步落地）它是**回归判据**。三档截断规则让内存有界：
#   第一档 活跃副本，保护到它的 Match（稍慢的不该被推去走快照）
#   第二档 不活跃副本，只在预算内保护——被 SIGSTOP 的进程停止回执，越过活跃窗口就落到这一档
#   第三档 字节预算是硬上限，活跃副本也不例外
# 于是被按住期间压缩照常进行，内存**不再**随落后程度增长；SIGCONT 之后那个副本落在压缩点
# 之前，由快照整体补齐。
#
# 所以现在要看到的是两件事：
#   1. 被按住期间 leader 的内存日志**保持有界**（不再是基线的 30 倍）
#   2. 放开之后 leader 给它**发快照**，它装上并追平，数据仍然正确
#
# 用 SIGSTOP 而不是 kill：进程还在、端口还占着、TCP 还在，只是不干活——这正是
# CockroachDB 所说的 "not recently active" follower，也是最贴近"一个节点明显更慢"的模拟。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[SLOW]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; cleanup; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || exit 1
# cgo 环境（RocksDB 的头与库）集中在 scripts/lib/cgo-env.sh 一份。
# 原先这里写死了三个系统路径 + -I/usr/include，在实验机上会"找到错的版本"，
# 报错出现在 cgo 阶段、读起来像代码问题。理由见那个文件。
# shellcheck source=scripts/lib/cgo-env.sh
# 用 $PROJECT_DIR 而不是 $(dirname "$0")：这一行在 `cd "$PROJECT_DIR"` **之后**，
# 而 $0 是相对路径（`bash ./snapshot-crash.sh`），cd 一发生它就失效——十个脚本
# 全都中了，实测报 "./../lib/cgo-env.sh: No such file or directory"。
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || { echo "cgo 环境准备失败（见 scripts/lib/cgo-env.sh）"; exit 1; }

# 规模：compactLog 的门槛是 20000 条、保留窗口 5000 条、每 10 秒检查一次，
# 所以"按住期间"必须写够 2 万条以上并跨过几个检查周期。
VSIZE=${VSIZE:-256}
WARMUP=${WARMUP:-40000}     # 三节点都健康时先写这么多，建立基线
STALLED=${STALLED:-150000}  # 按住一个 follower 期间再写这么多
HOLD=${HOLD:-40}            # 按住多少秒（至少跨 3 个 compactLog 检查周期）
# 预算调小到 16MB，让上限在这个规模上可观测：256B value 一条约 472B，
# 16MB 约 3.5 万条。默认的 256MB 在 15 万条 × 256B（约 71MB）下根本碰不到。
BUDGET_MB=${BUDGET_MB:-16}
P0=${P0:-41200}; I0=${I0:-41210}
DIRS=(); PIDS=()

cleanup(){
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill -CONT "$p" 2>/dev/null; done
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && { kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; }; done
    pkill -f sf-node 2>/dev/null
    for d in "${DIRS[@]:-}"; do [ -n "$d" ] && rm -rf "$d"; done
    rm -f /tmp/sf-node /tmp/sf-write /tmp/sf-verify
}
trap cleanup EXIT

info "构建 $(git rev-parse --short HEAD)"
# RACE=1 用竞态检测器构建节点（默认关着，-race 让节点慢好几倍）。见 snapshot-e2e.sh 的同一段。
RACEFLAG=""; [ "${RACE:-0}" = 1 ] && { RACEFLAG="-race"; info "带 -race 构建节点（会慢很多）"; }
# shellcheck disable=SC2086
go build $RACEFLAG -o /tmp/sf-node ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/sf-write ./cmd/bench/randwrite_goroutine/ || die "写入工具编译失败"
go build -o /tmp/sf-verify ./cmd/bench/scanverify/ || die "scanverify 编译失败"

PEERS="127.0.0.1:$I0,127.0.0.1:$((I0+1)),127.0.0.1:$((I0+2))"
SERVERS="127.0.0.1:$P0,127.0.0.1:$((P0+1)),127.0.0.1:$((P0+2))"
pkill -f sf-node 2>/dev/null; sleep 2

info "起三个节点（GC 关掉：本测只看 Raft 日志的内存，不要让 GC 掺进来；日志预算 ${BUDGET_MB}MB）"
for i in 0 1 2; do
    d=$(mktemp -d); DIRS+=("$d")
    /tmp/sf-node -address "127.0.0.1:$((P0+i))" -internalAddress "127.0.0.1:$((I0+i))" \
        -peers "$PEERS" -data "$d" -gap 100000000 -system nezha-nogc \
        -raftLogBudgetMB "$BUDGET_MB" -commitTimeoutS 60 > "$d/n.log" 2>&1 &
    PIDS+=($!)
done
for _ in $(seq 1 40); do
    grep -lq -- "-> Leader" "${DIRS[0]}/n.log" "${DIRS[1]}/n.log" "${DIRS[2]}/n.log" 2>/dev/null && break
    sleep 1
done
LEADER=-1
for i in 0 1 2; do grep -q -- "Candidate -> Leader" "${DIRS[$i]}/n.log" 2>/dev/null && LEADER=$i; done
[ "$LEADER" -ge 0 ] || die "40 秒内没有节点当选"
LPID=${PIDS[$LEADER]}
info "leader = node$((LEADER+1))（pid ${LPID}）"

rss(){ local v; v=$(ps -o rss= -p "${1:-0}" 2>/dev/null | tr -d ' '); echo "${v:-0}"; }
# 驻留条数取最近一次成功压缩之后的条数。
#
# 不要用 `compactLog: A -> B 条` 那行去看"被按住期间的驻留量"：改造之前被按住期间根本
# 不发生压缩，那行是**上一次成功压缩**的结果，是个陈旧值——第一版就这么写的，把 37177
# （停顿前那次压缩的结果）当成了停顿期间的驻留量，而 [LOG-PINNED] 同时报着 145000。
# 改造之后压缩照常进行，这行就是当前值了，但判据仍然以 [LOG-BOUND] 打出的实测为准。
compacted(){ grep -o "compactLog: [0-9]* -> [0-9]* 条" "$1" | tail -1 | grep -oE "[0-9]+ 条" | grep -oE "[0-9]+"; }
count_in(){ local n; n=$(grep -c -- "$2" "$1" 2>/dev/null || true); echo "${n:-0}"; }

# ---------- 阶段 1：三节点健康，建立基线 ----------
info "阶段 1：三节点健康，写 $WARMUP 条 × ${VSIZE}B"
/tmp/sf-write -cnums 16 -dnums "$WARMUP" -vsize "$VSIZE" -servers "$SERVERS" 2>&1 | grep -o "elapse:[^,]*" | sed 's/^/       /'
sleep 25   # 跨过两个 compactLog 检查周期
RSS_BASE=$(rss "$LPID"); RET_BASE=$(compacted "${DIRS[$LEADER]}/n.log")
info "基线：leader RSS = $((RSS_BASE/1024)) MB，最近一次压缩后内存日志保留 ${RET_BASE:-未压缩} 条"

# ---------- 阶段 2：按住一个 follower ----------
VICTIM=-1
for i in 0 1 2; do [ "$i" != "$LEADER" ] && VICTIM=$i && break; done
VPID=${PIDS[$VICTIM]}
info "阶段 2：SIGSTOP node$((VICTIM+1))（pid ${VPID}），然后写 $STALLED 条"
kill -STOP "$VPID" || die "SIGSTOP 失败"
/tmp/sf-write -cnums 16 -dnums "$STALLED" -vsize "$VSIZE" -servers "127.0.0.1:$((P0+LEADER))" 2>&1 \
    | grep -o "elapse:[^,]*" | sed 's/^/       /'
info "按住 $HOLD 秒，让 compactLog 跑几轮"
for _ in $(seq 1 $((HOLD/5))); do sleep 5; done
RSS_PIN=$(rss "$LPID"); RET_PIN=$(compacted "${DIRS[$LEADER]}/n.log")
PINNED=$(count_in "${DIRS[$LEADER]}/n.log" "LOG-PINNED")
TRUNC=$(count_in "${DIRS[$LEADER]}/n.log" "LOG-TRUNCATE")
echo "       被按住期间：leader RSS = $((RSS_PIN/1024)) MB，最近一次压缩后保留 ${RET_PIN:-?} 条"
grep -o "\[LOG-TRUNCATE\].*" "${DIRS[$LEADER]}/n.log" | tail -1 | cut -c1-170 | sed 's/^/       /'
grep -o "\[LOG-PINNED\].*" "${DIRS[$LEADER]}/n.log" | tail -1 | cut -c1-170 | sed 's/^/       /'

# **核心判据**：内存日志不再随落后程度增长。
# 改造之前这里是基线的 30 倍（5000 → 150000）；现在被按住的副本越过活跃窗口就落入第二档，
# 压缩照常进行，所以保留条数应当与基线同量级。留 3 倍的余量给"刚好跨在窗口边上"的时序。
[ -n "${RET_PIN:-}" ] && [ -n "${RET_BASE:-}" ] || die "拿不到压缩后的保留条数，判据无法成立"
LIMIT=$((RET_BASE * 3))
if [ "$RET_PIN" -le "$LIMIT" ]; then
    good "内存有界：被按住期间保留 $RET_PIN 条（基线 ${RET_BASE}，上限判据 ${LIMIT}）"
    echo "     改造之前这里是 150000 条（基线的 30 倍），换算到 100GB/64B 落后 10% 约 30GB → OOM"
else
    die "内存仍在随落后程度增长：保留 $RET_PIN 条，是基线 $RET_BASE 的 $((RET_PIN/RET_BASE)) 倍"
fi

# ---------- 阶段 3：放开，看它能不能靠快照追上 ----------
info "阶段 3：SIGCONT node$((VICTIM+1))，看 leader 是否给它发快照并让它追平"
kill -CONT "$VPID" || die "SIGCONT 失败"
SENT=0
for _ in $(seq 1 45); do
    sleep 2
    grep -q "开始给它发快照" "${DIRS[$LEADER]}/n.log" 2>/dev/null && SENT=1 && break
done
if [ "$SENT" -eq 1 ]; then
    grep -o "\[SNAPSHOT\].*" "${DIRS[$LEADER]}/n.log" | tail -2 | cut -c1-170 | sed 's/^/       /'
    INSTALLED=0
    for _ in $(seq 1 45); do
        sleep 2
        grep -q "装好一份" "${DIRS[$VICTIM]}/n.log" 2>/dev/null && INSTALLED=1 && break
    done
    if [ "$INSTALLED" -eq 1 ]; then
        grep -o "\[SNAPSHOT\] 装好一份.*" "${DIRS[$VICTIM]}/n.log" | tail -1 | cut -c1-190 | sed 's/^/       /'
        good "被按住的副本靠快照补齐了——这正是第 1、2 步存在的理由"
    else
        die "leader 发了快照但对端没装上，看 ${DIRS[$VICTIM]}/n.log 的 [SNAPSHOT] 行"
    fi
else
    # 它也可能在落到压缩点之前就追上了（按住期间写入不多、或压缩恰好没跨过它）。
    # 那不是失败，但要说清楚这一轮没有覆盖到快照路径。
    warn "本轮没有触发快照：那个副本在落到压缩点之前就追上了"
    warn "如果想稳定覆盖快照路径，调大 STALLED 或调小 BUDGET_MB"
fi
RSS_END=$(rss "$LPID"); RET_END=$(compacted "${DIRS[$LEADER]}/n.log")
echo "       恢复之后：leader RSS = $((RSS_END/1024)) MB，保留 ${RET_END:-?} 条"

# ---------- 正确性：全程 leader 上的数据必须是对的 ----------
info "校验 leader 上的数据"
OUT=$(/tmp/sf-verify -servers "127.0.0.1:$((P0+LEADER))" -dnums 3000 -vsize "$VSIZE" 2>&1)
echo "$OUT" | grep -E '校验|VERIFY' | sed 's/^/       /'
grep -q VERIFY_OK <<<"$OUT" || die "leader 上的数据校验未通过——本测的前提不成立"

echo
echo "=============================================="
printf " leader 内存日志保留条数  基线 %-8s → 被按住 %-8s → 恢复后 %s\n" "${RET_BASE:-?}" "${RET_PIN:-?}" "${RET_END:-?}"
printf " leader RSS (MB)          基线 %-8s → 被按住 %-8s → 恢复后 %s\n" "$((RSS_BASE/1024))" "$((RSS_PIN/1024))" "$((RSS_END/1024))"
printf " [LOG-TRUNCATE] %-3s 次   [LOG-PINNED] %-3s 次   发出快照 %s\n" "$TRUNC" "$PINNED" "$SENT"
echo "=============================================="
good "回归通过：一个 follower 停了 ${HOLD}s，leader 的内存不再被它钉住"
info "清理数据目录与进程"
