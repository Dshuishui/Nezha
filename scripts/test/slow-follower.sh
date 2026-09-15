#!/bin/bash
# 三节点：把一个 follower 按住不动，看 leader 的内存被它钉住多少。
#
# 为什么要有这个脚本：`compactLog` 的压缩上界被夹到 `min(matchIndex)`，理由是正当的
# ——截掉某个 follower 还没复制的条目，它就永久追不上（没有 InstallSnapshot）。
# 代价是 leader 内存里的 `rf.log` 随最慢 follower 的落后程度线性增长。
# 这件事此前只是算出来的（100GB/64B 落后 10% ≈ 30GB），从没在真进程上量过。
#
# 这里在小规模上把它量出来，作为 raft-snapshot 那三步改造的**回归判据**：
# 改造之后同样的脚本应当看到"内存不再随落后程度无界增长"。
#
# 用 SIGSTOP 而不是 kill：进程还在、端口还占着、TCP 还在，只是不干活——
# 这正是 CockroachDB 所说的 "not recently active" follower，也是最贴近
# "一个节点明显更慢"的模拟方式。
#
# 顺带验证一件互斥关系：失效 A（内存被钉住）与失效 B（落后超过压缩点后永久卡死）
# **现在不可能同时发生**——正是导致 A 的那个夹紧阻止了 B。所以本脚本期望
# 看到 [LOG-PINNED] 而**不该**看到 [LOG-STUCK]；只有把截断改成有界之后 B 才可达。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[SLOW]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; cleanup; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || exit 1
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && { echo "librocksdb.so 未找到"; exit 1; }
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

# 规模：compactLog 的门槛是 20000 条、保留窗口 5000 条、每 10 秒检查一次，
# 所以"按住期间"必须写够 2 万条以上并跨过几个检查周期。
VSIZE=${VSIZE:-256}
WARMUP=${WARMUP:-40000}     # 三节点都健康时先写这么多，建立基线
STALLED=${STALLED:-150000}  # 按住一个 follower 期间再写这么多
HOLD=${HOLD:-40}            # 按住多少秒（至少跨 3 个 compactLog 检查周期）
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
go build -o /tmp/sf-node ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/sf-write ./cmd/bench/randwrite_goroutine/ || die "写入工具编译失败"
go build -o /tmp/sf-verify ./cmd/bench/scanverify/ || die "scanverify 编译失败"

PEERS="127.0.0.1:$I0,127.0.0.1:$((I0+1)),127.0.0.1:$((I0+2))"
SERVERS="127.0.0.1:$P0,127.0.0.1:$((P0+1)),127.0.0.1:$((P0+2))"
pkill -f sf-node 2>/dev/null; sleep 2

info "起三个节点（GC 关掉：本测只看 Raft 日志的内存，不要让 GC 掺进来）"
for i in 0 1 2; do
    d=$(mktemp -d); DIRS+=("$d")
    /tmp/sf-node -address "127.0.0.1:$((P0+i))" -internalAddress "127.0.0.1:$((I0+i))" \
        -peers "$PEERS" -data "$d" -gap 100000000 -system nezha-nogc \
        -commitTimeoutS 60 > "$d/n.log" 2>&1 &
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
info "leader = node$((LEADER+1))（pid $LPID）"

rss(){ local v; v=$(ps -o rss= -p "${1:-0}" 2>/dev/null | tr -d ' '); echo "${v:-0}"; }
# 驻留条数取自 [LOG-PINNED] 打出的 len(rf.log)。
# 不要用 `compactLog: A -> B 条` 那行：被按住期间根本不发生压缩，那行是**上一次成功压缩**
# 的结果，是个陈旧值——第一版就这么写的，把 37177（停顿前那次压缩的结果）当成了停顿期间
# 的驻留量，而 [LOG-PINNED] 同时报着 145000。
retained(){ grep -o "内存日志驻留 [0-9]* 条" "$1" | tail -1 | grep -oE "[0-9]+"; }
# compacted 取最近一次成功压缩之后的条数，用来判断"恢复之后是否回落"
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
info "阶段 2：SIGSTOP node$((VICTIM+1))（pid $VPID），然后写 $STALLED 条"
kill -STOP "$VPID" || die "SIGSTOP 失败"
/tmp/sf-write -cnums 16 -dnums "$STALLED" -vsize "$VSIZE" -servers "127.0.0.1:$((P0+LEADER))" 2>&1 \
    | grep -o "elapse:[^,]*" | sed 's/^/       /'
info "按住 $HOLD 秒，让 compactLog 跑几轮"
for _ in $(seq 1 $((HOLD/5))); do sleep 5; done
RSS_PIN=$(rss "$LPID"); RET_PIN=$(retained "${DIRS[$LEADER]}/n.log")
PINNED=$(count_in "${DIRS[$LEADER]}/n.log" "LOG-PINNED")
STUCK=$(count_in "${DIRS[$LEADER]}/n.log" "LOG-STUCK")
echo "       被钉住期间：leader RSS = $((RSS_PIN/1024)) MB，保留 ${RET_PIN:-?} 条"
grep -o "\[LOG-PINNED\].*" "${DIRS[$LEADER]}/n.log" | tail -1 | cut -c1-150 | sed 's/^/       /'

if [ "$PINNED" -ge 1 ]; then
    good "[LOG-PINNED] 报了 $PINNED 次——压缩点确实被按住了，而且现在看得见"
else
    warn "没有 [LOG-PINNED]：可能落后条数没到 5 万的报警门槛（调大 STALLED 再试）"
fi
if [ "$STUCK" -eq 0 ]; then
    good "没有 [LOG-STUCK]，符合预期：导致 A 的那个夹紧同时阻止了 B，两者互斥"
else
    die "出现了 [LOG-STUCK] $STUCK 次——压缩竟然越过了 follower，这与当前代码的约束矛盾"
fi

# ---------- 阶段 3：放开，看能不能追上、内存能不能回落 ----------
info "阶段 3：SIGCONT node$((VICTIM+1))，看它追上之后内存是否回落"
kill -CONT "$VPID" || die "SIGCONT 失败"
for _ in $(seq 1 24); do
    sleep 5
    r=$(compacted "${DIRS[$LEADER]}/n.log")
    [ -n "$r" ] && [ "$r" -le $((${RET_BASE:-100000} * 3)) ] && break
done
RSS_END=$(rss "$LPID"); RET_END=$(compacted "${DIRS[$LEADER]}/n.log")
echo "       恢复之后：leader RSS = $((RSS_END/1024)) MB，保留 ${RET_END:-?} 条"

# ---------- 正确性：全程 leader 上的数据必须是对的 ----------
info "校验 leader 上的数据（$STALLED 条）"
OUT=$(/tmp/sf-verify -servers "127.0.0.1:$((P0+LEADER))" -dnums 3000 -vsize "$VSIZE" 2>&1)
echo "$OUT" | grep -E '校验|VERIFY' | sed 's/^/       /'
grep -q VERIFY_OK <<<"$OUT" || die "leader 上的数据校验未通过——本测的前提不成立"

echo
echo "=============================================="
printf " leader 内存日志保留条数  基线 %-8s → 被钉住 %-8s → 恢复后 %s\n" "${RET_BASE:-?}" "${RET_PIN:-?}" "${RET_END:-?}"
printf " leader RSS (MB)          基线 %-8s → 被钉住 %-8s → 恢复后 %s\n" "$((RSS_BASE/1024))" "$((RSS_PIN/1024))" "$((RSS_END/1024))"
echo "=============================================="
if [ -n "${RET_PIN:-}" ] && [ -n "${RET_BASE:-}" ] && [ "$RET_PIN" -gt $((RET_BASE*2)) ]; then
    good "复现成功：一个 follower 停了 ${HOLD}s，leader 的内存日志就涨到基线的 $((RET_PIN/RET_BASE)) 倍"
    echo "     这就是 100GB 规模上 OOM 的机理。修法见 docs/raft-snapshot.md 的三步。"
else
    warn "保留条数没有明显增长——检查 STALLED / HOLD 是否够大，或 compactLog 是否根本没触发"
fi
info "清理数据目录与进程"
