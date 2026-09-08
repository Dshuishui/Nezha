#!/bin/bash
# 攒批窗口扫描：开 fsync 后，group commit 的窗口该取多大。
#
# 为什么要重测。上一轮只测了 0/200/1000μs 各一次，批大小 1.00/12.26/35.55 随窗口单调上升，
# 符合预期；但延迟是 13.59/**16.98**/12.38ms —— **200μs 那档比两边都高**。窗口变长只会让
# 单条多等一会儿、批变大，延迟不该先升后降。要么是噪声（PUT 实测波动 ±7%），要么那里真有
# 拐点，两种解释指向完全不同的选值。单点数据分辨不了，所以每档跑 ROUNDS 轮取中位数，
# 并向上补 2000/5000μs 找收益饱和的位置。
#
# 不搞清楚就直接打开 group commit，等于往后面所有实验里塞一个没弄明白的变量。
#
# 测什么：只测 PUT。窗口影响的是 Raft 日志的写入与 fsync，四个系统在这条路径上完全相同
# （Start() 里写的都是 key+value），所以跑一个系统就够。用 nezha-nogc：有 KV 分离、但不跑 GC，
# 把 GC 的抖动从写入延迟里摘掉——要选的是窗口，不是在测 GC。
#
# 用法: bash scripts/bench/groupcommit-sweep.sh [标签]
# 环境变量:
#   WINDOWS="0 200 500 1000 2000 5000"   窗口（微秒），0 = 关闭攒批
#   ROUNDS=3 ENTRIES=50000 VSIZE=256 CLIENTS=50
#   OUT=/tmp/gcommit-<标签>.csv
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cd "${REPO_DIR:-$(dirname "$0")/../..}" || die "无项目目录"
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

LABEL="${1:-$(date +%m%d-%H%M)}"
WINDOWS="${WINDOWS:-0 200 500 1000 2000 5000}"
ROUNDS="${ROUNDS:-3}"
ENTRIES="${ENTRIES:-50000}"
VSIZE="${VSIZE:-256}"
CLIENTS="${CLIENTS:-50}"
OUT="${OUT:-/tmp/gcommit-$LABEL.csv}"
ADDR=127.0.0.1:3088
IADDR=127.0.0.1:30881
BIN=/tmp/nezha-gcommit
COMMIT=$(git rev-parse --short HEAD)

info "构建 $COMMIT"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/gc-randwrite ./cmd/bench/randwrite_goroutine/ || die "randwrite 编译失败"

if [ ! -f "$OUT" ]; then
  echo "commit,label,window_us,round,entries,vsize,clients,n,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms,ops,elapsed_s,ops_per_s,avg_batch,max_batch,fsync_saved,handler_ms,commit_wait_ms" > "$OUT"
fi

field(){ local v; v=$(grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2); echo "${v:-NA}"; }

PID=""; DATA=""
cleanup(){ [ -n "$PID" ] && kill "$PID" 2>/dev/null; sleep 1; [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null; }
trap cleanup EXIT

total=0
for w in $WINDOWS; do for r in $(seq 1 "$ROUNDS"); do total=$((total+1)); done; done
info "共 $total 次：windows=[$WINDOWS] rounds=$ROUNDS entries=$ENTRIES vsize=${VSIZE}B clients=$CLIENTS"
info "输出 $OUT"

done_n=0
for w in $WINDOWS; do
  for round in $(seq 1 "$ROUNDS"); do
    done_n=$((done_n+1))
    DATA="$TMPDIR/gc-$LABEL-w$w-$round"
    info "[$done_n/$total] window=${w}us round=$round"
    rm -rf "$DATA"; mkdir -p "$DATA"

    # -syncWAL 始终开着：窗口只有在每条都要 fsync 时才有意义
    nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
        -data "$DATA" -gap 100000000 -commitTimeoutS 60 \
        -system nezha-nogc -syncWAL -groupCommitUs "$w" \
        < /dev/null > "$DATA/n.log" 2>&1 &
    PID=$!
    sleep 8
    kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点未启动 (window=$w)"; }

    /tmp/gc-randwrite -cnums "$CLIENTS" -dnums "$ENTRIES" -vsize "$VSIZE" -servers "$ADDR" > "$DATA/put.out" 2>&1
    L=$(grep '^\[LATENCY\]' "$DATA/put.out" | tail -1)
    T=$(grep '^\[THROUGHPUT\]' "$DATA/put.out" | tail -1)
    [ -n "$L" ] || { tail -20 "$DATA/put.out"; die "PUT 无分位数输出 (window=$w round=$round)" ; }

    # 节点侧的统计每 15 秒打一次，取最后一行；短跑可能一行都没有，那就等一次
    for _ in 1 2; do
      G=$(grep '^\[GROUP-COMMIT\]' "$DATA/n.log" | tail -1)
      P=$(grep '^\[PUT-BREAKDOWN\]' "$DATA/n.log" | tail -1)
      [ -n "$P" ] && break
      sleep 16
    done
    # 关闭攒批时没有 [GROUP-COMMIT] 行，那不是失败——avg_batch 记 1 才是事实
    if [ "$w" = 0 ] && [ -z "${G:-}" ]; then G="batches=NA entries=NA avg_batch=1 max_batch=1 fsync_saved=0"; fi
    [ -n "${P:-}" ] || warn "window=$w round=$round 没拿到 [PUT-BREAKDOWN]，该行相关列为 NA"

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$COMMIT" "$LABEL" "$w" "$round" "$ENTRIES" "$VSIZE" "$CLIENTS" \
      "$(field "$L" n)" "$(field "$L" mean)" "$(field "$L" p50)" "$(field "$L" p90)" \
      "$(field "$L" p95)" "$(field "$L" p99)" "$(field "$L" p999)" "$(field "$L" min)" "$(field "$L" max)" \
      "$(field "$T" ops)" "$(field "$T" elapsed)" "$(field "$T" ops_per_s)" \
      "$(field "${G:-}" avg_batch)" "$(field "${G:-}" max_batch)" "$(field "${G:-}" fsync_saved)" \
      "$(field "${P:-}" handler)" "$(field "${P:-}" S4_commit_wait)" >> "$OUT"

    kill "$PID" 2>/dev/null; sleep 2; kill -9 "$PID" 2>/dev/null; PID=""
    rm -rf "$DATA/data"   # 只留日志与客户端输出，省盘
  done
done

info "完成，$OUT"
python3 "$(dirname "$0")/groupcommit-report.py" "$OUT" | tee "${OUT%.csv}-report.txt"
