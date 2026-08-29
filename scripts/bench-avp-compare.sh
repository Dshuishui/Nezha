#!/bin/bash
# AVP 分支 vs 原版 Nezha 的完整对比：内存 + PUT/GET/SCAN 性能。
#
# 关键点：三项改进（raft 日志压缩、有界内联缓存、稀疏块索引）中，前两项只影响内存，
# 稀疏索引则用「每次点查多读一个块」换取索引内存下降，这个代价必须实测。
# 因此本脚本把 GC 阈值压低，确保 sortedFile 与稀疏索引真正被建立并走到读路径上。
#
# 用法: bash scripts/bench-avp-compare.sh <标签> [写入量] [value大小] [缓存MB] [块KB]
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
warn(){ echo -e "${YEL}[WARN]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && fail "librocksdb.so 未找到"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

LABEL="${1:?用法: $0 <标签> [写入量] [vsize] [cacheMB] [blockKB]}"
N="${2:-500000}"; VSIZE="${3:-64}"; CACHE_MB="${4:-64}"; BLOCK_KB="${5:-64}"
GC_GB="${GC_GB:-0.05}"
COMMIT=$(git rev-parse --short HEAD)
CSV="/tmp/avpcmp_${LABEL}.csv"

# 旧版本没有这些 flag，探测后按需拼接，使同一脚本能跑两个分支
EXTRA=""
HELP=$(go run ./kvstore/FlexSync/ -h 2>&1 || true)
grep -q "inlineCacheMB" <<<"$HELP" && EXTRA="$EXTRA -inlineCacheMB $CACHE_MB"
grep -q "indexBlockKB"  <<<"$HELP" && EXTRA="$EXTRA -indexBlockKB $BLOCK_KB"
if grep -q "gcThresholdGB" <<<"$HELP"; then
    EXTRA="$EXTRA -gcThresholdGB $GC_GB"
else
    warn "该版本无 -gcThresholdGB，GC 阈值固定 4000GB，本轮不会触发 GC"
fi
info "版本 $COMMIT 支持的额外参数:${EXTRA:- (无)}"

info "构建..."
go build -o "/tmp/nezha-cmp-$LABEL" ./kvstore/FlexSync/ || fail "编译失败"

D=$(mktemp -d)
cleanup(){ kill $PID 2>/dev/null; rm -rf "$D" "/tmp/nezha-cmp-$LABEL"; }
trap cleanup EXIT

# shellcheck disable=SC2086
"/tmp/nezha-cmp-$LABEL" -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 -data "$D" -gap 1000000 $EXTRA > "$D/n.log" 2>&1 &
PID=$!
sleep 10
kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; fail "节点未启动"; }

rss(){ ps -o rss= -p $PID 2>/dev/null | tr -d ' ' || echo 0; }
( while kill -0 $PID 2>/dev/null; do rss; sleep 3; done ) > "$D/rss.txt" &
SAMPLER=$!

info "[$LABEL] PUT $N 条 (value=${VSIZE}B)..."
PUT=$(go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
    -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 2>&1 | grep elapse:) \
    || fail "PUT 失败"
echo "  $PUT"

info "等待 GC (40s)..."
sleep 40
if grep -q "垃圾回收完成" "$D/n.log"; then
    info "GC 已完成 —— 读路径将走 sortedFile（稀疏索引生效）"
    GC_RAN=yes
else
    warn "GC 未触发，读路径不经 sortedFile，本轮 GET/SCAN 不反映稀疏索引代价"
    GC_RAN=no
fi
kill -0 $PID 2>/dev/null || { tail -30 "$D/n.log"; fail "节点在 GC 中崩溃"; }

info "[$LABEL] GET (Zipf)..."
GET=$(go run ./benchmark/zipf_read/zipf_read.go \
    -cnums 20 -dnums 20000 -servers 127.0.0.1:3088 2>&1 | grep elapse:) || GET="(失败)"
echo "  $GET"

info "[$LABEL] SCAN..."
SCAN=$(go run ./benchmark/scan_pro/scan_pro.go \
    -cnums 1 -dnums 100 -servers 127.0.0.1:3088 2>&1 | grep elapse:) || SCAN="(失败)"
echo "  $SCAN"

kill $SAMPLER 2>/dev/null
PEAK=$(awk '{if($1>m)m=$1}END{print int(m/1024)}' "$D/rss.txt")
FINAL=$(( $(rss) / 1024 ))

lat(){ sed -n 's/.*latency:\([0-9.]*\)ms.*/\1/p' <<<"$1"; }
thr(){ sed -n 's/.*throughput:\([0-9.]*\)MB\/S.*/\1/p' <<<"$1"; }

{
  echo "label,commit,writes,vsize,gc_ran,peak_rss_mb,final_rss_mb,put_lat_ms,put_thr,get_lat_ms,get_thr,scan_lat_ms,scan_thr"
  echo "$LABEL,$COMMIT,$N,$VSIZE,$GC_RAN,$PEAK,$FINAL,$(lat "$PUT"),$(thr "$PUT"),$(lat "$GET"),$(thr "$GET"),$(lat "$SCAN"),$(thr "$SCAN")"
} > "$CSV"

echo ""; info "结果 -> $CSV"; column -s, -t "$CSV"
