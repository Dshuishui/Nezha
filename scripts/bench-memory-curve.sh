#!/bin/bash
# 内存随写入量的增长曲线：验证 rf.log 压缩使内存与数据集解耦。
# 用法: bash scripts/bench-memory-curve.sh <标签> [value大小] [写入量列表...]
# 例:   bash scripts/bench-memory-curve.sh after 64 250000 500000 1000000 2000000
set -u

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"
cd "$PROJECT_DIR" || fail "找不到项目目录 $PROJECT_DIR"

LABEL="${1:?用法: $0 <标签> [vsize] [写入量...]}"; shift
VSIZE="${1:-64}"; shift || true
SIZES=("$@"); [ ${#SIZES[@]} -eq 0 ] && SIZES=(250000 500000 1000000 2000000)
CNUMS=50

export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && fail "librocksdb.so 未找到"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

BIN=/tmp/nezha-curve-$LABEL
CSV=/tmp/curve_${LABEL}.csv
info "构建 ($(git rev-parse --short HEAD))..."
go build -o "$BIN" ./kvstore/FlexSync/ || fail "编译失败"

echo "label,commit,vsize,writes,peak_rss_mb,final_rss_mb,latency_ms,throughput_mbs,goodput" > "$CSV"
COMMIT=$(git rev-parse --short HEAD)

for N in "${SIZES[@]}"; do
    DATA_DIR=$(mktemp -d)
    "$BIN" -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
        -peers 127.0.0.1:30881 -data "$DATA_DIR" -gap 1000000 > "$DATA_DIR/node.log" 2>&1 &
    PID=$!
    sleep 8
    kill -0 $PID 2>/dev/null || { cat "$DATA_DIR/node.log"; rm -rf "$DATA_DIR"; fail "节点未启动 (N=$N)"; }

    ( while kill -0 $PID 2>/dev/null; do ps -o rss= -p $PID 2>/dev/null | tr -d ' '; sleep 3; done ) > "$DATA_DIR/rss.txt" &
    SAMPLER=$!

    info "[$LABEL] 写入 $N 条 (value=${VSIZE}B)..."
    OUT=$(go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
        -cnums $CNUMS -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 2>&1 | grep "elapse:")

    kill $SAMPLER 2>/dev/null
    sleep 15   # 等 compactLog 触发

    PEAK=$(awk '{if($1>m)m=$1}END{print int(m/1024)}' "$DATA_DIR/rss.txt")
    FINAL=$(( $(ps -o rss= -p $PID 2>/dev/null | tr -d ' ' || echo 0) / 1024 ))
    LAT=$(sed -n 's/.*avg latency:\([0-9.]*\)ms.*/\1/p' <<<"$OUT")
    THR=$(sed -n 's/.*throughput:\([0-9.]*\)MB\/S.*/\1/p' <<<"$OUT")
    GP=$(sed -n 's/.*goodPut \([0-9]*\).*/\1/p' <<<"$OUT")

    echo "$LABEL,$COMMIT,$VSIZE,$N,$PEAK,$FINAL,$LAT,$THR,$GP" >> "$CSV"
    info "[$LABEL] N=$N  峰值=${PEAK}MB  结束=${FINAL}MB  延迟=${LAT}ms  吞吐=${THR}MB/s"

    kill $PID 2>/dev/null; wait $PID 2>/dev/null
    rm -rf "$DATA_DIR"
    sleep 5
done

rm -f "$BIN"
echo ""; info "结果写入 $CSV"; column -s, -t "$CSV"
