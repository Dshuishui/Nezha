#!/bin/bash
# 内存随写入量的增长曲线：验证 rf.log 压缩使内存与数据集解耦。
# 用法: bash scripts/bench-memory-curve.sh <标签> [value大小] [写入量列表...]
# 例:   bash scripts/bench-memory-curve.sh after 64 250000 500000 1000000 2000000
set -u

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# shellcheck source=scripts/lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/bench-common.sh"

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

echo "label,commit,vsize,writes,peak_rss_mb,final_rss_mb,latency_ms,throughput_mbs,goodput,node_alive" > "$CSV"
COMMIT=$(git rev-parse --short HEAD)

for N in "${SIZES[@]}"; do
    DATA_DIR=$(mktemp -d)
    "$BIN" -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
        -peers 127.0.0.1:30881 -data "$DATA_DIR" -gap 1000000 > "$DATA_DIR/node.log" 2>&1 &
    PID=$!
    sleep 8
    kill -0 $PID 2>/dev/null || { cat "$DATA_DIR/node.log"; rm -rf "$DATA_DIR"; fail "节点未启动 (N=$N)"; }

    SAMPLER=$(start_rss_sampler "$PID" "$DATA_DIR/rss.txt" 3)

    info "[$LABEL] 写入 $N 条 (value=${VSIZE}B)..."
    # 完整输出先落盘，再抓结果行：管道退出码取自 grep，直接判断 $? 抓不到 benchmark 崩溃。
    go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
        -cnums $CNUMS -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 > "$DATA_DIR/put.out" 2>&1
    OUT=$(grep "elapse:" "$DATA_DIR/put.out" | tail -1)

    kill $SAMPLER 2>/dev/null
    sleep 15   # 等 compactLog 触发

    # 节点若在写入途中被 OOM 杀掉，本轮仍要把已采到的峰值记下来——
    # 那恰恰是最有价值的数据点（对照组撑不住的规模）。旧版在这里因为
    # 空 RSS 进算术展开而整轮报错退出，反而把结论丢了。
    ALIVE=DEAD; kill -0 $PID 2>/dev/null && ALIVE=ALIVE
    PEAK=$(peak_mb "$DATA_DIR/rss.txt") || { PEAK=0; info "[$LABEL] N=$N RSS 采样为空"; }
    FINAL=$(rss_mb "$PID")
    LAT=$(sed -n 's/.*avg latency:\([0-9.]*\)ms.*/\1/p' <<<"$OUT")
    THR=$(sed -n 's/.*throughput:\([0-9.]*\)MB\/S.*/\1/p' <<<"$OUT")
    GP=$(sed -n 's/.*goodPut \([0-9]*\).*/\1/p' <<<"$OUT")

    echo "$LABEL,$COMMIT,$VSIZE,$N,$PEAK,$FINAL,${LAT:-NA},${THR:-NA},${GP:-NA},$ALIVE" >> "$CSV"
    info "[$LABEL] N=$N  峰值=${PEAK}MB  结束=${FINAL}MB  延迟=${LAT:-NA}ms  吞吐=${THR:-NA}MB/s  节点=$ALIVE"
    if [ "$ALIVE" = DEAD ]; then
        info "[$LABEL] N=$N 节点已退出（很可能被 OOM 杀死），节点日志尾部："
        tail -5 "$DATA_DIR/node.log"
    fi

    kill $PID 2>/dev/null; wait $PID 2>/dev/null
    rm -rf "$DATA_DIR"
    sleep 5
done

rm -f "$BIN"
echo ""; info "结果写入 $CSV"; column -s, -t "$CSV"
