#!/bin/bash
# 内存随写入量的增长曲线：验证 rf.log 压缩使内存与数据集解耦。
# 用法: bash scripts/bench/memory-curve.sh <标签> [value大小] [写入量列表...]
# 例:   bash scripts/bench/memory-curve.sh after 64 250000 500000 1000000 2000000
set -u

GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# shellcheck source=../lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/bench-common.sh"

# 项目目录按**脚本自身位置**推导，不要写死 $HOME/Github/Nezha：实验机上仓库在
# ~/work/Nezha，写死的那版在那里直接 "无项目目录" 退出（2026-09-17 实测）。
PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"
cd "$PROJECT_DIR" || fail "找不到项目目录 $PROJECT_DIR"

LABEL="${1:?用法: $0 <标签> [vsize] [写入量...]}"; shift
VSIZE="${1:-64}"; shift || true
SIZES=("$@"); [ ${#SIZES[@]} -eq 0 ] && SIZES=(250000 500000 1000000 2000000)
CNUMS=50

# cgo 环境（RocksDB 的头与库）集中在 scripts/lib/cgo-env.sh 一份。
# 原先这里写死了三个系统路径 + -I/usr/include，在实验机上会"找到错的版本"，
# 报错出现在 cgo 阶段、读起来像代码问题。理由见那个文件。
# shellcheck source=scripts/lib/cgo-env.sh
# 用 $PROJECT_DIR 而不是 $(dirname "$0")：这一行在 `cd "$PROJECT_DIR"` **之后**，
# 而 $0 是相对路径（`bash ./snapshot-crash.sh`），cd 一发生它就失效——十个脚本
# 全都中了，实测报 "./../lib/cgo-env.sh: No such file or directory"。
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || fail "cgo 环境准备失败（见 scripts/lib/cgo-env.sh）"

BIN=/tmp/nezha-curve-$LABEL
CSV=/tmp/curve_${LABEL}.csv
info "构建 ($(git rev-parse --short HEAD))..."
go build -o "$BIN" ./cmd/nezha/ || fail "编译失败"

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
    go run ./cmd/bench/randwrite_goroutine/ \
        -cnums $CNUMS -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 > "$DATA_DIR/put.out" 2>&1
    OUT=$(grep "elapse:" "$DATA_DIR/put.out" | tail -1)

    # **采样必须盖住压缩期。** 原先是先 kill 采样器、再 sleep 等 compactLog，于是
    # "峰值"只覆盖写入阶段，而压缩后那个数是 15 秒后的一次孤立采样。compactLog 用
    # make+copy，会临时再分配一份等长数组，而 Go 不把 RSS 还给操作系统——所以
    # "压缩后" 必然**大于** "峰值"（2026-09-17 实测 361MB vs 415MB），读起来像是压缩
    # 把内存搞大了。那是测量假象，不是结论。
    #
    # 现在让采样器一直跑到压缩窗口结束，峰值才真的是全程峰值；而"压缩是否有效"的
    # 判据本来就不该看 RSS，要看 compactLog 报的**保留条数**（下面一并打出来）。
    sleep 15   # 等 compactLog 触发，采样器仍在跑
    kill $SAMPLER 2>/dev/null

    # 节点若在写入途中被 OOM 杀掉，本轮仍要把已采到的峰值记下来——
    # 那恰恰是最有价值的数据点（对照组撑不住的规模）。旧版在这里因为
    # 空 RSS 进算术展开而整轮报错退出，反而把结论丢了。
    ALIVE=DEAD; kill -0 $PID 2>/dev/null && ALIVE=ALIVE
    PEAK=$(peak_mb "$DATA_DIR/rss.txt") || { PEAK=0; info "[$LABEL] N=$N RSS 采样为空"; }
    FINAL=$(rss_mb "$PID")
    # compactLog 报的保留条数：压缩是否生效看这个，不看 RSS（Go 不归还 RSS）。
    RET=$(grep -o "compactLog: [0-9]* -> [0-9]* 条" "$DATA_DIR/node.log" 2>/dev/null | tail -1 | grep -oE "[0-9]+ 条" | grep -oE "[0-9]+")
    RET=${RET:-none}
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
