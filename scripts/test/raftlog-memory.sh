#!/bin/bash
# 量化 rf.log 压缩效果：跑写入负载，采样节点 RSS。
# 用法: bash scripts/test/raftlog-memory.sh [value大小] [写入条数]
set -e

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# shellcheck source=../lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/../lib/bench-common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
cd "$PROJECT_DIR"

VSIZE="${1:-64}"
DNUMS="${2:-500000}"
CNUMS=50

export PATH=$PATH:/usr/local/go/bin
ROCKSDB_LIB_DIR=""
# cgo 环境（RocksDB 的头与库）集中在 scripts/lib/cgo-env.sh 一份。
# 原先这里写死了三个系统路径 + -I/usr/include，在实验机上会"找到错的版本"，
# 报错出现在 cgo 阶段、读起来像代码问题。理由见那个文件。
# shellcheck source=scripts/lib/cgo-env.sh
# 用 $PROJECT_DIR 而不是 $(dirname "$0")：这一行在 `cd "$PROJECT_DIR"` **之后**，
# 而 $0 是相对路径（`bash ./snapshot-crash.sh`），cd 一发生它就失效——十个脚本
# 全都中了，实测报 "./../lib/cgo-env.sh: No such file or directory"。
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || fail "cgo 环境准备失败（见 scripts/lib/cgo-env.sh）"

info "构建..."
go build -o /tmp/nezha-memtest ./cmd/nezha/ || fail "编译失败"

DATA_DIR="$(mktemp -d)"
trap 'kill $NODE_PID 2>/dev/null; rm -rf "$DATA_DIR" /tmp/nezha-memtest' EXIT

info "启动节点 (value=${VSIZE}B, 写入 ${DNUMS} 条)..."
/tmp/nezha-memtest -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 -data "$DATA_DIR" -gap 100000 > "$DATA_DIR/node.log" 2>&1 &
NODE_PID=$!
sleep 8
kill -0 "$NODE_PID" 2>/dev/null || { cat "$DATA_DIR/node.log"; fail "节点未启动"; }

info "写入前 RSS: $(rss_mb "$NODE_PID") MB"

# 后台采样 RSS
SAMPLER=$(start_rss_sampler "$NODE_PID" "$DATA_DIR/rss.txt" 5)

info "开始写入..."
go run ./cmd/bench/randwrite_goroutine/ \
    -cnums $CNUMS -dnums $DNUMS -vsize $VSIZE -servers 127.0.0.1:3088 2>&1 | tail -3

# **采样必须盖住压缩期。** 原先是先 kill 采样器、再 sleep 等 compactLog，于是
# "峰值"只覆盖写入阶段，而压缩后那个数是 15 秒后的一次孤立采样。compactLog 用
# make+copy，会临时再分配一份等长数组，而 Go 不把 RSS 还给操作系统——所以
# "压缩后" 必然**大于** "峰值"（2026-09-17 实测 361MB vs 415MB），读起来像是压缩
# 把内存搞大了。那是测量假象，不是结论。
#
# 现在让采样器一直跑到压缩窗口结束，峰值才真的是全程峰值；而"压缩是否有效"的
# 判据本来就不该看 RSS，要看 compactLog 报的**保留条数**（下面一并打出来）。
info "写入完成，等待 compactLog 触发 (15s，采样继续)..."
sleep 15
kill $SAMPLER 2>/dev/null || true

PEAK=$(peak_mb "$DATA_DIR/rss.txt") || fail "RSS 采样为空，无法给出峰值内存"
FINAL=$(rss_mb "$NODE_PID")
kill -0 "$NODE_PID" 2>/dev/null || warn "节点已退出（很可能被 OOM 杀死），压缩后 RSS 不代表稳态"
echo ""
echo "=============================================="
echo " value 大小      : ${VSIZE} B"
echo " 写入条数        : ${DNUMS}"
RET=$(grep -o "compactLog: [0-9]* -> [0-9]* 条" "$DATA_DIR/node.log" 2>/dev/null | tail -1 | grep -oE "[0-9]+ 条" | grep -oE "[0-9]+")
echo " 全程峰值 RSS    : ${PEAK} MB  （采样覆盖写入 + 压缩窗口）"
echo " 末次 RSS        : ${FINAL} MB  （Go 不把 RSS 还给 OS，所以它不会随压缩下降）"
echo " 压缩后保留条数  : ${RET:-取不到}  ← **压缩是否生效看这个**，不看 RSS"
echo "=============================================="
echo ""
info "compactLog 日志:"
grep -i "compactLog" "$DATA_DIR/node.log" | tail -10 || warn "未见 compactLog 输出（日志条数可能未超阈值 20000，或 DPrintf 未开启）"
