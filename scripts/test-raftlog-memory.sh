#!/bin/bash
# 量化 rf.log 压缩效果：跑写入负载，采样节点 RSS。
# 用法: bash scripts/test-raftlog-memory.sh [value大小] [写入条数]
set -e

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[INFO]${NC} $1"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# shellcheck source=scripts/lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/bench-common.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_DIR"

VSIZE="${1:-64}"
DNUMS="${2:-500000}"
CNUMS=50

export PATH=$PATH:/usr/local/go/bin
ROCKSDB_LIB_DIR=""
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && ROCKSDB_LIB_DIR="$d" && break
done
[ -z "$ROCKSDB_LIB_DIR" ] && fail "librocksdb.so 未找到，先跑 scripts/setup-env.sh"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L${ROCKSDB_LIB_DIR} -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:${ROCKSDB_LIB_DIR}

info "构建..."
go build -o /tmp/nezha-memtest ./kvstore/FlexSync/ || fail "编译失败"

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
go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
    -cnums $CNUMS -dnums $DNUMS -vsize $VSIZE -servers 127.0.0.1:3088 2>&1 | tail -3

kill $SAMPLER 2>/dev/null || true
info "写入完成，等待 compactLog 触发 (15s)..."
sleep 15

PEAK=$(peak_mb "$DATA_DIR/rss.txt") || fail "RSS 采样为空，无法给出峰值内存"
FINAL=$(rss_mb "$NODE_PID")
kill -0 "$NODE_PID" 2>/dev/null || warn "节点已退出（很可能被 OOM 杀死），压缩后 RSS 不代表稳态"
echo ""
echo "=============================================="
echo " value 大小      : ${VSIZE} B"
echo " 写入条数        : ${DNUMS}"
echo " 峰值 RSS        : ${PEAK} MB"
echo " 压缩后 RSS      : ${FINAL} MB"
echo "=============================================="
echo ""
info "compactLog 日志:"
grep -i "compactLog" "$DATA_DIR/node.log" | tail -10 || warn "未见 compactLog 输出（日志条数可能未超阈值 20000，或 DPrintf 未开启）"
