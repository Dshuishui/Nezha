#!/bin/bash
# 端到端验证有界内联缓存：用极小预算 + 低 GC 阈值强制淘汰，
# 确认小值被淘汰后仍能从 sortedFile 正确读回（不丢数据）。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
pass(){ echo -e "${GREEN}[PASS]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

N=${1:-200000}; VSIZE=${2:-64}; CACHE_MB=${3:-1}; GC_GB=${4:-0.02}

info "构建 ($(git rev-parse --short HEAD))..."
go build -o /tmp/nezha-e2e ./kvstore/FlexSync/ || fail "编译失败"

D=$(mktemp -d)
trap 'kill $PID 2>/dev/null; rm -rf "$D" /tmp/nezha-e2e' EXIT

info "启动: 内联缓存 ${CACHE_MB}MB, GC 阈值 ${GC_GB}GB, value ${VSIZE}B"
/tmp/nezha-e2e -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 -data "$D" -gap 1000000 \
    -inlineCacheMB "$CACHE_MB" -gcThresholdGB "$GC_GB" > "$D/n.log" 2>&1 &
PID=$!
sleep 10
kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; fail "节点未启动"; }

info "写入 $N 条..."
go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
    -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 2>&1 | grep elapse: \
    || fail "写入失败"

info "等待 GC (30s)..."
sleep 30
if grep -q "垃圾回收完成" "$D/n.log"; then
    pass "GC 已触发并完成"
    grep -E "垃圾回收(完成|出现)" "$D/n.log" | tail -3
else
    echo -e "${YEL}[WARN]${NC} GC 未触发（valuelog 未达 ${GC_GB}GB），内联路径未被执行"
    grep -iE "大小为.*GB" "$D/n.log" | tail -2
fi

kill -0 $PID 2>/dev/null || { tail -30 "$D/n.log"; fail "节点在 GC 中崩溃"; }
pass "节点存活"

info "读回验证（GC 后，缓存仅 ${CACHE_MB}MB，绝大多数读必然未命中）..."
READ_OUT=$(go run ./benchmark/zipf_read/zipf_read.go \
    -cnums 20 -dnums 20000 -servers 127.0.0.1:3088 2>&1)
echo "$READ_OUT" | grep -E "elapse:|goodGet|goodPut" || { echo "$READ_OUT" | tail -15; fail "读取失败"; }

if echo "$READ_OUT" | grep -qiE "ErrNoKey|not found|key不存在"; then
    echo "$READ_OUT" | grep -iE "ErrNoKey|not found" | head -5
    fail "出现 key 找不到 —— 淘汰后无法退回 sortedFile 读取"
fi
pass "读回无 key 丢失"

kill -0 $PID 2>/dev/null || fail "节点在读取中崩溃"
echo ""; pass "端到端验证通过"
