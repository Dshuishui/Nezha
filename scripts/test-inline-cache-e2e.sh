#!/bin/bash
# 端到端验证 GC 后的读路径（稀疏块索引 + 有界内联缓存）。
#
# 做法是对照实验：同样的写入负载、同样的读负载，分别跑一遍
#   A) GC 关闭 —— 读全部走 valuelog，作为正确性基准
#   B) GC 开启 —— 读走 sortedFile 稀疏索引，且内联缓存故意设得极小以强制淘汰
# 两者命中数接近才算通过。
#
# 单看 B 的绝对命中率没有意义：zipf_read 从 1 亿的键空间采样，而我们只写了 dnums 个 key，
# 大部分采样落在从未写入的 key 上，本来就查不到。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
pass(){ echo -e "${GREEN}[PASS]${NC} $1"; }
warn(){ echo -e "${YEL}[WARN]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && fail "librocksdb.so 未找到，先跑 scripts/setup-env.sh"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

N=${1:-200000}; VSIZE=${2:-64}; CACHE_MB=${3:-1}; GC_GB=${4:-0.005}
READS=${READS:-20000}

info "构建 ($(git rev-parse --short HEAD))..."
go build -o /tmp/nezha-e2e ./cmd/nezha/ || fail "编译失败"

pkill -f nezha-e2e 2>/dev/null; sleep 2   # 清掉上一轮残留，否则抢 3088 端口

# run_case <标签> <GC阈值GB> -> 回显 "GoodPut 命中数"，并在失败时 dump 节点日志
run_case() {
    local label="$1" gcgb="$2"
    local D; D=$(mktemp -d)
    /tmp/nezha-e2e -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
        -peers 127.0.0.1:30881 -data "$D" -gap 1000000 \
        -inlineCacheMB "$CACHE_MB" -gcThresholdGB "$gcgb" > "$D/n.log" 2>&1 &
    local PID=$!
    sleep 10
    kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; rm -rf "$D"; fail "[$label] 节点未启动"; }

    go run ./cmd/bench/randwrite_goroutine/ \
        -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 2>&1 | grep elapse: \
        || { kill $PID; rm -rf "$D"; fail "[$label] 写入失败"; }

    sleep 30   # 给 GC 触发的时间（GC 关闭时这段只是等待落盘）
    local gc_rounds
    gc_rounds=$(grep -c "垃圾回收完成" "$D/n.log" 2>/dev/null || echo 0)
    info "[$label] GC 完成计数 = $gc_rounds"

    kill -0 $PID 2>/dev/null || { tail -30 "$D/n.log"; rm -rf "$D"; fail "[$label] 节点在 GC 中崩溃"; }

    local out
    out=$(go run ./cmd/bench/zipf_read/ \
        -cnums 20 -dnums "$READS" -servers 127.0.0.1:3088 2>&1)

    if ! kill -0 $PID 2>/dev/null; then
        echo "--- 节点日志尾部 ---"; tail -40 "$D/n.log"
        rm -rf "$D"; fail "[$label] 节点在读取阶段退出"
    fi

    # zipf_read 输出形如 "... Total: 20000, GoodPut: 13869, ..."（注意大写）
    local gp
    gp=$(grep -oE "GoodPut: *[0-9]+" <<<"$out" | tail -1 | grep -oE "[0-9]+")
    kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$D"

    [ -z "$gp" ] && { echo "$out" | tail -15; fail "[$label] 无法解析 GoodPut"; }
    echo "$gc_rounds $gp"
}

info "=== 对照组 A：GC 关闭（读走 valuelog） ==="
readarray -t A < <(run_case "GC关闭" 999 | tail -1 | tr ' ' '\n')
A_ROUNDS=${A[0]}; A_GP=${A[1]}
[ "$A_ROUNDS" != "0" ] && warn "对照组本不该触发 GC，却完成了 $A_ROUNDS 次"
pass "对照组 GoodPut = $A_GP / $READS"

info "=== 实验组 B：GC 开启（读走 sortedFile 稀疏索引，内联缓存仅 ${CACHE_MB}MB） ==="
readarray -t B < <(run_case "GC开启" "$GC_GB" | tail -1 | tr ' ' '\n')
B_ROUNDS=${B[0]}; B_GP=${B[1]}
[ "$B_ROUNDS" = "0" ] && fail "实验组未触发 GC（valuelog 未达 ${GC_GB}GB），本轮什么也没验证到"
pass "实验组 GC 完成 $B_ROUNDS 次，GoodPut = $B_GP / $READS"

# 两组读的是随机 Zipf 采样，允许少量抖动；差异超过 2% 说明 GC 后丢了数据
DIFF=$(( A_GP > B_GP ? A_GP - B_GP : B_GP - A_GP ))
TOL=$(( A_GP / 50 ))
echo ""
echo "=============================================="
printf " 对照组(GC关闭) GoodPut : %d\n" "$A_GP"
printf " 实验组(GC开启) GoodPut : %d\n" "$B_GP"
printf " 差异 / 容差            : %d / %d\n" "$DIFF" "$TOL"
echo "=============================================="
[ "$DIFF" -gt "$TOL" ] && fail "GC 后命中数下降超出容差，说明数据在 GC 路径上丢失"

rm -f /tmp/nezha-e2e
echo ""; pass "端到端验证通过：GC 后读路径与未 GC 时一致"
