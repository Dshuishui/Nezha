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

# shellcheck source=scripts/lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/bench-common.sh"

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
# valuelog 每条约 20B 头 + 10B key + value。阈值取预计总量的三分之一，确保 GC 必然触发。
BYTES_PER_ENTRY=$((20 + 10 + VSIZE))
GC_GB="${GC_GB:-$(awk -v n="$N" -v b="$BYTES_PER_ENTRY" 'BEGIN{printf "%.4f", n*b/1073741824/3}')}"
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
# 失败时保留 $D：里面有节点日志和 RSS 采样，删掉就无从追查了。
KEEP_DATA=0
cleanup(){
    kill ${PID:-} 2>/dev/null
    rm -f "/tmp/nezha-cmp-$LABEL"
    if [ "$KEEP_DATA" = 1 ]; then
        echo -e "${YEL}[WARN]${NC} 已保留现场: $D"
    else
        rm -rf "$D"
    fi
}
trap cleanup EXIT
fail(){ KEEP_DATA=1; echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

# shellcheck disable=SC2086
"/tmp/nezha-cmp-$LABEL" -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 -data "$D" -gap 1000000 $EXTRA > "$D/n.log" 2>&1 &
PID=$!
sleep 10
kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; fail "节点未启动"; }

SAMPLER=$(start_rss_sampler "$PID" "$D/rss.txt" 3)

# 先落盘完整输出再抓结果行：写成 `X=$(cmd | grep ...) || fail` 是抓不到失败的，
# 管道退出码取自最后一个命令，grep/tail 的成功会掩盖 benchmark 的崩溃。
run_bench(){  # $1=名称 $2...=命令
    local name="$1"; shift
    local raw="$D/${name}.out"
    "$@" > "$raw" 2>&1
    grep -iE "elapse" "$raw" | tail -1
}

info "[$LABEL] PUT $N 条 (value=${VSIZE}B)..."
PUT=$(run_bench put go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
    -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088)
echo "  ${PUT:-（无输出）}"
if reason=$(bench_invalid_reason PUT "$PUT"); then
    tail -20 "$D/put.out"; fail "$reason"
fi

info "等待 GC (40s)..."
sleep 40
if grep -q "垃圾回收完成" "$D/n.log"; then
    info "GC 已完成 —— 读路径将走 sortedFile（稀疏索引生效）"
    GC_RAN=yes
else
    ls -la "$D"/data/valuelog/ 2>/dev/null | tail -3
    kill $PID 2>/dev/null
    fail "GC 未触发（valuelog 未达 ${GC_GB}GB）——读路径不经 sortedFile，本轮测不出稀疏索引代价"
fi
kill -0 $PID 2>/dev/null || { tail -30 "$D/n.log"; fail "节点在 GC 中崩溃"; }

info "[$LABEL] GET (Zipf)..."
GET=$(run_bench get go run ./benchmark/zipf_read/zipf_read.go \
    -cnums 20 -dnums 20000 -servers 127.0.0.1:3088)
echo "  ${GET:-（无输出）}"

# scan_pro 内部把轮数硬编码成 numTests=100，每轮跑 dnums 次范围扫描。
# dnums=100 时在 50 万 key 上要跑 4 小时以上，用 README 文档化的 dnums=4 规模，快 25 倍。
info "[$LABEL] SCAN..."
SCAN=$(run_bench scan go run ./benchmark/scan_pro/scan_pro.go \
    -cnums 1 -dnums 4 -servers 127.0.0.1:3088)
echo "  ${SCAN:-（无输出）}"

for pair in "GET:$GET" "SCAN:$SCAN"; do
    name=${pair%%:*}; text=${pair#*:}
    if reason=$(bench_invalid_reason "$name" "$text"); then
        echo "--- 节点日志尾部 ---"; tail -40 "$D/n.log"
        fail "$reason"
    fi
done

kill $SAMPLER 2>/dev/null
if ! kill -0 $PID 2>/dev/null; then
    echo "--- 节点日志尾部 ---"; tail -40 "$D/n.log"
    fail "节点在读取阶段退出（GET/SCAN 数据不可用）"
fi
PEAK=$(peak_mb "$D/rss.txt") || fail "RSS 采样为空，无法给出峰值内存"
FINAL=$(rss_mb "$PID")

# 解析见 lib/bench-common.sh：三个 benchmark 的输出格式互不相同。
# 解析结果为空说明格式又变了，宁可报错也不要往 CSV 里写空字段——
# 空字段在汇总表里看起来只是"这一列没测"，很容易被当成正常结果读过去。
# 校验必须在主 shell 里做：放进 $( ) 的话 fail 的 exit 只会结束那个子 shell，
# 主脚本照常把空字段写进 CSV。
CELLS=()
for pair in "PUT:$PUT" "GET:$GET" "SCAN:$SCAN"; do
    name=${pair%%:*}; text=${pair#*:}
    l=$(bench_latency "$text"); t=$(bench_throughput "$text")
    [ -n "$l" ] || fail "$name 延迟解析失败：输出格式与解析规则不符 -> $text"
    [ -n "$t" ] || fail "$name 吞吐解析失败：输出格式与解析规则不符 -> $text"
    CELLS+=("$l" "$t")
done
ROW="$LABEL,$COMMIT,$N,$VSIZE,$GC_RAN,$PEAK,$FINAL,$(IFS=,; echo "${CELLS[*]}")"

{
  echo "label,commit,writes,vsize,gc_ran,peak_rss_mb,final_rss_mb,put_lat_ms,put_thr,get_lat_ms,get_thr,scan_lat_ms,scan_thr"
  echo "$ROW"
} > "$CSV"

echo ""; info "结果 -> $CSV"; column -s, -t "$CSV"
