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
run_bench(){  # $1=名称 $2...=命令；完整输出留在 $D/<名称>.out
    local name="$1"; shift
    "$@" > "$D/${name}.out" 2>&1
}

info "[$LABEL] PUT $N 条 (value=${VSIZE}B)..."
run_bench put go run ./benchmark/randwrite_goroutine/randwrite_goroutine.go \
    -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088
PUT=$(grep -iE "elapse" "$D/put.out" | tail -1)   # randwrite 只跑一轮
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
run_bench get go run ./benchmark/zipf_read/zipf_read.go \
    -cnums 20 -dnums 20000 -servers 127.0.0.1:3088
GET=$(<"$D/get.out")

# scan_pro 内部把轮数硬编码成 numTests=100，每轮跑 dnums 次范围扫描。
# dnums=100 时在 50 万 key 上要跑 4 小时以上，用 README 文档化的 dnums=4 规模，快 25 倍。
info "[$LABEL] SCAN..."
# SCAN 每轮扫遍全库，耗时随数据量线性增长；轮数可调，默认沿用 benchmark 的 100
run_bench scan go run ./benchmark/scan_pro/scan_pro.go \
    -cnums 1 -dnums 4 -tests "${SCAN_TESTS:-100}" -servers 127.0.0.1:3088
SCAN=$(<"$D/scan.out")

# 节点存活要先判：它决定后面的空数据该怎么解读。
# 节点被 OOM 杀掉本身就是结论——"原版在这个规模连读负载都跑不完"正是要证明的事，
# 此时把已经拿到的阶段数据丢掉才是错的。只有节点还活着却读不到东西，才是真故障。
kill $SAMPLER 2>/dev/null
NODE_ALIVE=yes
if ! kill -0 $PID 2>/dev/null; then
    NODE_ALIVE=no
    warn "节点在读取阶段退出（多半被 OOM 杀死）——已完成阶段的数据仍然有效，之后的阶段记为 DEAD"
    echo "--- 节点日志尾部 ---"; tail -20 "$D/n.log"
fi

# 两个读 benchmark 各跑 100 轮并自己算平均，取那个平均而不是最后一轮。
# 空轮占比同时汇报出来：scan_pro 的扫描起点是 rand.Intn(370000) 写死的，
# 数据量低于 37 万时大部分轮次会扫到空区间，SCAN 数字就没有意义了。
for name in GET SCAN; do
    text=$([ "$name" = GET ] && echo "$GET" || echo "$SCAN")
    read -r rounds empty <<<"$(bench_round_stats "$text")"
    if [ "$rounds" -gt 0 ] && [ "$empty" -ge "$rounds" ]; then
        if [ "$NODE_ALIVE" = no ]; then
            warn "$name: $rounds 轮全部 GoodPut 为 0——节点此时已被杀死，本阶段记为 DEAD"
            continue
        fi
        echo "--- 节点日志尾部 ---"; tail -40 "$D/n.log"
        fail "$name 的 $rounds 轮全部 GoodPut 为 0，而节点仍存活——这是真故障，不是内存不足"
    fi
    if [ "$rounds" -gt 0 ] && [ "$empty" -gt $((rounds / 5)) ]; then
        # 空轮有两个完全不同的成因，不能混为一谈：数据量小于扫描起点范围是负载配置
        # 问题，节点已死则是内存问题。写死的扫描范围是 370000。
        if [ "$NODE_ALIVE" = no ]; then
            warn "$name: $rounds 轮中 $empty 轮为空——节点已被杀死，本阶段数据作废"
        elif [ "$N" -lt 370000 ]; then
            warn "$name: $rounds 轮中 $empty 轮扫到空区间（数据量 $N 小于扫描起点范围 370000），平均值失真"
        else
            warn "$name: $rounds 轮中 $empty 轮为空，但数据量 $N 已覆盖扫描范围且节点存活——请检查读路径"
        fi
    fi
    info "  $name: $rounds 轮，平均吞吐 $(bench_avg_throughput "$text" || echo NA) MB/S，平均延迟 $(bench_avg_latency "$text" || echo NA) ms"
done
PEAK=$(peak_mb "$D/rss.txt") || fail "RSS 采样为空，无法给出峰值内存"
FINAL=$(rss_mb "$PID")   # 节点已死时为 0

# 解析见 lib/bench-common.sh：三个 benchmark 的输出格式互不相同。
# 解析结果为空说明格式又变了，宁可报错也不要往 CSV 里写空字段——
# 空字段在汇总表里看起来只是"这一列没测"，很容易被当成正常结果读过去。
# 校验必须在主 shell 里做：放进 $( ) 的话 fail 的 exit 只会结束那个子 shell，
# 主脚本照常把空字段写进 CSV。
# PUT 是单轮，GET/SCAN 取各自的 100 轮平均。延迟一律换算成毫秒——
# 三个 benchmark 混用 µs/ms/s，直接取数字会让秒被当成毫秒记进 *_lat_ms 列。
# 解析不出来分两种情况：节点已死是预期内的（记 DEAD），节点活着就是格式变了（fail）。
#
# 一个阶段只要判定为 DEAD，它的每一项指标都必须记 DEAD，哪怕某项还能解析出数字。
# 节点死后 scan_pro 照样把失败轮计进平均，于是打印出 "平均延迟 18164ms" 这种
# 纯粹的垃圾值——一旦让它进了 CSV，看表的人没有任何办法把它和真实测量区分开。
stage_dead(){  # $1=阶段名，判断该阶段是否发生在节点死亡之后
    [ "$NODE_ALIVE" = no ] || return 1
    case "$1" in SCAN) return 0;; esac   # 节点死在读阶段，SCAN 是最后一个阶段
    # PUT/GET 若能解析出值就是死亡前完成的，保留
    return 1
}
parse_or_dead(){  # $1=名称 $2=解析出的值（可能为空）
    if stage_dead "$1"; then echo "DEAD"
    elif [ -n "$2" ]; then echo "$2"
    elif [ "$NODE_ALIVE" = no ]; then echo "DEAD"
    else return 1
    fi
}
PUT_LAT=$(parse_or_dead PUT  "$(bench_latency "$PUT")")           || fail "PUT 延迟解析失败 -> $PUT"
PUT_THR=$(parse_or_dead PUT  "$(bench_throughput "$PUT")")        || fail "PUT 吞吐解析失败 -> $PUT"
GET_LAT=$(parse_or_dead GET  "$(bench_avg_latency "$GET")")       || fail "GET 平均延迟解析失败"
GET_THR=$(parse_or_dead GET  "$(bench_avg_throughput "$GET")")    || fail "GET 平均吞吐解析失败"
SCAN_LAT=$(parse_or_dead SCAN "$(bench_avg_latency "$SCAN")")     || fail "SCAN 平均延迟解析失败"
SCAN_THR=$(parse_or_dead SCAN "$(bench_avg_throughput "$SCAN")")  || fail "SCAN 平均吞吐解析失败"
read -r GET_ROUNDS GET_EMPTY <<<"$(bench_round_stats "$GET")"
read -r SCAN_ROUNDS SCAN_EMPTY <<<"$(bench_round_stats "$SCAN")"
ROW="$LABEL,$COMMIT,$N,$VSIZE,$GC_RAN,$PEAK,$FINAL,$PUT_LAT,$PUT_THR,$GET_LAT,$GET_THR,$SCAN_LAT,$SCAN_THR,$GET_EMPTY/$GET_ROUNDS,$SCAN_EMPTY/$SCAN_ROUNDS,$NODE_ALIVE"

{
  echo "label,commit,writes,vsize,gc_ran,peak_rss_mb,final_rss_mb,put_lat_ms,put_thr,get_lat_ms,get_thr,scan_lat_ms,scan_thr,get_empty_rounds,scan_empty_rounds,node_alive"
  echo "$ROW"
} > "$CSV"

echo ""; info "结果 -> $CSV"; column -s, -t "$CSV"
