#!/bin/bash
# 验证 goodPut 缺口是不是真丢数据。
#
# 每轮写入都有约 0.01% 被计为失败（例：999915/1000000）。唯一来源是 StartPut 的
# 超时分支 `case <-timer.C: reply.Err = "defeat"`——60 秒内没等到 apply 回调。
# 但日志此时早已落盘，apply 也可能只是晚到，所以"客户端没收到确认"未必等于
# "数据没写进去"。客户端的计数分不出这两者，只能从服务端数。
#
# 判定：
#   RocksDB key 数 == 请求总数   -> 数据完好，goodPut 是统计口径问题
#   RocksDB key 数 == goodPut    -> 真丢了那些写入，是正确性问题
#
# 用法: bash scripts/test/verify-goodput.sh [写入量] [value大小]
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/autodl-tmp/work/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"
N="${1:-200000}"; VSIZE="${2:-64}"
D=$(mktemp -d -p "${TMPDIR:-/tmp}")
BIN=/tmp/nezha-vgp
GB=$(awk -v n="$N" -v v="$VSIZE" 'BEGIN{printf "%.4f", n*(30+v)/1073741824/3}')

go build -o "$BIN" ./cmd/nezha/ || fail "编译失败"
go build -o /tmp/countkeys ./cmd/bench/countkeys/ || fail "countkeys 编译失败"

"$BIN" -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
    -peers 127.0.0.1:30881 -data "$D" -gap 1000000 \
    -inlineCacheMB 256 -indexBlockKB 4 -gcThresholdGB "$GB" > "$D/n.log" 2>&1 &
PID=$!
cleanup(){ kill $PID 2>/dev/null; wait $PID 2>/dev/null; rm -rf "$D" "$BIN"; }
trap cleanup EXIT
sleep 10
kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; fail "节点未启动"; }

info "写入 $N 条 × ${VSIZE}B..."
go run ./cmd/bench/randwrite_goroutine/ \
    -cnums 50 -dnums "$N" -vsize "$VSIZE" -servers 127.0.0.1:3088 > "$D/put.out" 2>&1
PUT=$(grep elapse: "$D/put.out" | tail -1)
echo "  $PUT"
GOODPUT=$(sed -n 's/.*goodPut \([0-9]*\).*/\1/p' <<<"$PUT")
[ -n "$GOODPUT" ] || fail "解析不出 goodPut"
GAP=$((N - GOODPUT))
info "写入端报告 goodPut=$GOODPUT，缺口=$GAP"

# 停机后再数：RocksDB 是独占打开的，且要让 apply 把队列排干
info "等待 apply 排空并停止节点..."
sleep 30
kill $PID 2>/dev/null; wait $PID 2>/dev/null

DB=$(find "$D" -name "CURRENT" -path "*/db*" 2>/dev/null | head -1 | xargs -r dirname)
[ -n "$DB" ] || DB=$(find "$D" -name "CURRENT" 2>/dev/null | head -1 | xargs -r dirname)
[ -n "$DB" ] || fail "找不到 RocksDB 目录"
info "RocksDB 目录: $DB"

COUNT=$(/tmp/countkeys -db "$DB" | sed -n 's/^KEYCOUNT \([0-9]*\)/\1/p')
[ -n "$COUNT" ] || fail "计数失败"

echo ""
echo "=============================================="
echo " 请求总数        : $N"
echo " 写入端 goodPut  : $GOODPUT  (缺口 $GAP)"
echo " RocksDB 实际 key: $COUNT"
echo "=============================================="
if [ "$COUNT" -eq "$N" ]; then
    echo -e "${GREEN}结论：数据完好。goodPut 缺口是统计口径问题——那些请求写进去了，只是 60 秒内没返回确认。${NC}"
elif [ "$COUNT" -eq "$GOODPUT" ]; then
    echo -e "${RED}结论：确实丢了 $GAP 条写入，是正确性问题，需要排查。${NC}"
else
    echo -e "${YEL}结论：key 数($COUNT)既不等于请求数也不等于 goodPut，需要单独排查。${NC}"
fi
