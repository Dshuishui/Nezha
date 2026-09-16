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

# 项目目录按**脚本自身位置**推导，不要写死某台机器的布局：这里原先是
# $HOME/autodl-tmp/work/Nezha（又一台机器的路径），实验机上是 ~/work/Nezha。
PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || fail "无项目目录（推导得 ${PROJECT_DIR}）"
N="${1:-200000}"; VSIZE="${2:-64}"
D=$(mktemp -d -p "${TMPDIR:-/tmp}")
BIN=/tmp/nezha-vgp
# GC 阈值刻意设得**够不到**：这个脚本回答的是"写进去的条数对不对"，靠 countkeys 数存储
# 引擎里的行数与写入条数做精确相等比较。而一轮 GC 之后 key 随 value 迁入分区文件、存储引擎
# 里的行数趋近 0（实测就是 0），那个比较立刻失去意义。
#
# 此前阈值取的是数据量的三分之一（`n*r/3`），也就是**一定会触发 GC**——判定成不成立取决于
# GC 有没有恰好跑完，这是个竞态。现在取数据量的 10 倍，再在取数前断言一轮都没跑过。
GB=$(awk -v n="$N" -v r="$(record_bytes "$VSIZE")" 'BEGIN{printf "%.4f", n*r*10/1073741824}')

# cgo 环境必须从机器自己的 ~/env.sh 取。这个脚本原先**什么都不设**就直接 go build，
# 于是在实验机上用默认 GOPROXY（proxy.golang.org）去下载模块并超时，报"编译失败"——
# 实际是取不到依赖，跟代码无关。机器的 env.sh 里设着 GOPROXY=https://goproxy.cn
# 与 GOMODCACHE，所以走 cgo-env 就都有了。理由见 scripts/lib/cgo-env.sh。
# shellcheck source=scripts/lib/cgo-env.sh
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || fail "cgo 环境准备失败（见 scripts/lib/cgo-env.sh）"
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
info "写入端报告 goodPut=${GOODPUT}，缺口=$GAP"

# 停机后再数：RocksDB 是独占打开的，且要让 apply 把队列排干
info "等待 apply 排空并停止节点..."
sleep 30
kill $PID 2>/dev/null; wait $PID 2>/dev/null

DB=$(find "$D" -name "CURRENT" -path "*/db*" 2>/dev/null | head -1 | xargs -r dirname)
[ -n "$DB" ] || DB=$(find "$D" -name "CURRENT" 2>/dev/null | head -1 | xargs -r dirname)
[ -n "$DB" ] || fail "找不到 RocksDB 目录"
info "RocksDB 目录: $DB"

GCN=$(grep -c '垃圾回收完成' "$D/n.log" 2>/dev/null || true)
[ "${GCN:-0}" = 0 ] || fail "GC 跑了 ${GCN} 轮：key 已迁入分区文件，countkeys 的计数不再等于写入条数，本次判定作废（调大 gcThresholdGB）"
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
