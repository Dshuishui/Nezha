#!/bin/bash
# 端到端验证租约读（lease read）。
#
# 单测钉住了"租约短于选举超时""退位撤租约"这些不变量，但钉不住最基本的一件事：
# 心跳循环到底有没有把租约发出来。发不出来的后果是静默的——读会一路退回 ReadIndex，
# 每次多一轮多数派往返，功能完全正常，只是延迟涨一到两个数量级。所以这里要在真实进程上
# 看到 `[LEASE] 取得 leader 租约` 那一行，并验证读确实答得出正确的值。
#
# 四个场景：
#   A) -leaseRead（默认）     取得租约，读成功
#   B) -leaseRead=false       只查 role，读成功（证明新开关没把旧路径弄坏）
#   C) -leaderCheck=false     完全不查，读成功（验证工具走的那条路）
#   D) 三节点（本机三进程）    **只有 leader 拿到租约**，而且是靠多数派回执拿到的
#
# D 是这里唯一覆盖"多数派确认"那条路的场景：单节点时 quorum 等于 1，租约在派出心跳之前
# 就直接发了，per-peer 的回执协程一个都不会跑。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
pass(){ echo -e "${GREEN}[PASS]${NC} $1"; }
warn(){ echo -e "${YEL}[WARN]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || fail "无项目目录"
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && fail "librocksdb.so 未找到，先跑 scripts/setup-env.sh"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

N=${N:-3000}; VSIZE=${VSIZE:-64}; PORT=${PORT:-3098}; IPORT=${IPORT:-30981}

info "构建 ($(git rev-parse --short HEAD))..."
go build -o /tmp/nezha-lease ./cmd/nezha/ || fail "编译失败"
go build -o /tmp/scanverify-lease ./cmd/bench/scanverify/ || fail "scanverify 编译失败"

wait_port_free() {
    for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$1 " || return 0; sleep 1; done
    return 1
}
count_in() { # count_in <文件> <模式>  —— grep -c 无匹配时输出 0 且退出码为 1，不能写成 `|| echo 0`
    local n; n=$(grep -c -- "$2" "$1" 2>/dev/null || true); echo "${n:-0}"
}

pkill -f nezha-lease 2>/dev/null
wait_port_free "$PORT" || fail "端口 $PORT 一直不放开"

# run_case <标签> <额外的节点参数...>
# 把三个计数写进 LEASES / LAPSED / FELLBACK；任何一步不对就直接 fail（不在子 shell 里跑，
# 所以 fail 能真的终止整个脚本，诊断也直接打到终端而不是被命令替换吞掉）。
run_case() {
    local label="$1"; shift
    local D; D=$(mktemp -d)
    /tmp/nezha-lease -address "127.0.0.1:$PORT" -internalAddress "127.0.0.1:$IPORT" \
        -peers "127.0.0.1:$IPORT" -data "$D" -gap 1000000 -gcThresholdGB 999 \
        "$@" > "$D/n.log" 2>&1 &
    local PID=$!
    # 等到真的当选为止，而不是 sleep 一个猜的秒数
    for _ in $(seq 1 40); do grep -q -- "-> Leader" "$D/n.log" 2>/dev/null && break; sleep 1; done
    kill -0 $PID 2>/dev/null || { tail -20 "$D/n.log"; rm -rf "$D"; fail "[$label] 节点未启动"; }

    sleep 3   # 心跳间隔 500ms，给几拍让租约发出来
    LEASES=$(count_in "$D/n.log" "取得 leader 租约")

    local out
    out=$(/tmp/scanverify-lease -servers "127.0.0.1:$PORT" -dnums "$N" -vsize "$VSIZE" 2>&1)
    if ! kill -0 $PID 2>/dev/null; then
        echo "--- 节点日志尾部 ---"; tail -30 "$D/n.log"; rm -rf "$D"; fail "[$label] 节点在读阶段退出"
    fi
    LAPSED=$(count_in "$D/n.log" "租约失效")
    FELLBACK=$(count_in "$D/n.log" "等 apply 追上超时")
    kill $PID 2>/dev/null; wait $PID 2>/dev/null
    wait_port_free "$PORT" || warn "[$label] 端口 $PORT 未及时放开"

    # scanverify 一律以 0 退出，判定只能看它打出来的那几行。
    # getOK 必须大于 0：读被 ErrWrongLeader 全部挡掉时它的 GET 分支只是 continue，
    # 不看这个数就会把"一条都没读到"当成通过。
    local getok; getok=$(grep -oE "GET 校验: 正确 [0-9]+" <<<"$out" | grep -oE "[0-9]+$" || true)
    if ! grep -q "VERIFY_OK" <<<"$out" || [ "${getok:-0}" -eq 0 ]; then
        echo "$out" | tail -20
        cp "$D/n.log" "/tmp/lease-$label.log" 2>/dev/null
        rm -rf "$D"; fail "[$label] 校验未通过（GET 正确 ${getok:-0} 条；节点日志存到 /tmp/lease-$label.log）"
    fi
    rm -rf "$D"
}

info "=== A：-leaseRead（默认档） ==="
run_case A -leaderCheck -leaseRead
[ "$LEASES" -ge 1 ] || fail "A 组一行 [LEASE] 都没有：心跳循环没发放租约，读在一路退回 ReadIndex"
[ "$LAPSED" -eq 0 ] || warn "A 组租约中途失效 $LAPSED 次——单节点上不该发生，检查心跳循环"
[ "$FELLBACK" -eq 0 ] || warn "A 组有 $FELLBACK 次读因为 apply 没追上被拒"
pass "A 取得租约（[LEASE] 行 $LEASES 条），$N 条逐条校验通过"

info "=== B：-leaseRead=false（只查 role） ==="
run_case B -leaderCheck -leaseRead=false
pass "B 通过（租约仍会发放并打日志 $LEASES 条，只是读不看它）"

info "=== C：-leaderCheck=false（完全不查，验证工具那条路） ==="
run_case C -leaderCheck=false
pass "C 通过（[LEASE] 行 $LEASES 条）"

info "=== D：三节点，只有 leader 该拿到租约（覆盖多数派回执那条路） ==="
P3=${P3:-41081}; I3=${I3:-40001}
PEERS="127.0.0.1:$I3,127.0.0.1:$((I3+1)),127.0.0.1:$((I3+2))"
DIRS=()
for i in 0 1 2; do
    d=$(mktemp -d); DIRS+=("$d")
    /tmp/nezha-lease -address "127.0.0.1:$((P3+i))" -internalAddress "127.0.0.1:$((I3+i))" \
        -peers "$PEERS" -data "$d" -gap 1000000 -gcThresholdGB 999 \
        -leaderCheck -leaseRead > "$d/n.log" 2>&1 &
done
cleanup3(){ pkill -f nezha-lease 2>/dev/null; rm -rf "${DIRS[@]}"; }
for _ in $(seq 1 40); do grep -lq -- "-> Leader" "${DIRS[0]}/n.log" "${DIRS[1]}/n.log" "${DIRS[2]}/n.log" 2>/dev/null && break; sleep 1; done
sleep 6   # 心跳 500ms；给几拍让多数派回执把租约发出来

holders=0; elected=0
for i in 0 1 2; do
    g=$(count_in "${DIRS[$i]}/n.log" "取得 leader 租约")
    e=$(count_in "${DIRS[$i]}/n.log" "Candidate -> Leader")
    x=$(count_in "${DIRS[$i]}/n.log" "租约失效")
    echo "       node$((i+1))  当选=$e  取得租约=$g  租约失效=$x"
    [ "$g" -gt 0 ] && holders=$((holders+1))
    [ "$e" -gt 0 ] && elected=$((elected+1))
done
[ "$elected" -eq 1 ] || { cleanup3; fail "D 有 $elected 个节点当选，三节点应当只有一个"; }
[ "$holders" -eq 1 ] || { cleanup3; fail "D 有 $holders 个节点持有租约，必须恰好是 1（0 = 多数派回执没被计数，>1 = 安全性破了）"; }

out=$(/tmp/scanverify-lease -servers "127.0.0.1:$P3,127.0.0.1:$((P3+1)),127.0.0.1:$((P3+2))" \
      -dnums "$N" -vsize "$VSIZE" 2>&1)
getok=$(grep -oE "GET 校验: 正确 [0-9]+" <<<"$out" | grep -oE "[0-9]+$" || true)
if ! grep -q "VERIFY_OK" <<<"$out" || [ "${getok:-0}" -eq 0 ]; then
    echo "$out" | tail -20; cleanup3; fail "D 校验未通过（GET 正确 ${getok:-0} 条）"
fi
cleanup3
pass "D 通过：恰好一个节点当选并持有租约，$N 条经客户端重定向逐条校验通过"

rm -f /tmp/nezha-lease /tmp/scanverify-lease
echo ""; pass "租约读端到端验证通过：租约真的发出来了（含多数派回执那条路），各档读路径都答得出正确的值"
