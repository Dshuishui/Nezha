#!/bin/bash
# 快照路径的崩溃恢复：传到一半崩、装到一半崩。
#
# 为什么不放进 crash-recovery.sh：那个脚本的前提是**单节点**（它测的是 GC 与分区装载的
# 恢复路径），而快照必须有两个节点才走得到。两套前提混在一个脚本里，读的人要先分辨哪个
# 场景用几个节点才能看懂判据。
#
# 两个场景，失败方式不同，所以分开测：
#   E 传到一半崩   接收端正在重组分块时 kill -9 → 收件目录里留着半份快照。
#                  重启必须把它**整份丢弃**（不做断点续传），退回原来的状态，
#                  再由 leader 重发。三家业界实现都选幂等可重来而不是可续传：
#                  TiKV 接收端崩了从头重放，CockroachDB 是一次原子 ingest-and-excise。
#   F 装到一半崩   文件都已落位、状态文件尚未写时 kill -9。状态文件是**最后**写的，
#                  所以这一刻之前崩等于这份快照没装过：节点重启后仍是原来的状态，
#                  落位的那批文件成为孤儿、由启动时的清理带走，leader 再重发一次。
#
# 两个窗口的进入方式刻意不同：
#   E 靠 **-snapshotRateMB 1** 把传输从 0.2 秒拉长到十几秒。那是生产旋钮，不是测试开关——
#     能少一个钩子就少一个。
#   F 靠 NEZHA_SNAP_INSTALL_PAUSE_MS。一次安装只有几十毫秒，从外面轮询抓不到。
#
# 判据两档：逐条校验（抽样，证明"读得对"）+ lost-keys.py（全量数盘上的 key，
# 是"一条没丢"的唯一凭据）。gate-audit.sh 里算过：0.07% 的损坏对 2.5% 的抽样
# 期望命中 0.36 条，小规模丢失只能靠全量判据。
#
# 用法: bash scripts/test/snapshot-crash.sh [E] [F]   默认全跑
# 环境变量: ENTRIES=60000 VSIZE=256 PARTITION_MB=2 GC_GB=0.01
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[SNAP]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
ok(){   echo -e "${GREEN}[ 好 ]${NC} $*"; }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }
fail(){ echo -e "${RED}[FAIL]${NC} $*"; FAILED=$((FAILED+1)); }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || exit 1
# cgo 环境（RocksDB 的头与库）集中在 scripts/lib/cgo-env.sh 一份。
# 原先这里写死了三个系统路径 + -I/usr/include，在实验机上会"找到错的版本"，
# 报错出现在 cgo 阶段、读起来像代码问题。理由见那个文件。
# shellcheck source=scripts/lib/cgo-env.sh
# 用 $PROJECT_DIR 而不是 $(dirname "$0")：这一行在 `cd "$PROJECT_DIR"` **之后**，
# 而 $0 是相对路径（`bash ./snapshot-crash.sh`），cd 一发生它就失效——十个脚本
# 全都中了，实测报 "./../lib/cgo-env.sh: No such file or directory"。
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || { echo "cgo 环境准备失败（见 scripts/lib/cgo-env.sh）"; exit 1; }

ENTRIES=${ENTRIES:-60000}
VSIZE=${VSIZE:-256}
PARTITION_MB=${PARTITION_MB:-2}
GC_GB=${GC_GB:-0.01}
# 1 MiB/s：17MB 的快照要传十几秒，脚本轮询就能稳定抓到传输中的那一刻
SLOW_RATE=${SLOW_RATE:-1}
P0=${P0:-41400}; I0=${I0:-41410}
FAILED=0
DIRS=(); PIDS=(); LD=-1; VIC=-1

cleanup(){
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && { kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; }; done
    pkill -f sc-node 2>/dev/null
    for d in "${DIRS[@]:-}"; do [ -n "$d" ] && rm -rf "$d"; done
    rm -f /tmp/sc-node /tmp/sc-write /tmp/sc-read
}
trap cleanup EXIT

info "构建 $(git rev-parse --short HEAD)"
# RACE=1 用竞态检测器构建节点（默认关着）。见 snapshot-e2e.sh 的同一段。
RACEFLAG=""; [ "${RACE:-0}" = 1 ] && { RACEFLAG="-race"; info "带 -race 构建节点（会慢很多）"; }
# shellcheck disable=SC2086
go build $RACEFLAG -o /tmp/sc-node ./cmd/nezha/ || { echo "节点编译失败"; exit 1; }
# 写入用 scanverify：它写的 value 由 key 派生，与两个校验工具的口径一致。
# 用 randwrite_goroutine 写（固定生成串）会让每一条都报值错，看起来像快照搬坏了数据。
go build -o /tmp/sc-write ./cmd/bench/scanverify/ || { echo "scanverify 编译失败"; exit 1; }
go build -o /tmp/sc-read ./cmd/bench/readonly/ || { echo "readonly 编译失败"; exit 1; }

PEERS="127.0.0.1:$I0,127.0.0.1:$((I0+1)),127.0.0.1:$((I0+2))"
SERVERS="127.0.0.1:$P0,127.0.0.1:$((P0+1)),127.0.0.1:$((P0+2))"

# start_node <节点号> <数据目录> [env=val ...] [-- 额外参数...]
start_node(){
    local i=$1 d=$2; shift 2
    local envs=() extra=()
    while [ $# -gt 0 ]; do
        case "$1" in
            --) shift; extra=("$@"); break;;
            *)  envs+=("$1"); shift;;
        esac
    done
    # 注意展开方式：`"${arr[@]:-}"` 在数组为空时会展开成**一个空字符串参数**，
    # 节点会把那个空串当成一个 flag 而拒绝启动。所以两个数组都用 +x 形式。
    env ${envs[@]+"${envs[@]}"} /tmp/sc-node \
        -address "127.0.0.1:$((P0+i))" -internalAddress "127.0.0.1:$((I0+i))" \
        -peers "$PEERS" -data "$d" -system nezha -gcThresholdGB "$GC_GB" \
        -partitionTargetMB "$PARTITION_MB" -snapshotRateMB "$SLOW_RATE" \
        -commitTimeoutS 60 ${extra[@]+"${extra[@]}"} >> "$d/n.log" 2>&1 &
    PIDS[$i]=$!
}

# 起集群、写数据、跑出至少一轮 GC，然后抹掉一个 follower 的数据目录。
# 结果放进全局 LD / VIC，**不能**用 $(...) 取回：命令替换跑在子 shell 里，
# DIRS 与 PIDS 的赋值不会传回父 shell（第一版就是这么写的，第一处用 DIRS 的地方
# 直接报 unbound variable）。
setup_cluster(){
    pkill -f sc-node 2>/dev/null; sleep 2
    PIDS=(); DIRS=()
    for i in 0 1 2; do
        local d; d=$(mktemp -d); DIRS[$i]="$d"
        start_node "$i" "$d"
    done
    for _ in $(seq 1 40); do
        grep -lq -- "Candidate -> Leader" "${DIRS[0]}/n.log" "${DIRS[1]}/n.log" "${DIRS[2]}/n.log" 2>/dev/null && break
        sleep 1
    done
    local ld=-1
    for i in 0 1 2; do grep -q -- "Candidate -> Leader" "${DIRS[$i]}/n.log" 2>/dev/null && ld=$i; done
    [ "$ld" -ge 0 ] || return 1
    /tmp/sc-write -dnums "$ENTRIES" -vsize "$VSIZE" -servers "$SERVERS" -sample 5 >/dev/null 2>&1
    for _ in $(seq 1 12); do
        sleep 5
        [ "$(grep -c "垃圾回收完成" "${DIRS[$ld]}/n.log" 2>/dev/null || true)" -ge 1 ] && break
    done
    [ "$(grep -c "垃圾回收完成" "${DIRS[$ld]}/n.log" 2>/dev/null || true)" -ge 1 ] || return 1
    local vic=-1
    for i in 0 1 2; do [ "$i" != "$ld" ] && vic=$i && break; done
    kill -9 "${PIDS[$vic]}" 2>/dev/null; wait "${PIDS[$vic]}" 2>/dev/null
    rm -rf "${DIRS[$vic]}"/data "${DIRS[$vic]}"/n.log
    LD=$ld; VIC=$vic
}

# 校验一个节点上的数据：抽样 + 全量。$1 = 节点号
verify_node(){
    local i=$1 label=$2
    local out
    out=$(/tmp/sc-read -servers "127.0.0.1:$((P0+i))" -dnums "$ENTRIES" -vsize "$VSIZE" \
            -check 400 -sample 30 2>&1)
    echo "$out" | grep -E "校验" | sed 's/^/       /'
    grep -q FAILOVER_VERIFY_OK <<<"$out" || { fail "$label: 逐条校验没通过"; return 1; }
    local lk n
    lk=$(python3 scripts/bench/lost-keys.py "${DIRS[$i]}" "$ENTRIES" "$VSIZE" 2>&1)
    grep -E "写入 |解析不了|不适用" <<<"$lk" | sed 's/^/       /'
    n=$(grep -o '丢失 [0-9]*' <<<"$lk" | grep -o '[0-9]*' | tail -1)
    [ -n "$n" ] || { fail "$label: lost-keys.py 没给出条数（工具出错，不是被测系统丢数据）"; return 1; }
    [ "$n" = 0 ] || { fail "$label: 盘上丢了 $n 条"; return 1; }
    return 0
}

wait_for(){ # wait_for <文件> <关键字> <最多多少个 2 秒>
    local f=$1 k=$2 t=$3
    for _ in $(seq 1 "$t"); do
        grep -q "$k" "$f" 2>/dev/null && return 0
        sleep 2
    done
    return 1
}

# ---------- 场景 E：传到一半崩 ----------
scenario_E(){
    info "=== E：快照传到一半，接收端 kill -9 ==="
    setup_cluster || { fail "E: 集群没起来或 GC 没跑"; return; }
    local ld=$LD vic=$VIC
    info "leader = node$((ld+1))，被抹掉的是 node$((vic+1))；传输限速 ${SLOW_RATE} MiB/s"

    start_node "$vic" "${DIRS[$vic]}" -- -leaderCheck=false
    wait_for "${DIRS[$ld]}/n.log" "开始给它发快照" 45 || { fail "E: leader 没发起快照"; return; }
    info "传输已开始，等接收端落下第一批分块"
    local inc="${DIRS[$vic]}/data/incoming"
    local hit=0
    for _ in $(seq 1 40); do
        if [ -d "$inc" ] && [ -n "$(ls -A "$inc" 2>/dev/null)" ]; then hit=1; break; fi
        sleep 0.5
    done
    [ "$hit" -eq 1 ] || { fail "E: 收件目录里一直没东西，限速可能没生效"; return; }
    local half; half=$(du -sk "$inc" 2>/dev/null | cut -f1)
    info "收件目录已有 ${half}KB，现在 kill -9 接收端"
    kill -9 "${PIDS[$vic]}" 2>/dev/null; wait "${PIDS[$vic]}" 2>/dev/null

    # 半份快照必须还在盘上——否则这一刻没抓到，场景不成立
    [ -n "$(ls -A "$inc" 2>/dev/null)" ] || { fail "E: kill 之后收件目录是空的，没抓到传输中的窗口"; return; }
    ok "抓到窗口：盘上留着半份快照（${half}KB）"

    # 重启：半成品必须被整份丢弃，然后由 leader 重发
    start_node "$vic" "${DIRS[$vic]}" -- -leaderCheck=false
    sleep 5
    if [ -n "$(ls -A "$inc" 2>/dev/null)" ]; then
        # 也可能是重发的那一份已经开始收了，所以只在"启动时没清"这件事上判失败
        grep -q "装好一份" "${DIRS[$vic]}/n.log" 2>/dev/null || \
            warn "E: 收件目录非空——可能是重发已开始，继续看最终结果"
    fi
    wait_for "${DIRS[$vic]}/n.log" "装好一份" 60 || { fail "E: 重启之后没能靠重发的快照补齐"; return; }
    grep -o "\[SNAPSHOT\] 装好一份.*" "${DIRS[$vic]}/n.log" | tail -1 | cut -c1-190 | sed 's/^/       /'
    sleep 5
    verify_node "$vic" "E" && ok "E: 半份快照被丢弃，重发之后完整装上，盘上丢失 0"
}

# ---------- 场景 F：装到一半崩 ----------
scenario_F(){
    info "=== F：文件已落位、状态文件未写时 kill -9 ==="
    setup_cluster || { fail "F: 集群没起来或 GC 没跑"; return; }
    local ld=$LD vic=$VIC
    info "leader = node$((ld+1))，被抹掉的是 node$((vic+1))"

    # 带钩子重启：安装会在"文件落位、状态文件未写"处停 20 秒
    start_node "$vic" "${DIRS[$vic]}" NEZHA_SNAP_INSTALL_PAUSE_MS=20000 -- -leaderCheck=false
    wait_for "${DIRS[$vic]}/n.log" "state file not written" 60 || { fail "F: 没进到安装窗口"; return; }
    ok "抓到窗口：$(grep -o '\[GC-PAUSE\].*' "${DIRS[$vic]}/n.log" | tail -1)"
    # 落位的文件应当已经在盘上，而状态文件还指着旧状态
    local snapfiles; snapfiles=$(ls "${DIRS[$vic]}"/data/valuelog/*snap* 2>/dev/null | wc -l)
    [ "$snapfiles" -ge 1 ] || { fail "F: 窗口里盘上没有落位的快照文件"; return; }
    info "盘上已落位 $snapfiles 个快照文件，状态文件尚未写"
    kill -9 "${PIDS[$vic]}" 2>/dev/null; wait "${PIDS[$vic]}" 2>/dev/null

    # 重启（不带钩子）：必须能起来，而且应当清掉孤儿产物，再靠重发的快照装好
    start_node "$vic" "${DIRS[$vic]}" -- -leaderCheck=false
    sleep 6
    grep -q "清理上次未完成安装留下的孤儿产物" "${DIRS[$vic]}/n.log" 2>/dev/null \
        && ok "重启时清掉了孤儿产物：$(grep -o '清理上次未完成安装留下的孤儿产物.*' "${DIRS[$vic]}/n.log" | tail -1)" \
        || warn "F: 没看到孤儿清理那一行（该节点被抹过数据，可能本来就没有可引用的旧状态）"
    wait_for "${DIRS[$vic]}/n.log" "装好一份" 60 || { fail "F: 重启之后没能装上重发的快照"; return; }
    grep -o "\[SNAPSHOT\] 装好一份.*" "${DIRS[$vic]}/n.log" | tail -1 | cut -c1-190 | sed 's/^/       /'
    sleep 5
    verify_node "$vic" "F" && ok "F: 半途安装不生效，重发之后完整装上，盘上丢失 0"
}


WANT=("$@"); [ ${#WANT[@]} -eq 0 ] && WANT=(E F)
for s in "${WANT[@]}"; do
    case "$s" in
        E) scenario_E;;
        F) scenario_F;;
        *) warn "未知场景 $s";;
    esac
    cleanup
done

echo
if [ "$FAILED" -eq 0 ]; then
    good "快照的崩溃恢复通过（场景 ${WANT[*]}）：崩在哪一步都不会留下半生效的状态"
else
    echo -e "${RED}[FAIL]${NC} $FAILED 个场景未通过"
    exit 1
fi
