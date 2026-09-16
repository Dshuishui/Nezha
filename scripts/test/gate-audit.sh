#!/bin/bash
# 校验闸门自审：往盘上注入一个**已知的**故障，看每个闸门是不是真的会判失败。
#
# 为什么要有这个脚本：过去几轮里，测量与校验工具链反复出现两类失效，而且都是静默的——
#
#   工具坏了看起来像系统坏了：lost-keys.py 把 key 宽度写死成 10，宽度改成 24 之后解析
#     抛异常，crash-recovery.sh 拿到空串，把 A/B/D 报成"盘上丢了 ? 条记录"。
#   系统坏了工具说没事：countkeys 把存储层的 applied index 那一行算成用户 key，于是
#     KEYCOUNT 恒比写入条数多 1，而 verify-goodput.sh 做的是精确相等比较——
#     "数据完好"和"确实丢了"两个分支都不成立，判定一直落在兜底分支上。
#
# 通过的实验数字全部建立在这些闸门"能判失败"这个前提上，而那个前提一直没有被验证过。
# 这里逐个验证：**每个场景都期望闸门报失败**，闸门报通过才是本脚本的失败。
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[AUDIT]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
bad(){  echo -e "${RED}[漏报]${NC} $*"; FAILED=$((FAILED+1)); }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
FAILED=0

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || die "无项目目录"
# cgo 环境（RocksDB 的头与库）集中在 scripts/lib/cgo-env.sh 一份。
# 原先这里写死了三个系统路径 + -I/usr/include，在实验机上会"找到错的版本"，
# 报错出现在 cgo 阶段、读起来像代码问题。理由见那个文件。
# shellcheck source=scripts/lib/cgo-env.sh
. "$(dirname "$0")/../lib/cgo-env.sh"
setup_cgo_env || die "cgo 环境准备失败"

N=${N:-20000}; VSIZE=${VSIZE:-256}; PORT=${PORT:-3102}; IPORT=${IPORT:-31021}
PART_MB=${PART_MB:-2}; GCGB=${GCGB:-0.004}

info "构建 $(git rev-parse --short HEAD)"
go build -o /tmp/ga-node ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/ga-verify ./cmd/bench/scanverify/ || die "scanverify 编译失败"
go build -o /tmp/ga-readonly ./cmd/bench/readonly/ || die "readonly 编译失败"

wait_port(){ for _ in $(seq 1 30); do ss -ltn 2>/dev/null | grep -q ":$1 " || return 0; sleep 1; done; return 1; }
pkill -f ga-node 2>/dev/null; wait_port "$PORT" || die "端口 $PORT 不放开"

# start <数据目录> [额外参数...] —— 回显 pid，起不来回显空
start(){
    local d="$1"; shift
    /tmp/ga-node -address "127.0.0.1:$PORT" -internalAddress "127.0.0.1:$IPORT" \
        -peers "127.0.0.1:$IPORT" -data "$d" -gap 100000000 -system nezha \
        -gcThresholdGB "$GCGB" -partitionTargetMB "$PART_MB" "$@" >> "$d/n.log" 2>&1 &
    local pid=$!
    for _ in $(seq 1 40); do
        grep -q -- "-> Leader" "$d/n.log" 2>/dev/null && { echo "$pid"; return; }
        kill -0 $pid 2>/dev/null || { echo ""; return; }
        sleep 1
    done
    echo "$pid"
}
stop(){ [ -n "${1:-}" ] && { kill "$1" 2>/dev/null; wait "$1" 2>/dev/null; }; wait_port "$PORT" || true; }

# ---------- 准备一个跑过 GC、有多个分区的数据目录 ----------
info "准备基线数据（$N 条 × ${VSIZE}B，分区 ${PART_MB}MB，GC 阈值 ${GCGB}GB）"
BASE=$(mktemp -d); : > "$BASE/n.log"
PID=$(start "$BASE")
[ -n "$PID" ] || { tail -20 "$BASE/n.log"; die "基线节点起不来"; }
OUT=$(/tmp/ga-verify -servers "127.0.0.1:$PORT" -dnums "$N" -vsize "$VSIZE" 2>&1)
grep -q VERIFY_OK <<<"$OUT" || { echo "$OUT" | tail -10; stop "$PID"; die "基线写入/校验就没通过，后面的注入没有意义"; }
for _ in $(seq 1 60); do [ "$(grep -c '垃圾回收完成' "$BASE/n.log")" -ge 1 ] && break; sleep 2; done
GCN=$(grep -c '垃圾回收完成' "$BASE/n.log" || true)
[ "${GCN:-0}" -ge 1 ] || { stop "$PID"; die "GC 一轮都没跑，分区注入无从下手"; }
stop "$PID"
PARTS=$(ls "$BASE"/data/valuelog/*.p* 2>/dev/null | wc -l | tr -d ' ')
info "基线就绪：GC $GCN 轮，$PARTS 个分区，校验通过"
good "闸门前置条件成立（scanverify 在无故障时报 VERIFY_OK）"

# snapshot 复制一份基线数据供注入。
#
# 本脚本第一版在这里栽了一次，值得留个记号：清单当时存的是**绝对路径**，所以副本一启动
# 读的是原目录的文件，注入到副本上的故障全部"没有效果"，脚本报了 6 项漏报——全是假的。
# 现在清单存文件名、状态文件在装载时 rebase 到当前 -data，副本才真的是独立的一份。
# 这里顺手把它验一遍：副本起来之后打开的描述符必须都在副本目录里。
snapshot(){ local d; d=$(mktemp -d); cp -a "$BASE/." "$d/"; echo "$d"; }

# assert_reads_own_dir <pid> <目录> —— 节点必须只读自己目录下的文件
assert_reads_own_dir(){
    local pid="$1" d="$2" foreign
    foreign=$(ls -l /proc/"$pid"/fd 2>/dev/null | grep -oE "/tmp/tmp[^ ]*/data/valuelog/[^ ]*" \
              | grep -v "^$d/" | sort -u | head -3)
    [ -z "$foreign" ] && return 0
    bad "节点以 -data $d 启动，却打开了别的目录的文件：$(echo "$foreign" | tr '\n' ' ')"
}

# ---------- 场景 1：同长度篡改分区内容 ----------
# 这一处正是持久化稀疏索引换走的那层校验：扫描重建会因为记录框架解析不下去而发现它，
# 读旁挂索引则不会。所以这里要同时验证"-verifyPartitions 抓得到"和"默认路径抓不到"，
# 后者不是漏报，是已知取舍——但它必须由**别的闸门**（逐条校验）兜住。
info "=== 场景 1：同长度篡改一个分区的内容 ==="
D=$(snapshot); VICTIM=$(ls "$D"/data/valuelog/*.p0 2>/dev/null | head -1)
[ -n "$VICTIM" ] || die "找不到 .p0 分区"
SZ=$(stat -c %s "$VICTIM")
dd if=/dev/zero of="$VICTIM" bs=1 seek=$((SZ/2)) count=4096 conv=notrunc status=none
: > "$D/n.log"
PID=$(start "$D" -verifyPartitions)
if [ -z "$PID" ]; then
    good "-verifyPartitions 拒绝启动：$(grep -oE 'manifest says [0-9]+ bytes, parsed [0-9]+|旁挂索引与扫描结果不一致' "$D/n.log" | head -1)"
else
    bad "-verifyPartitions 竟然起来了——同长度篡改没被扫描发现"
    stop "$PID"
fi
: > "$D/n.log"
PID=$(start "$D")
if [ -z "$PID" ]; then
    good "默认路径也拒绝了启动（比预期更严）"
else
    assert_reads_own_dir "$PID" "$D"
    OUT=$(/tmp/ga-readonly -servers "127.0.0.1:$PORT" -dnums "$N" -vsize "$VSIZE" -check 500 -sample 40 2>&1)
    echo "$OUT" | grep -E '校验' | sed 's/^/       /'
    # 抽样闸门**抽不到**这么小的损坏，这是算得出来的、不是缺陷：
    # 4096 字节 ≈ 14 条记录 = 0.07% 的数据，抽查 500/20000 = 2.5%，
    # 期望命中 0.36 条。所以这里不要求它报错——真正该兜住的是全量覆盖的 lost-keys.py。
    if grep -q FAILOVER_VERIFY_OK <<<"$OUT"; then
        warn "逐条校验没抓到（抽样期望命中 0.36 条，抽不到是预期）——全量判据看下一行"
    else
        good "逐条校验恰好抽到了：$(grep -oE '错误 [0-9]+|取不到 [0-9]+' <<<"$OUT" | tr '\n' ' ')"
    fi
    LKOUT=$(python3 scripts/bench/lost-keys.py "$D" "$N" "$VSIZE" 2>&1)
    LK=$(grep -oE '丢失 [0-9]+' <<<"$LKOUT" | grep -oE '[0-9]+' || true)
    BROKEN=$(grep -oE '\*\*[0-9]+ 条记录解析不了' <<<"$LKOUT" | grep -oE '[0-9]+' || true)
    if [ -z "${LK:-}" ]; then
        bad "lost-keys.py 没给出条数（工具本身又坏了）：$(tail -2 <<<"$LKOUT" | tr '\n' ' ')"
    elif [ -n "${BROKEN:-}" ] && [ "$BROKEN" -gt 0 ]; then
        good "lost-keys.py 报出 $BROKEN 条记录解析不了、丢失 $LK 条 —— 全量判据抓到了损坏"
    elif [ "$LK" -gt 0 ]; then
        good "lost-keys.py 报丢失 $LK 条"
    else
        bad "lost-keys.py 报丢失 0 且没提到解析失败 —— 4KB 清零它一点没看见"
    fi
    stop "$PID"
fi
rm -rf "$D"

# ---------- 场景 2：截断分区一个字节 ----------
info "=== 场景 2：截断一个分区一个字节（清单长度检查） ==="
D=$(snapshot); VICTIM=$(ls "$D"/data/valuelog/*.p0 2>/dev/null | head -1)
truncate -s -1 "$VICTIM"; : > "$D/n.log"
PID=$(start "$D")
if [ -z "$PID" ]; then
    good "启动被拒绝：$(grep -oE 'manifest says [0-9]+ bytes, file has [0-9]+' "$D/n.log" | head -1)"
else
    bad "截断一个字节，节点照样起来了 —— 清单长度检查没生效"
    stop "$PID"
fi
rm -rf "$D"

# ---------- 场景 3：删掉一个分区文件 ----------
info "=== 场景 3：整个删掉一个分区文件 ==="
D=$(snapshot); VICTIM=$(ls "$D"/data/valuelog/*.p0 2>/dev/null | head -1)
rm -f "$VICTIM"; : > "$D/n.log"
PID=$(start "$D")
if [ -z "$PID" ]; then
    good "启动被拒绝：$(grep -oE 'partition .*: .*no such file[^"]*' "$D/n.log" | head -1 | cut -c1-90)"
else
    bad "分区文件被删，节点照样起来了 —— 读路径会静默少一段 key 区间"
    stop "$PID"
fi
rm -rf "$D"

# ---------- 场景 4：篡改旁挂索引 ----------
# 旁挂索引坏了必须退回扫描重建，而不是拿着错的索引去服务读。
info "=== 场景 4：篡改旁挂稀疏索引 ==="
D=$(snapshot); IDX=$(ls "$D"/data/valuelog/index/*.idx 2>/dev/null | head -1)
if [ -z "$IDX" ]; then
    bad "没有旁挂索引文件 —— 持久化根本没生效"
else
    ISZ=$(stat -c %s "$IDX")
    dd if=/dev/zero of="$IDX" bs=1 seek=$((ISZ/2)) count=16 conv=notrunc status=none
    : > "$D/n.log"
    PID=$(start "$D")
    if [ -z "$PID" ]; then
        bad "索引坏了就起不来 —— 应当退回扫描重建，而不是拒绝启动"
    else
        if grep -q "旁挂索引不可用，改为扫描重建" "$D/n.log"; then
            good "检出并退回扫描重建：$(grep -oE 'sparse index checksum [0-9a-f]+, want [0-9a-f]+' "$D/n.log" | head -1)"
        else
            bad "索引被篡改却没有任何一行日志提到 —— 可能正拿着错的索引在服务读"
        fi
        OUT=$(/tmp/ga-readonly -servers "127.0.0.1:$PORT" -dnums "$N" -vsize "$VSIZE" -check 300 -sample 30 2>&1)
        grep -q FAILOVER_VERIFY_OK <<<"$OUT" && good "退回扫描后读路径仍然全对" \
            || bad "退回扫描后读路径仍有错：$(grep -oE '错误 [0-9]+|取不到 [0-9]+' <<<"$OUT" | tr '\n' ' ')"
        stop "$PID"
    fi
fi
rm -rf "$D"

# ---------- 场景 5：countkeys 与 verify-goodput 的计数口径 ----------
info "=== 场景 5：countkeys 的计数是否等于写入条数 ==="
go build -o /tmp/ga-count ./cmd/bench/countkeys/ || die "countkeys 编译失败"
# GC 之后存储引擎实例会换目录（newKeyIndex_N），所以按状态文件里的 current_db 找，
# 不要写死 keyIndex——写死的话这一格会被"跳过"而不是被检查。
DB=$(python3 -c "import json,sys; print(json.load(open(sys.argv[1]))['current_db'])" "$BASE/data/kv_state.json" 2>/dev/null)
[ -d "$DB" ] || DB=$(find "$BASE" -maxdepth 3 -type d -name "*eyIndex*" | head -1)
if [ -z "$DB" ]; then
    bad "找不到存储引擎目录（current_db=${DB}）——这一格本来要检查计数口径，跳过就等于没查"
else
    # 这组数据跑过 GC，key 已随 value 迁入分区文件，存储引擎里本就应该是空的。
    # 期望 KEYCOUNT==N 是**错的期望**（本脚本第一版就这么写，报了一个假漏报）。
    # 该验的是两件事：元数据行没被算进去，以及工具自己说清了这个数的含义。
    CNTOUT=$(/tmp/ga-count -db "$DB")
    CNT=$(sed -n 's/^KEYCOUNT \([0-9]*\)/\1/p' <<<"$CNTOUT")
    if grep -q "另有 1 行存储层元数据，未计入" <<<"$CNTOUT"; then
        good "元数据行被正确排除（KEYCOUNT=${CNT}，GC 后 key 已迁入分区，这个数本就该趋近 0）"
    else
        bad "没有排除存储层元数据行 —— KEYCOUNT 会恒比写入条数多 1，而 verify-goodput.sh 做精确相等比较"
    fi
    grep -q "若这组数据跑过 GC" <<<"$CNTOUT" \
        && good "KEYCOUNT=0 时工具自己说清了含义，不会被读成丢数据" \
        || bad "KEYCOUNT=$CNT 但工具没说清这个数在 GC 后的含义"
fi

rm -rf "$BASE"
rm -f /tmp/ga-node /tmp/ga-verify /tmp/ga-readonly /tmp/ga-count
echo
if [ "$FAILED" -eq 0 ]; then
    good "自审通过：注入的每一种故障都至少被一个闸门判出来了"
else
    die "$FAILED 项漏报 —— 这些故障可以在实验里静默发生"
fi
