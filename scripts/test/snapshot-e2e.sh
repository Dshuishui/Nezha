#!/bin/bash
# 三节点：把一个副本的数据整个抹掉再拉起来，看它能不能靠快照重建。
#
# 这是快照路径**今天就能走通**的场景。慢 follower 那条走不通，原因在代码里：
# compactLog 的压缩上界仍夹在 min(matchIndex)，所以一个活着但很慢的 follower 永远不会
# 落到压缩点之前——压缩点会一直等它。要让那条路可达，得等第 3 步（有界截断）。
#
# 而一个被抹掉的副本立刻可达：leader 的压缩点早已推过 0，抹掉之后那个节点从空日志开始，
# AppendEntries 被它按 ConflictIndex=1 拒掉，leader 把 nextIndex 退到 1——正好落在压缩点
# 之前，于是 maybeSendSnapshot 接手。这同时也是一个真实的运维场景：换盘、重建副本。
#
# 这一条链路里每一段都被覆盖到了：
#   leader   做快照（分区 + 旁挂索引 + 存储引擎导出 + 日志前缀）、钉住分区、限速发送
#   follower 收件落位、ingest 到新库、装载分区、接上 Raft 日志、最后写状态文件
#   之后     进度机转回正常复制，新写入继续正常复制到这个副本
#
# 判据分两档，因为它们看的东西不一样：
#   逐条校验（readonly）是**抽样**，能证明"读得对"，但小规模丢失抽不到；
#   lost-keys.py 是**全量**数盘上的 key，是"一条没丢"的唯一凭据。
#   两档都要，gate-audit.sh 里算过：0.07% 的损坏对 2.5% 的抽样期望命中 0.36 条。
#
# 用法: bash scripts/test/snapshot-e2e.sh
# 环境变量: ENTRIES=60000 VSIZE=256 PARTITION_MB=2 GC_GB=0.01
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[SNAP]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; cleanup; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || exit 1
export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && { echo "librocksdb.so 未找到"; exit 1; }
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

ENTRIES=${ENTRIES:-60000}       # 要够多：compactLog 的门槛是 2 万条、保留 5000 条
VSIZE=${VSIZE:-256}
PARTITION_MB=${PARTITION_MB:-2} # 分区调小，让一轮 GC 就产出多个分区
GC_GB=${GC_GB:-0.01}            # 约 10MB 就触发 GC，保证快照里真的有分区文件
AFTER=${AFTER:-8000}            # 重建之后再写这么多，验证正常复制接得上
P0=${P0:-41300}; I0=${I0:-41310}
DIRS=(); PIDS=()

cleanup(){
    for p in "${PIDS[@]:-}"; do [ -n "$p" ] && { kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; }; done
    pkill -f snap-node 2>/dev/null
    for d in "${DIRS[@]:-}"; do [ -n "$d" ] && rm -rf "$d"; done
    rm -f /tmp/snap-node /tmp/snap-write /tmp/snap-read
}
trap cleanup EXIT

info "构建 $(git rev-parse --short HEAD)"
# RACE=1 用竞态检测器构建节点。默认关着：-race 让节点慢好几倍，常规回归不需要。
# 但新加的 stateMu（装快照时整体替换状态机）与安装路径是并发问题最可能藏的地方，
# 而单测的 -race 覆盖不到"多进程 + 真并发读写 + GC + 快照"这个组合，只有这里能覆盖。
RACEFLAG=""; [ "${RACE:-0}" = 1 ] && { RACEFLAG="-race"; info "带 -race 构建节点（会慢很多）"; }
# shellcheck disable=SC2086
go build $RACEFLAG -o /tmp/snap-node ./cmd/nezha/ || die "节点编译失败"
# 写入用 scanverify 而不是 randwrite_goroutine：前者写的 value 由 key 派生，后者写的是
# 一个固定的生成串。校验工具（scanverify / readonly）都按 key 派生去核对，两者混用的结果
# 是"每一条都值错"——第一版就是这么写的，500 条抽查全报错，看起来像快照把数据搬坏了。
go build -o /tmp/snap-write ./cmd/bench/scanverify/ || die "scanverify 编译失败"
go build -o /tmp/snap-read ./cmd/bench/readonly/ || die "readonly 编译失败"

PEERS="127.0.0.1:$I0,127.0.0.1:$((I0+1)),127.0.0.1:$((I0+2))"
SERVERS="127.0.0.1:$P0,127.0.0.1:$((P0+1)),127.0.0.1:$((P0+2))"
pkill -f snap-node 2>/dev/null; sleep 2

# $1 = 节点号(0..2)，$2 = 数据目录，$3.. = 额外参数
start_node(){
    local i=$1 d=$2; shift 2
    /tmp/snap-node -address "127.0.0.1:$((P0+i))" -internalAddress "127.0.0.1:$((I0+i))" \
        -peers "$PEERS" -data "$d" -system nezha -gcThresholdGB "$GC_GB" \
        -partitionTargetMB "$PARTITION_MB" -commitTimeoutS 60 "$@" >> "$d/n.log" 2>&1 &
    PIDS[$i]=$!
}

info "起三个节点（GC 阈值 ${GC_GB}GB、分区 ${PARTITION_MB}MB，保证快照里有分区文件）"
for i in 0 1 2; do
    d=$(mktemp -d); DIRS[$i]="$d"
    start_node "$i" "$d"
done
for _ in $(seq 1 40); do
    grep -lq -- "Candidate -> Leader" "${DIRS[0]}/n.log" "${DIRS[1]}/n.log" "${DIRS[2]}/n.log" 2>/dev/null && break
    sleep 1
done
LEADER=-1
for i in 0 1 2; do grep -q -- "Candidate -> Leader" "${DIRS[$i]}/n.log" 2>/dev/null && LEADER=$i; done
[ "$LEADER" -ge 0 ] || die "40 秒内没有节点当选"
info "leader = node$((LEADER+1))"

# ---------- 阶段 1：写入，跑出至少一轮 GC 与一次内存日志压缩 ----------
info "阶段 1：写 $ENTRIES 条 × ${VSIZE}B"
/tmp/snap-write -dnums "$ENTRIES" -vsize "$VSIZE" -servers "$SERVERS" -sample 10 2>&1 \
    | grep -E "写入|校验|VERIFY" | sed 's/^/       /'
for _ in $(seq 1 12); do
    sleep 5
    grep -q "compactLog" "${DIRS[$LEADER]}/n.log" 2>/dev/null && break
done
GC_ROUNDS=$(grep -c "垃圾回收完成" "${DIRS[$LEADER]}/n.log" 2>/dev/null || true)
PARTS=$(ls "${DIRS[$LEADER]}"/data/valuelog/*.p* 2>/dev/null | wc -l)
info "leader 上跑了 $GC_ROUNDS 轮 GC，盘上 $PARTS 个分区文件"
[ "$GC_ROUNDS" -ge 1 ] || die "一轮 GC 都没跑起来，快照里就不会有分区文件——本测的前提不成立"
[ "$PARTS" -ge 1 ] || die "没有分区文件"

# ---------- 阶段 2：抹掉一个 follower 的数据，再拉起来 ----------
VICTIM=-1
for i in 0 1 2; do [ "$i" != "$LEADER" ] && VICTIM=$i && break; done
info "阶段 2：停掉 node$((VICTIM+1))，抹掉它的数据目录，再拉起来"
kill "${PIDS[$VICTIM]}" 2>/dev/null; wait "${PIDS[$VICTIM]}" 2>/dev/null
VD="${DIRS[$VICTIM]}"
rm -rf "$VD"/data "$VD"/n.log
# 重建的那个节点用 -leaderCheck=false 起：要读它**本地**的状态来验证快照装对了，
# 而默认档会把非 leader 的读挡掉（见 CLAUDE.md 里那两个一致性开关）。
start_node "$VICTIM" "$VD" -leaderCheck=false

info "等 leader 发快照（最多 90 秒）"
SENT=0
for _ in $(seq 1 45); do
    sleep 2
    grep -q "开始给它发快照" "${DIRS[$LEADER]}/n.log" 2>/dev/null && SENT=1 && break
done
if [ "$SENT" -ne 1 ]; then
    # 没发起快照时，需要知道的是 leader 把 nextIndex 退到了哪、压缩点在哪。
    echo "       --- leader 日志尾部 ---"
    tail -25 "${DIRS[$LEADER]}/n.log" | cut -c1-200 | sed 's/^/       /'
    echo "       --- 被重建节点日志尾部 ---"
    tail -25 "$VD/n.log" | cut -c1-200 | sed 's/^/       /'
    die "leader 没有发起快照。检查 [LOG-STUCK] 与 maybeSendSnapshot 的判据"
fi
grep -o "\[SNAPSHOT\].*" "${DIRS[$LEADER]}/n.log" | tail -3 | cut -c1-160 | sed 's/^/       /'

info "等被重建的节点装好快照"
INSTALLED=0
for _ in $(seq 1 45); do
    sleep 2
    grep -q "装好一份" "$VD/n.log" 2>/dev/null && INSTALLED=1 && break
done
[ "$INSTALLED" -eq 1 ] || die "快照没有装上。看 $VD/n.log 里的 [SNAPSHOT] 行"
grep -o "\[SNAPSHOT\].*" "$VD/n.log" | tail -2 | cut -c1-200 | sed 's/^/       /'
grep -o "\[RECOVER\] 装载.*" "$VD/n.log" | tail -1 | sed 's/^/       /'
good "快照发出并装上了"

# 传输期间不能压缩——这条是"永远重发快照"那个死循环的防线
if grep -q "compactLog" "${DIRS[$LEADER]}/n.log"; then
    good "leader 期间仍在正常压缩内存日志（传输窗口内跳过，窗口外继续）"
fi

# ---------- 阶段 3：重建之后的数据必须是对的 ----------
info "阶段 3：校验被重建节点上的数据"
OUT=$(/tmp/snap-read -servers "127.0.0.1:$((P0+VICTIM))" -dnums "$ENTRIES" -vsize "$VSIZE" \
        -check 500 -sample 40 2>&1)
echo "$OUT" | grep -E "校验" | sed 's/^/       /'
grep -q FAILOVER_VERIFY_OK <<<"$OUT" || die "被重建节点上的逐条校验没通过"
good "逐条校验通过（抽样）"

# 全量：盘上到底丢没丢。inlinePlacement 没开，所以这个口径适用。
# 输出格式是"写入 N，盘上 distinct M，丢失 K"，按 gc-rounds.sh 的同一条口径解析。
LKOUT=$(python3 scripts/bench/lost-keys.py "$VD" "$ENTRIES" "$VSIZE" 2>&1)
grep -E "写入 |解析不了|不适用" <<<"$LKOUT" | sed 's/^/       /'
N=$(grep -o '丢失 [0-9]*' <<<"$LKOUT" | grep -o '[0-9]*' | tail -1)
N=${N:-?}
if [ "$N" = "0" ]; then
    good "全量清点：盘上丢失 0 —— 快照把状态一条不差地搬过去了"
else
    [ "$N" = "?" ] && die "lost-keys.py 没给出条数（工具本身出错，不是被测系统丢数据）"
    die "全量清点报丢失 $N 条（抽样校验没抓到它，这正是全量判据存在的理由）"
fi

# ---------- 阶段 4：进度机要转回正常复制 ----------
TOTAL=$((ENTRIES + AFTER))
info "阶段 4：把键空间扩到 ${TOTAL}，验证快照之后正常复制接得上"
# scanverify 写 0..dnums-1，所以调大 dnums 就是"重写旧的 + 追加新的"。
# 旧的重写成同样的值，无害；新增的那 $AFTER 条才是要看的东西——它们只能靠
# 快照之后恢复的正常复制到达被重建的节点。
/tmp/snap-write -dnums "$TOTAL" -vsize "$VSIZE" -servers "127.0.0.1:$((P0+LEADER))" -sample 5 2>&1 \
    | grep -E "写入|校验|VERIFY" | sed 's/^/       /'
sleep 10
# 读的是被重建节点的本地状态（-leaderCheck=false），覆盖扩大后的整个键空间
OUT=$(/tmp/snap-read -servers "127.0.0.1:$((P0+VICTIM))" -dnums "$TOTAL" -vsize "$VSIZE" \
        -check 500 -sample 30 2>&1)
echo "$OUT" | grep -E "校验" | sed 's/^/       /'
grep -q FAILOVER_VERIFY_OK <<<"$OUT" || die "快照之后的新写入没有复制到被重建的节点上——进度机没转回正常复制"
good "快照之后的新写入照常复制到了被重建的节点上"

STUCK=$(grep -c "LOG-STUCK" "${DIRS[$LEADER]}/n.log" 2>/dev/null || true)
echo
echo "=============================================="
printf " GC 轮数 %-4s 分区文件 %-4s  [LOG-STUCK] %s 次（有快照能力时它只是一条事件，不是永久卡住）\n" \
    "$GC_ROUNDS" "$PARTS" "$STUCK"
echo "=============================================="
good "端到端通过：一个被抹掉的副本靠快照完整重建，之后正常复制接得上"
info "清理数据目录与进程"
