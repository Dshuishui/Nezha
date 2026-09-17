#!/bin/bash
# 范围扫描与 GC 换库并发跑：单节点，写入不停、扫描不停，GC 阈值调到每几秒一轮。
#
# 这个用例针对的是一处**具体**的风险。此前 scanNewFile 整段迭代都持 kvs.mu，于是
# GC 的收尾（finishFirstGC / finishAnotherGC 都先取 kvs.mu，之后才走
# removeSupersededStore → oldPersister.Close()）必然排在扫描之后。代价是 applyLoop
# 用的是同一把锁，一次扫描把写入按住整段扫描时长（实测见 KVServer.storeRetireMu）。
# 改成 storeRetireMu 之后 apply 不再被挡，而"迭代器用在已关闭的 RocksDB 上"这件事
# 就完全靠那把新锁挡着了。**它挡不住的后果不是 Go panic，是 C++ 层的 use-after-free**：
# 可能直接段错误，也可能静默返回垃圾。所以要有一个专门把这两件事撞在一起的用例。
#
# 与 gc-rounds.sh 的区别：那个是"写一遍、校验一遍"顺序跑，扫描落在 GC 换库那一瞬间的
# 概率很低；这里让两个客户端**同时**持续跑，并且把 GC 阈值压到几秒一轮，好让重叠
# 在统计上必然发生。
#
# 用法: bash scripts/test/scan-during-gc.sh
# 环境变量: ENTRIES=200000 VSIZE=256 DURATION=180 PARTITION_MB=2 MIN_ROUNDS=3 MIN_SCANS=50
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[TEST]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
ok(){   echo -e "${GREEN}[ OK ]${NC} $*"; }

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "${REPO_DIR:-$SCRIPT_DIR/../..}" || die "无项目目录"
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

ENTRIES="${ENTRIES:-200000}"
VSIZE="${VSIZE:-256}"
DURATION="${DURATION:-180}"
PARTITION_MB="${PARTITION_MB:-2}"
MIN_ROUNDS="${MIN_ROUNDS:-3}"
MIN_SCANS="${MIN_SCANS:-50}"
DATA="${DATA:-$TMPDIR/scan-during-gc}"
ADDR=127.0.0.1:3097
IADDR=127.0.0.1:30971
BIN=/tmp/nezha-sdg

DATASET=$(( ENTRIES * (20 + 10 + VSIZE) ))
# 阈值取数据集的 1/8：写入一直在覆盖同一批 key，所以垃圾很快堆满，几秒就一轮。
GCGB=$(awk -v b="$DATASET" 'BEGIN{printf "%.9f", b/8/1073741824}')

info "构建 $(git rev-parse --short HEAD)"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
for t in scanverify scan_pro randwrite_goroutine; do
  go build -o "/tmp/sdg-$t" "./cmd/bench/$t/" || die "$t 编译失败"
done

PID=""; WPID=""; SPID=""
cleanup(){
  for p in "$WPID" "$SPID"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null
}
trap cleanup EXIT

rm -rf "$DATA"; mkdir -p "$DATA"
nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
    -data "$DATA" -gap 100000000 -commitTimeoutS 60 -system nezha \
    -gcThresholdGB "$GCGB" -partitionTargetMB "$PARTITION_MB" \
    < /dev/null > "$DATA/n.log" 2>&1 &
PID=$!
for _ in $(seq 1 30); do sleep 1; grep -q '\[SYSTEM\]' "$DATA/n.log" && break; done
kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点未启动"; }

rounds(){ grep -c '轮垃圾回收完成' "$DATA/n.log" 2>/dev/null || echo 0; }

info "先写入并逐条校验 ${ENTRIES} 条 × ${VSIZE}B（GC 阈值 $(awk -v g=$GCGB 'BEGIN{printf "%.1f", g*1024}')MB，分区 ${PARTITION_MB}MB）"
/tmp/sdg-scanverify -servers "$ADDR" -leader 0 -dnums "$ENTRIES" -vsize "$VSIZE" \
    -span 50 -sample 20 > "$DATA/seed.out" 2>&1
grep -q VERIFY_OK "$DATA/seed.out" || { tail -5 "$DATA/seed.out"; die "初始写入或校验失败"; }
R0=$(rounds)

# ---- 并发窗口：写不停 + 扫不停 ----
# 扫描范围取 ENTRIES/4，够大到一次扫描要几百毫秒（这样它更容易横跨一次 GC 换库），
# 又不至于大到整个窗口只跑得下几次。
GAP=$(( ENTRIES / 4 ))
info "并发 ${DURATION}s：覆盖写（20 客户端）+ 连续扫描（gapkey=${GAP}）"
# 覆盖写：keyspace=ENTRIES 且 dist=uniform，所以每一条都在覆盖一个已有 key，
# 制造的垃圾量与写入量相当——这才是 GC 反复触发的来源。
# dnums 给得远超窗口所需，窗口结束时由 cleanup 杀掉；这两个客户端的输出只用来
# 数"扫了多少次"，不出延迟数字，所以被信号打断没关系。
nohup /tmp/sdg-randwrite_goroutine -cnums 20 -dnums $(( ENTRIES * 50 )) -vsize "$VSIZE" \
    -keyspace "$ENTRIES" -dist uniform -servers "$ADDR" \
    < /dev/null > "$DATA/w.out" 2>&1 &
WPID=$!
# **扫描次数必须数得出来，否则判据 b 只是个装饰。** 第一版写的是
# `-dnums 1000000 -tests 1`，然后数 s.out 里带 scan/范围 的行——而 scan_pro
# **每次扫描什么都不打**，只在每个 test 结束时打一行 `Test N: elapse:...`。
# 于是那 2 行其实是它的表头，判据 b 报"只扫了 2 次"，而扫描一直在跑。
# 2026-09-18 实测：36 轮 GC 已经跑出来了，用例却在前提那一步失败。
# 改成每 SDNUMS 次扫描算一个 test，于是 `^Test ` 的行数 × SDNUMS 就是完成次数；
# 中途被杀最多丢掉当前这个未完成的 test。
SDNUMS=10
nohup /tmp/sdg-scan_pro -cnums 1 -dnums "$SDNUMS" -tests 100000 -rest 0 \
    -gapkey "$GAP" -keyspace "$ENTRIES" -servers "$ADDR" \
    < /dev/null > "$DATA/s.out" 2>&1 &
SPID=$!

for _ in $(seq 1 "$DURATION"); do
  sleep 1
  kill -0 "$PID" 2>/dev/null || { tail -40 "$DATA/n.log"; die "节点在并发窗口中崩溃——这正是本用例要抓的失效"; }
done

kill "$WPID" 2>/dev/null; kill "$SPID" 2>/dev/null
sleep 3
R1=$(rounds)
# 扫了多少次：每个已完成的 test 打一行 `Test N: elapse:...`，一个 test 是 SDNUMS 次扫描。
# grep -c 在没有命中时**打 0 并且返回 1**，所以不能写 `|| echo 0`（那会输出两行）。
TESTLINES=$(grep -c '^Test ' "$DATA/s.out" 2>/dev/null); TESTLINES=${TESTLINES:-0}
SCANS=$(( TESTLINES * SDNUMS ))

echo
fail=0

# ---- 判据 a：前提。GC 必须真的跑了很多轮，否则本用例什么也没测到 ----
# 放在最前面，且它不成立时后面的"通过"一律没有意义——CLAUDE.md 里那条
# "用例的规模参数可能是它自己前提的一部分"就是为此写的。
DR=$(( R1 - R0 ))
if [ "$DR" -ge "$MIN_ROUNDS" ]; then
  ok "a: 并发窗口内跑了 ${DR} 轮 GC（下限 ${MIN_ROUNDS}）"
else
  die "a: 并发窗口内只跑了 ${DR} 轮 GC（下限 ${MIN_ROUNDS}）——换库一次都没怎么发生，本用例没有测到要测的东西"
fi

# ---- 判据 b：前提。扫描必须真的在跑 ----
if [ "$SCANS" -ge "$MIN_SCANS" ]; then
  ok "b: 窗口内完成了 ${SCANS} 次扫描（下限 ${MIN_SCANS}）"
else
  die "b: 窗口内只完成了 ${SCANS} 次扫描（下限 ${MIN_SCANS}）——扫描与换库没有重叠，前提不成立"
fi

# ---- 判据 c：节点还活着，且日志里没有崩溃/竞态/GC 出错 ----
kill -0 "$PID" 2>/dev/null || { tail -40 "$DATA/n.log"; die "c: 节点已经不在了"; }
BAD=$(grep -cE 'panic|DATA RACE|fatal|SIGSEGV|signal arrived during|垃圾回收出现了错误|error in scan' "$DATA/n.log" 2>/dev/null); BAD=${BAD:-0}
if [ "$BAD" = 0 ]; then
  ok "c: 节点存活，日志里没有 panic / DATA RACE / 段错误 / GC 出错 / 扫描出错"
else
  grep -E 'panic|DATA RACE|fatal|SIGSEGV|signal arrived during|垃圾回收出现了错误|error in scan' "$DATA/n.log" | head -5
  die "c: 上列 ${BAD} 行——迭代器很可能用在了已关闭的库上"
fi

# ---- 判据 d：并发窗口结束后，数据仍然逐条对得上 ----
# use-after-close 也可能不崩、只是读出垃圾，所以光看"没崩"不够。
/tmp/sdg-scanverify -servers "$ADDR" -leader 0 -dnums "$ENTRIES" -vsize "$VSIZE" \
    -span 50 -sample 40 > "$DATA/final.out" 2>&1
if grep -q VERIFY_OK "$DATA/final.out"; then
  ok "d: 并发之后逐条校验通过"
else
  tail -8 "$DATA/final.out"
  die "d: 并发之后逐条校验没通过"
fi

echo
[ "$fail" = 0 ] && ok "全部通过（${DR} 轮 GC 与 ${SCANS} 次扫描并发，数据一致）"
