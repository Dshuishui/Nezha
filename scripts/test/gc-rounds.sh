#!/bin/bash
# 多轮 GC 验证：改造之后 GC 还能不能一直跑下去，以及空间会不会有界。
#
# 改造前 `numGC >= 2` 让 GC 永久停止：两轮之后 valuelog 无限增长，空间放大无界。
# 实测里这一点被掩盖得很好——主表每格只跑 1~2 轮，正好碰不到上限；而放大率那组
# 两轮全烧在零垃圾的装载阶段，等覆盖写真正产生垃圾时已经封死，于是跑了 GC 的 nezha
# 空间放大 4.49，比压根不回收的 nezha-nogc 的 2.92 还差。
#
# 所以这里要的不是"跑通一轮"，而是**连续跑很多轮，且空间不随轮数上涨**。
#
# 负载用 scanverify 反复重写同一批 key：value 从 key 派生，所以每一轮之后都能逐条校验，
# 而且每一遍重写都把上一遍的全部记录变成垃圾——正是要回收的东西。
#
# 用法: bash scripts/test/gc-rounds.sh
# 环境变量: ENTRIES=50000 VSIZE=256 PASSES=3 PARTITION_MB=2 ABSORB_RATIO=0.25 MIN_ROUNDS=5
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

ENTRIES="${ENTRIES:-50000}"
VSIZE="${VSIZE:-256}"
PASSES="${PASSES:-3}"
PARTITION_MB="${PARTITION_MB:-2}"
# 留空则不传 -absorbRatio：改造前的二进制不认识这个 flag，传了会直接退出。
# 对照实验要用同一个脚本驱动两边，否则差异里会混进脚本行为的变化。
ABSORB_RATIO="${ABSORB_RATIO-0.25}"
MIN_ROUNDS="${MIN_ROUNDS-5}"
DATA="${DATA:-$TMPDIR/gc-rounds}"
ADDR=127.0.0.1:3096
IADDR=127.0.0.1:30961
BIN=/tmp/nezha-rounds

LOGICAL=$(( ENTRIES * (10 + VSIZE) ))
DATASET=$(( ENTRIES * (20 + 10 + VSIZE) ))
# 下限取得比"比例触发"要小，好让比例真正起作用而不是被下限盖住。
#
# **但这个意图有个门槛，而它此前没被写出来也没被检查。** 触发条件是
#     need = max(下限, 分区总量 × ABSORB_RATIO)
# 下限在这里是 DATASET/8，而 GC 把数据吸收完之后分区总量 ≈ DATASET，所以比例要压过
# 下限就得 `DATASET × ratio > DATASET/8`，即 **ratio > 0.125——与数据量无关**。
# 比例低于这个门槛时，那一档从头到尾都是下限驱动的，测的不是比例。
# 2026-09-17 我照默认参数扫 0.05 就是这样：跑完、判过、什么也没测到，而且三个点的
# 空间放大全都一样（7.02），因为它们实际上跑的是同一个触发条件。
GC_FLOOR_DIVISOR="${GC_FLOOR_DIVISOR:-8}"
GCGB=$(awk -v b="$DATASET" -v d="$GC_FLOOR_DIVISOR" 'BEGIN{printf "%.9f", b/d/1073741824}')
if [ -n "${ABSORB_RATIO:-}" ]; then
  awk -v r="$ABSORB_RATIO" -v d="$GC_FLOOR_DIVISOR" 'BEGIN{exit !(r*d > 1)}' || {
    warn "ABSORB_RATIO=$ABSORB_RATIO 配 GC_FLOOR_DIVISOR=${GC_FLOOR_DIVISOR}：比例永远压不过下限"
    warn "  （需要 ratio × divisor > 1），这一档全程是下限驱动的，测不到比例的影响。"
    warn "  要扫这么低的比例，把下限调小：GC_FLOOR_DIVISOR=$(awk -v r="$ABSORB_RATIO" 'BEGIN{printf "%d", 2/r}')"
  }
fi

info "构建 $(git rev-parse --short HEAD)"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/rounds-scanverify ./cmd/bench/scanverify/ || die
go build -o /tmp/rounds-readonly ./cmd/bench/readonly/ || die

PID=""
cleanup(){ [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null; }
trap cleanup EXIT

rm -rf "$DATA"; mkdir -p "$DATA"
EXTRA=""
[ -n "$ABSORB_RATIO" ] && EXTRA="-absorbRatio $ABSORB_RATIO"
# shellcheck disable=SC2086
nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
    -data "$DATA" -gap 100000000 -commitTimeoutS 60 -system nezha \
    -gcThresholdGB "$GCGB" -partitionTargetMB "$PARTITION_MB" $EXTRA \
    < /dev/null > "$DATA/n.log" 2>&1 &
PID=$!
for _ in $(seq 1 30); do sleep 1; grep -q '\[SYSTEM\]' "$DATA/n.log" && break; done
kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点未启动"; }

rounds(){ grep -c '轮垃圾回收完成' "$DATA/n.log" 2>/dev/null || echo 0; }
dirmb(){ du -sm "$DATA/data" 2>/dev/null | awk '{print $1}'; }

info "数据集 $(( DATASET/1048576 ))MB，分区目标 ${PARTITION_MB}MB，吸收比例 ${ABSORB_RATIO:-（不传）}，GC 下限 $(awk -v g=$GCGB 'BEGIN{printf "%.1f", g*1024}')MB"
printf '%-6s %-8s %-8s %-10s %-8s %-10s %s\n' "阶段" "GC轮数" "静止MB" "静止放大" "峰值MB" "峰值放大" "分区数"

for pass in $(seq 0 "$PASSES"); do
  # **峰值也要量，不能只量静止态。** 下面那个 dirmb 是在"GC 轮数连续三次不变"之后取的，
  # 也就是 GC 已经把尾部吸收完的静止态。而 ABSORB_RATIO 真正影响的是"多久吸收一次"，
  # 也就是**峰值尾部**：比例越大，尾部允许涨得越高才开一轮。
  # 2026-09-17 扫 0.05/0.25/0.5/0.9 时四档静止放大**完全一样（7.02）**——不是比例没用，
  # 是这个口径量不到它。
  #
  # 采样必须盖到 GC 等待窗口**之后**才停（与 raftlog-memory / memory-curve 同一条教训，
  # 见闸门自审第二十节）：GC 就发生在那段等待里，先停采样等于把要看的那一段排除掉。
  : > "$DATA/dirsz-$pass.txt"
  ( while kill -0 "$PID" 2>/dev/null; do dirmb >> "$DATA/dirsz-$pass.txt"; sleep 2; done ) &
  SAMPLER=$!
  /tmp/rounds-scanverify -servers "$ADDR" -leader 0 -dnums "$ENTRIES" -vsize "$VSIZE" \
      -span 50 -sample 20 > "$DATA/pass$pass.out" 2>&1
  grep -q VERIFY_OK "$DATA/pass$pass.out" || { tail -5 "$DATA/pass$pass.out"; die "第 $pass 遍写入或即时校验失败"; }
  # 等这一遍触发的 GC 走完：轮数连续三次检查不变才算稳定
  prev=-1; stable=0
  for _ in $(seq 1 60); do
    r=$(rounds)
    if [ "$r" = "$prev" ]; then stable=$((stable+1)); [ "$stable" -ge 3 ] && break; else stable=0; prev=$r; fi
    sleep 2
  done
  kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null
  mb=$(dirmb); parts=$(ls "$DATA"/data/valuelog/*.p* 2>/dev/null | wc -l | tr -d ' ')
  # 峰值取采样序列的最大值；采样为空就报 ?，不要用静止值冒充峰值。
  peak=$(awk '$1 ~ /^[0-9]+$/ && $1 > m { m = $1; seen = 1 } END { if (seen) print m; else print "?" }' "$DATA/dirsz-$pass.txt")
  peakamp="?"
  [ "$peak" != "?" ] && peakamp=$(awk -v m="$peak" -v l="$LOGICAL" 'BEGIN{printf "%.2f", m*1048576/l}')
  printf '%-6s %-8s %-8s %-10s %-8s %-10s %s\n' "第${pass}遍" "$(rounds)" "$mb" \
    "$(awk -v m="$mb" -v l="$LOGICAL" 'BEGIN{printf "%.2f", m*1048576/l}')" "$peak" "$peakamp" "$parts"
  kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点在第 $pass 遍后崩溃"; }
done

R=$(rounds)
echo
# 轮数下限这条断言是为"改造前封顶两轮"设的回归守卫，**它对 ABSORB_RATIO 无感知**：
# 比例调大就是要让 GC 少触发，轮数自然变少。2026-09-17 实测 ABSORB_RATIO=0.9 跑出 4 轮，
# 被判成"轮数上限可能没真正去掉"——原因猜错了，而错误的原因比没有原因更费时间。
# 所以改了比例就必须一起给 MIN_ROUNDS：这条断言只有在"比例已知"的前提下才有意义。
if [ -n "${ABSORB_RATIO:-}" ] && [ "$ABSORB_RATIO" != 0.25 ] && [ "${MIN_ROUNDS-unset}" = 5 ]; then
  warn "ABSORB_RATIO=$ABSORB_RATIO 非默认，而 MIN_ROUNDS 仍是默认的 5：轮数下限对比例无感知，"
  warn "  比例调大本就会让轮数变少。本轮只判正确性，不判轮数下限（要判就显式给 MIN_ROUNDS）。"
  MIN_ROUNDS=""
fi
if [ -z "$MIN_ROUNDS" ]; then
  warn "未设轮数下限（对照组：改造前封顶两轮，本来就跑不满；或比例非默认，见上）"
elif [ "$R" -lt "$MIN_ROUNDS" ]; then
  die "只跑了 $R 轮 GC，少于要求的 $MIN_ROUNDS 轮（吸收比例 ${ABSORB_RATIO:-默认}）——"\
      "比例是默认值时这说明轮数上限可能没真正去掉；比例调大过则是预期行为，应显式设 MIN_ROUNDS"
fi
ok "共跑了 $R 轮 GC"

# 每一轮都不该丢记录。GC 搬丢不报错，只在某次 GET 上变成一个 NOKEY，必须直接数盘。
LOST=$(python3 "$SCRIPT_DIR/../bench/lost-keys.py" "$DATA" "$ENTRIES" "$VSIZE" 2>/dev/null | grep -o '丢失 [0-9]*' | grep -o '[0-9]*')
[ "${LOST:-NA}" = 0 ] || die "$R 轮之后盘上丢了 ${LOST:-?} 条记录"
ok "盘上丢失 0"

OUT=$(/tmp/rounds-readonly -servers "$ADDR" -dnums "$ENTRIES" -vsize "$VSIZE" \
    -span 50 -sample 30 -check 300 2>&1 | grep -v 'new pool success')
echo "$OUT" | grep -E '校验|VERIFY' | sed 's/^/       /'
echo "$OUT" | grep -q FAILOVER_VERIFY_OK || die "逐条校验未通过"
ok "逐条校验通过"

grep -h '\[GC-ABSORB\]' "$DATA/n.log" | tail -5 | sed 's/^/       /'
ok "全部通过"
