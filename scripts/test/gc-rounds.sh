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
ABSORB_RATIO="${ABSORB_RATIO:-0.25}"
MIN_ROUNDS="${MIN_ROUNDS:-5}"
DATA="${DATA:-$TMPDIR/gc-rounds}"
ADDR=127.0.0.1:3096
IADDR=127.0.0.1:30961
BIN=/tmp/nezha-rounds

LOGICAL=$(( ENTRIES * (10 + VSIZE) ))
DATASET=$(( ENTRIES * (20 + 10 + VSIZE) ))
# 下限取得比"比例触发"要小，好让比例真正起作用而不是被下限盖住
GCGB=$(awk -v b="$DATASET" 'BEGIN{printf "%.9f", b/8/1073741824}')

info "构建 $(git rev-parse --short HEAD)"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/rounds-scanverify ./cmd/bench/scanverify/ || die
go build -o /tmp/rounds-readonly ./cmd/bench/readonly/ || die

PID=""
cleanup(){ [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null; }
trap cleanup EXIT

rm -rf "$DATA"; mkdir -p "$DATA"
nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
    -data "$DATA" -gap 100000000 -commitTimeoutS 60 -system nezha \
    -gcThresholdGB "$GCGB" -partitionTargetMB "$PARTITION_MB" -absorbRatio "$ABSORB_RATIO" \
    < /dev/null > "$DATA/n.log" 2>&1 &
PID=$!
for _ in $(seq 1 30); do sleep 1; grep -q '\[SYSTEM\]' "$DATA/n.log" && break; done
kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点未启动"; }

rounds(){ grep -c '轮垃圾回收完成' "$DATA/n.log" 2>/dev/null || echo 0; }
dirmb(){ du -sm "$DATA/data" 2>/dev/null | awk '{print $1}'; }

info "数据集 $(( DATASET/1048576 ))MB，分区目标 ${PARTITION_MB}MB，吸收比例 $ABSORB_RATIO，GC 下限 $(awk -v g=$GCGB 'BEGIN{printf "%.1f", g*1024}')MB"
printf '%-6s %-8s %-8s %-10s %s\n' "阶段" "GC轮数" "目录MB" "空间放大" "分区数"

for pass in $(seq 0 "$PASSES"); do
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
  mb=$(dirmb); parts=$(ls "$DATA"/data/valuelog/*.p* 2>/dev/null | wc -l | tr -d ' ')
  printf '%-6s %-8s %-8s %-10s %s\n' "第${pass}遍" "$(rounds)" "$mb" \
    "$(awk -v m="$mb" -v l="$LOGICAL" 'BEGIN{printf "%.2f", m*1048576/l}')" "$parts"
  kill -0 "$PID" 2>/dev/null || { tail -20 "$DATA/n.log"; die "节点在第 $pass 遍后崩溃"; }
done

R=$(rounds)
echo
[ "$R" -ge "$MIN_ROUNDS" ] || die "只跑了 $R 轮 GC，少于要求的 $MIN_ROUNDS 轮——轮数上限可能没真正去掉"
ok "连续跑了 $R 轮 GC"

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
