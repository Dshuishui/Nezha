#!/bin/bash
# 论文主表：单节点，四个系统 × PUT/GET/SCAN × 三档 value，每格若干轮，输出一行一次跑的 CSV。
#
# 四个系统只差存储侧的开关，Raft 与客户端路径完全相同：
#   baseline    -system original              无 KV 分离、无 GC
#   nezha-nogc  -system nezha-nogc            KV 分离，不回收 valuelog
#   nezha       -system nezha                 KV 分离 + GC
#   nezha-avp   -system nezha -inlinePlacement  再加小值内联
# GC 只对后两个有意义（baseline 没有 valuelog，nezha-nogc 按定义不回收），
# 所以 gc_done 这一列在前两个系统上恒为 0——那是设计，不是失败。
#
# 规模按"总字节数统一"取：每档 value 都写 TOTAL_MB，条数随 value 反比变化，
# 这样跨档比较的是同样多的数据，GC 与空间放大才可比。
#
# 用法: bash scripts/bench/maintable.sh [标签]
# 环境变量:
#   TOTAL_MB=100      每格写入的总字节数（MiB）
#   ROUNDS=3          每格轮数，取中位数
#   VSIZES="64 256 1024"
#   SYSTEMS="baseline nezha-nogc nezha nezha-avp"
#   SYNC_WAL=0        1 则加 -syncWAL（每条一次 fsync）
#   PUT_CLIENTS=50 GET_CLIENTS=20 GET_OPS=20000 SCAN_TESTS=20 SCAN_GAP=1000
#   OUT=/tmp/maintable-<标签>.csv
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cd "$(dirname "$0")/../.." || die "无项目目录"
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

LABEL="${1:-$(date +%m%d-%H%M)}"
TOTAL_MB="${TOTAL_MB:-100}"
ROUNDS="${ROUNDS:-3}"
VSIZES="${VSIZES:-64 256 1024}"
SYSTEMS="${SYSTEMS:-baseline nezha-nogc nezha nezha-avp}"
SYNC_WAL="${SYNC_WAL:-0}"
PUT_CLIENTS="${PUT_CLIENTS:-50}"
GET_CLIENTS="${GET_CLIENTS:-20}"
GET_OPS="${GET_OPS:-20000}"
SCAN_TESTS="${SCAN_TESTS:-20}"
SCAN_GAP="${SCAN_GAP:-1000}"
OUT="${OUT:-/tmp/maintable-$LABEL.csv}"
ADDR=127.0.0.1:3088
IADDR=127.0.0.1:30881
BIN=/tmp/nezha-maintable
COMMIT=$(git rev-parse --short HEAD)

# 每条 valuelog 记录约 20B 头 + 10B key + value（与 GC 阈值的算法保持一致）。
entries_for(){ awk -v mb="$TOTAL_MB" -v v="$1" 'BEGIN{printf "%d", mb*1048576/(20+10+v)}'; }
gc_threshold_gb(){ awk -v mb="$TOTAL_MB" 'BEGIN{printf "%.6f", mb/1024/3}'; }   # 总量的 1/3，确保触发

info "构建 $COMMIT"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
for t in randwrite_goroutine zipf_read scan_pro; do
  go build -o "/tmp/mt-$t" "./cmd/bench/$t/" || die "$t 编译失败"
done

# 表头：一行 = 一次跑。分位数单位毫秒，放大率无量纲。
if [ ! -f "$OUT" ]; then
  echo "commit,label,system,syncwal,vsize,entries,total_mb,round,op,n,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms,ops,bytes,elapsed_s,ops_per_s,mb_per_s,extra,gc_done,write_amp,space_amp,peak_rss_mb" > "$OUT"
fi

# field <文本> <键>：从 "键=值" 里取值，取不到给 NA（空字段在汇总表里看起来只是
# "这列没测"，极易被当成正常结果读过去，所以宁可写 NA）。
field(){ local v; v=$(grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2); echo "${v:-NA}"; }

PID=""; SAMPLER=""; DATA=""
cleanup(){ [ -n "$SAMPLER" ] && kill "$SAMPLER" 2>/dev/null; [ -n "$PID" ] && kill "$PID" 2>/dev/null; sleep 1; [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null; }
trap cleanup EXIT

# start_node <system> <gcGB> <数据目录>
start_node(){
  local sys="$1" gcgb="$2" d="$3" flags=""
  case "$sys" in
    baseline)   flags="-system original" ;;
    nezha-nogc) flags="-system nezha-nogc" ;;
    nezha)      flags="-system nezha" ;;
    nezha-avp)  flags="-system nezha -inlinePlacement" ;;
    *) die "未知系统 $sys" ;;
  esac
  [ "$SYNC_WAL" = 1 ] && flags="$flags -syncWAL"
  rm -rf "$d"; mkdir -p "$d"
  # shellcheck disable=SC2086
  nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
      -data "$d" -gap 100000000 -gcThresholdGB "$gcgb" -commitTimeoutS 60 $flags \
      < /dev/null > "$d/n.log" 2>&1 &
  PID=$!
  sleep 8
  kill -0 "$PID" 2>/dev/null || { tail -20 "$d/n.log"; die "节点未启动 ($sys)"; }
}

# 放大率。写放大 = 进程实际落盘字节 / 用户逻辑字节；空间放大 = 数据目录占用 / 用户逻辑字节。
# 两者都是 KV 分离论文的常规指标：KV 分离拿空间换写放大，GC 又把空间吃回去。
write_bytes(){ awk '/^write_bytes:/{print $2}' "/proc/$1/io" 2>/dev/null || echo 0; }
dir_bytes(){ du -sb "$1" 2>/dev/null | awk '{print $1}'; }

total=0; done_n=0
for s in $SYSTEMS; do for v in $VSIZES; do for r in $(seq 1 "$ROUNDS"); do total=$((total+1)); done; done; done
info "共 $total 格：systems=[$SYSTEMS] vsizes=[$VSIZES] rounds=$ROUNDS total=${TOTAL_MB}MB syncWAL=$SYNC_WAL"
info "输出 $OUT"

for sys in $SYSTEMS; do
 for vs in $VSIZES; do
  N=$(entries_for "$vs"); GCGB=$(gc_threshold_gb)
  LOGICAL=$(awk -v n="$N" -v v="$vs" 'BEGIN{printf "%d", n*(10+v)}')   # 用户看到的 key+value 字节
  for round in $(seq 1 "$ROUNDS"); do
    done_n=$((done_n+1))
    DATA="$TMPDIR/mt-$LABEL-$sys-$vs-$round"
    info "[$done_n/$total] $sys value=${vs}B entries=$N round=$round"
    start_node "$sys" "$GCGB" "$DATA"
    W0=$(write_bytes "$PID")
    RSSF="$DATA/rss.txt"; ( while kill -0 "$PID" 2>/dev/null; do awk '/^VmRSS:/{print $2}' "/proc/$PID/status" 2>/dev/null; sleep 3; done > "$RSSF" ) & SAMPLER=$!

    # ---- PUT ----
    /tmp/mt-randwrite_goroutine -cnums "$PUT_CLIENTS" -dnums "$N" -vsize "$vs" -servers "$ADDR" > "$DATA/put.out" 2>&1
    PUTL=$(grep '^\[LATENCY\]' "$DATA/put.out" | tail -1)
    PUTT=$(grep '^\[THROUGHPUT\]' "$DATA/put.out" | tail -1)
    [ -n "$PUTL" ] || { tail -20 "$DATA/put.out"; die "PUT 无分位数输出 ($sys/$vs/$round)"; }

    # ---- GC ----
    # 只有 nezha / nezha-avp 会回收；另外两个恒为 0，是设计不是失败。
    GC=0
    case "$sys" in nezha|nezha-avp)
      for _ in $(seq 1 30); do
        GC=$(grep -c '轮垃圾回收完成' "$DATA/n.log") || GC=0
        [ "$GC" -ge 1 ] && break; sleep 5
      done
      [ "$GC" -ge 1 ] || { ls -l "$DATA"/data/valuelog/ 2>/dev/null | tail -3; die "GC 未触发（阈值 ${GCGB}GB）——读路径不走 sortedFile，这一格测的不是要测的东西"; }
      ;;
    esac
    kill -0 "$PID" 2>/dev/null || { tail -30 "$DATA/n.log"; die "节点在 GC 中崩溃 ($sys/$vs/$round)"; }

    # 放大率在写入与 GC 都结束后采一次
    W1=$(write_bytes "$PID"); DIRB=$(dir_bytes "$DATA")
    WAMP=$(awk -v a="$W0" -v b="$W1" -v l="$LOGICAL" 'BEGIN{ if(l>0) printf "%.4f", (b-a)/l; else print "NA" }')
    SAMP=$(awk -v d="$DIRB" -v l="$LOGICAL" 'BEGIN{ if(l>0 && d!="") printf "%.4f", d/l; else print "NA" }')

    # ---- GET ----
    # keyspace 必须等于实际写入量，否则读的大多是从未写入的 key。
    /tmp/mt-zipf_read -cnums "$GET_CLIENTS" -dnums "$GET_OPS" -keyspace "$N" -servers "$ADDR" > "$DATA/get.out" 2>&1
    GETL=$(grep '^\[LATENCY\]' "$DATA/get.out" | tail -1)
    GETT=$(grep '^\[THROUGHPUT\]' "$DATA/get.out" | tail -1)
    HIT=$(grep '^\[HITRATE\]' "$DATA/get.out" | tail -1)
    [ -n "$GETL" ] || { tail -20 "$DATA/get.out"; die "GET 无分位数输出 ($sys/$vs/$round)"; }

    # ---- SCAN ----
    /tmp/mt-scan_pro -cnums 1 -dnums 4 -tests "$SCAN_TESTS" -gapkey "$SCAN_GAP" -keyspace "$N" -servers "$ADDR" > "$DATA/scan.out" 2>&1
    SCANL=$(grep '^\[LATENCY\]' "$DATA/scan.out" | tail -1)
    SCANT=$(grep '^\[THROUGHPUT\]' "$DATA/scan.out" | tail -1)
    YIELD=$(grep '^\[SCANYIELD\]' "$DATA/scan.out" | tail -1)
    [ -n "$SCANL" ] || { tail -20 "$DATA/scan.out"; die "SCAN 无分位数输出 ($sys/$vs/$round)"; }

    kill "$SAMPLER" 2>/dev/null; SAMPLER=""
    RSS=$(awk 'BEGIN{m=0} {if($1>m)m=$1} END{if(NR==0) print "NA"; else printf "%.1f", m/1024}' "$RSSF")

    emit(){ # emit <op> <latency行> <throughput行> <extra>
      local op="$1" L="$2" T="$3" X="$4"
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$COMMIT" "$LABEL" "$sys" "$SYNC_WAL" "$vs" "$N" "$TOTAL_MB" "$round" "$op" \
        "$(field "$L" n)" "$(field "$L" mean)" "$(field "$L" p50)" "$(field "$L" p90)" \
        "$(field "$L" p95)" "$(field "$L" p99)" "$(field "$L" p999)" "$(field "$L" min)" "$(field "$L" max)" \
        "$(field "$T" ops)" "$(field "$T" bytes)" "$(field "$T" elapsed)" "$(field "$T" ops_per_s)" "$(field "$T" mb_per_s)" \
        "$X" "$GC" "$WAMP" "$SAMP" "$RSS" >> "$OUT"
    }
    emit PUT  "$PUTL"  "$PUTT"  "NA"
    emit GET  "$GETL"  "$GETT"  "$(field "$HIT" ratio)"
    emit SCAN "$SCANL" "$SCANT" "$(field "$YIELD" pairs_per_query)"

    cleanup; PID=""
    # 数据目录留到下一格开始前才删：这一格失败时还能现场查看。
  done
 done
done

info "完成，$OUT"
awk -F, 'NR>1{printf "%-11s %-5s %-5s v=%-5s p50=%-9s p99=%-9s ops/s=%-10s wamp=%-7s samp=%s\n", $3,$9,$8,$5,$12,$15,$22,$26,$27}' "$OUT" | tail -40
