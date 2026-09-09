#!/bin/bash
# 写放大与空间放大：装载 → 覆盖写制造垃圾 → 触发 GC → 静默后取数。
#
# 与 maintable.sh 的分工：那个测读写延迟，键是 0..N-1 的一个排列，每键只写一次，
# **零垃圾**——于是它报的"写放大"量的是"在没有垃圾的数据上重排一遍的代价"，
# 不是文献里那个"回收垃圾的代价"。同类工作（Scavenger+、HashKV、Titan）的流程
# 都是"装载 → update 制造垃圾 → 触发 GC → 再测"，本脚本照此。
#
# 三处方法学上的讲究：
#  1. 分子以**设备级**计数器为准（/sys/block/<dev>/<part>/stat 第 7 字段 = 写扇区数 ×512），
#     它是真正落到块设备的字节。/proc/PID/io 的 write_bytes 按内核文档是"页被弄脏时"
#     计数，被覆盖后从未落盘的脏页也算，只能作交叉验证。跑时必须独占该磁盘。
#  2. 取数前 sync + **等静默**：判据看被测进程的写入速率降到阈值以下（不是设备计数不变——
#     数据目录在整机根文件系统上，系统守护进程一直在写，那个条件永远不成立）。
#     不等的话 LSM 里还没跑完的 compaction 不被计入，而 baseline 把完整 value 存在 LSM 里、
#     这部分恰恰最重，会被系统性低估。
#  3. 分母分两个口径：write_amp_total 用"装载+覆盖"的全部用户字节；
#     write_amp_gc 只用覆盖阶段的用户字节，量的是"每写入一字节新数据、GC 连带写了多少"。
#     空间放大的分母是**活数据**（覆盖之后仍是 N 个键），不是累计写入量。
#
# 用法: bash scripts/bench/amplification.sh [标签]
# 环境变量:
#   TOTAL_MB=100          装载阶段写满多少 MiB
#   OVERWRITE="25 50 100" 覆盖比例（%），即平均每个键被覆盖 0.25/0.5/1 次
#   VSIZES="64 256 1024"
#   SYSTEMS="baseline nezha-nogc nezha nezha-avp"
#   DIST=zipf|uniform     覆盖写的键分布（默认 zipf，与 GET 的热点口径一致）
#   DEV=sdc/sdc3          设备与分区名，用于读 /sys/block/<DEV>/stat
#   SYNC_WAL=0
#   PARTITION_MB=16       GC 产出分区的目标大小。**必须让数据集分成好几个分区**：
#                         吸收只重写尾部覆盖到的分区，只有一个分区时它退化成全量
#                         重写，P2 相对 P1 的写放大优势整个测不出来。默认 128MB 的
#                         话 100MiB 数据集只有一个分区，所以这里默认调小。
#   ABSORB_RATIO=0.25     按比例触发吸收。**留空则不传该 flag**，因为改造前的二进制
#                         不认识它，传了会直接退出；对照实验要用同一个脚本驱动两边。
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
OVERWRITE="${OVERWRITE:-25 50 100}"
VSIZES="${VSIZES:-64 256 1024}"
SYSTEMS="${SYSTEMS:-baseline nezha-nogc nezha nezha-avp}"
DIST="${DIST:-zipf}"
DEV="${DEV:-sdc/sdc3}"
SYNC_WAL="${SYNC_WAL:-0}"
PUT_CLIENTS="${PUT_CLIENTS:-50}"
PARTITION_MB="${PARTITION_MB:-16}"
ABSORB_RATIO="${ABSORB_RATIO-0.25}"
QUIET_SECS="${QUIET_SECS:-10}"     # 连续这么多秒进程写入低于阈值才算静默
QUIET_BYTES="${QUIET_BYTES:-1048576}"  # 每 2 秒写入低于这个字节数即视为已停止
QUIET_MAX="${QUIET_MAX:-180}"      # 等静默的上限，超时记 NA 而不是硬等
OUT="${OUT:-/tmp/amplification-$LABEL.csv}"
ADDR=127.0.0.1:3088
IADDR=127.0.0.1:30881
BIN=/tmp/nezha-amp
COMMIT=$(git rev-parse --short HEAD)

DEVSTAT="/sys/block/$DEV/stat"
[ -r "$DEVSTAT" ] || die "读不到 $DEVSTAT —— 用 DEV=<disk>/<part> 指定，例如 DEV=sdc/sdc3"

entries_for(){ awk -v mb="$TOTAL_MB" -v v="$1" 'BEGIN{printf "%d", mb*1048576/(20+10+v)}'; }
gc_threshold_gb(){ awk -v mb="$TOTAL_MB" 'BEGIN{printf "%.6f", mb/1024/3}'; }

# 设备写入字节：第 7 字段是写扇区数，扇区固定 512 字节。
dev_written(){ awk '{printf "%.0f", $7*512}' "$DEVSTAT"; }
proc_written(){ awk '/^write_bytes:/{print $2}' "/proc/$1/io" 2>/dev/null || echo 0; }
dir_bytes(){ du -sb "$1" 2>/dev/null | awk '{print $1}'; }

# wait_quiet <被测进程pid>：等本进程不再写。返回 0 表示已静默，1 表示超时。
#
# 判据看**进程级**计数而非设备级：数据目录在整机的根文件系统上，系统日志与守护进程
# 一直在写，"设备计数完全不变"这个条件永远不可能满足——第一版就是这么写的，
# 36 格全部白等 180 秒后标记超时。
#
# 而且判据是"速率低于阈值"而不是"完全不变"：回写是分批的，偶尔几十 KB 的尾巴
# 不该让整个等待失败。
wait_quiet(){
  local pid="$1" last cur delta stable=0 waited=0
  last=$(proc_written "$pid")
  while [ "$waited" -lt "$QUIET_MAX" ]; do
    sleep 2; waited=$((waited+2))
    cur=$(proc_written "$pid")
    delta=$((cur-last)); last=$cur
    if [ "$delta" -lt "$QUIET_BYTES" ]; then
      stable=$((stable+2))
      [ "$stable" -ge "$QUIET_SECS" ] && return 0
    else
      stable=0
    fi
  done
  return 1
}

info "构建 $COMMIT"
go build -o "$BIN" ./cmd/nezha/ || die "节点编译失败"
go build -o /tmp/amp-randwrite ./cmd/bench/randwrite_goroutine/ || die "写入工具编译失败"

if [ ! -f "$OUT" ]; then
  echo "commit,label,system,syncwal,vsize,entries,total_mb,overwrite_pct,dist,gc_done,quiesced,dev_bytes_load,dev_bytes_update,proc_bytes_total,logical_load,logical_update,live_logical,write_amp_total,write_amp_update,space_amp,put_ops_s_load,put_ops_s_update" > "$OUT"
fi

PID=""
cleanup(){ [ -n "$PID" ] && { kill "$PID" 2>/dev/null; sleep 1; kill -9 "$PID" 2>/dev/null; }; }
trap cleanup EXIT

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
  [ -n "$ABSORB_RATIO" ] && flags="$flags -absorbRatio $ABSORB_RATIO"
  rm -rf "$d"; mkdir -p "$d"
  # shellcheck disable=SC2086
  nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
      -data "$d" -gap 100000000 -gcThresholdGB "$gcgb" -commitTimeoutS 60 \
      -partitionTargetMB "$PARTITION_MB" $flags \
      < /dev/null > "$d/n.log" 2>&1 &
  PID=$!
  sleep 8
  kill -0 "$PID" 2>/dev/null || { tail -20 "$d/n.log"; die "节点未启动 ($sys)"; }
}

ops_of(){ grep -o 'ops_per_s=[0-9.]*' "$1" | head -1 | cut -d= -f2; }

total=0; done_n=0
for s in $SYSTEMS; do for v in $VSIZES; do for o in $OVERWRITE; do total=$((total+1)); done; done; done
info "共 $total 格；设备 $DEVSTAT；覆盖比例 [$OVERWRITE]%，分布 $DIST；分区 ${PARTITION_MB}MB，吸收比例 ${ABSORB_RATIO:-（不传）}"
info "输出 $OUT"

for sys in $SYSTEMS; do
 for vs in $VSIZES; do
  N=$(entries_for "$vs"); GCGB=$(gc_threshold_gb)
  LOGICAL_LOAD=$(awk -v n="$N" -v v="$vs" 'BEGIN{printf "%d", n*(10+v)}')
  for pct in $OVERWRITE; do
    done_n=$((done_n+1))
    M=$(awk -v n="$N" -v p="$pct" 'BEGIN{printf "%d", n*p/100}')
    LOGICAL_UPD=$(awk -v m="$M" -v v="$vs" 'BEGIN{printf "%d", m*(10+v)}')
    D="$TMPDIR/amp-$LABEL-$sys-$vs-$pct"
    info "[$done_n/$total] $sys value=${vs}B load=$N overwrite=${pct}% ($M 次)"

    start_node "$sys" "$GCGB" "$D"

    # ---- 装载：唯一键，零垃圾 ----
    D0=$(dev_written)
    P0=$(proc_written "$PID")
    /tmp/amp-randwrite -cnums "$PUT_CLIENTS" -dnums "$N" -vsize "$vs" -servers "$ADDR" > "$D/load.out" 2>&1
    # 先 sync 再等静默：不强制落盘的话，装载阶段弄脏的页会在覆盖阶段才被写下去，
    # 两个阶段的设备字节划分就是错的（实测 load/upd 会整个颠倒而合计不变）。
    sync
    wait_quiet "$PID"; QL=$?
    D1=$(dev_written)

    # ---- 覆盖写：从 [0,N) 按 $DIST 抽键，制造垃圾 ----
    /tmp/amp-randwrite -cnums "$PUT_CLIENTS" -dnums "$M" -vsize "$vs" \
        -keyspace "$N" -dist "$DIST" -servers "$ADDR" > "$D/update.out" 2>&1

    # ---- 等 GC ----
    # 等轮数稳定而非"至少一轮"，理由同 maintable.sh：写得慢的配置 GC 会多跑几轮，
    # 不等的话不同格子的回收量不可比。
    GC=0
    case "$sys" in nezha|nezha-avp)
      prev=-1; stable=0
      for _ in $(seq 1 60); do
        GC=$(grep -c '轮垃圾回收完成' "$D/n.log") || GC=0
        if [ "$GC" = "$prev" ] && [ "$GC" -ge 1 ]; then
          stable=$((stable+1)); [ "$stable" -ge 4 ] && break
        else
          stable=0; prev=$GC
        fi
        sleep 5
      done
      ;;
    esac
    kill -0 "$PID" 2>/dev/null || { tail -30 "$D/n.log"; die "节点崩溃 ($sys/$vs/$pct)"; }

    # 每轮吸收的明细单独留一份。设备级写放大量的是"系统总共写了多少"，答不了
    # "吸收相对全量重写省了多少"——后者要看每轮**复用**与**重写**各自的源字节数：
    # 复用的那些字节正是全量重写会白写一遍的量。有了这两个数就不必再造一个
    # "全量重写"的二进制来对照。
    grep -h '\[GC-ABSORB\]' "$D/n.log" 2>/dev/null | sed "s|^|$sys,$vs,$pct,|" >> "$OUT.absorb"

    # ---- 静默后取终值 ----
    sync
    wait_quiet "$PID"; QU=$?
    D2=$(dev_written); P1=$(proc_written "$PID"); DIRB=$(dir_bytes "$D")
    QUIESCED=$([ "$QL" = 0 ] && [ "$QU" = 0 ] && echo yes || echo "no(超时${QUIET_MAX}s)")

    DEV_LOAD=$((D1-D0)); DEV_UPD=$((D2-D1)); PROC_TOT=$((P1-P0))
    WA_TOT=$(awk -v d="$((D2-D0))" -v l="$((LOGICAL_LOAD+LOGICAL_UPD))" 'BEGIN{ if(l>0) printf "%.4f", d/l; else print "NA" }')
    WA_UPD=$(awk -v d="$DEV_UPD" -v l="$LOGICAL_UPD" 'BEGIN{ if(l>0) printf "%.4f", d/l; else print "NA" }')
    # 空间放大的分母是活数据：覆盖之后活的仍是 N 个键
    SA=$(awk -v d="$DIRB" -v l="$LOGICAL_LOAD" 'BEGIN{ if(l>0 && d!="") printf "%.4f", d/l; else print "NA" }')

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$COMMIT" "$LABEL" "$sys" "$SYNC_WAL" "$vs" "$N" "$TOTAL_MB" "$pct" "$DIST" "$GC" "$QUIESCED" \
      "$DEV_LOAD" "$DEV_UPD" "$PROC_TOT" "$LOGICAL_LOAD" "$LOGICAL_UPD" "$LOGICAL_LOAD" \
      "$WA_TOT" "$WA_UPD" "$SA" "$(ops_of "$D/load.out")" "$(ops_of "$D/update.out")" >> "$OUT"

    cleanup; PID=""
  done
 done
done

info "完成，$OUT"
awk -F, 'NR>1{printf "%-11s v=%-5s ovw=%-4s gc=%-2s quiet=%-14s WA_total=%-8s WA_update=%-9s SA=%s\n", $3,$5,$8,$10,$11,$18,$19,$20}' "$OUT"
