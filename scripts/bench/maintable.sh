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
#   PARTITION_MB=     GC 产物中单个分区的目标大小（MB）。留空则不传这个 flag——
#                     "改进前"的基线二进制不认识它，传了会直接退出。
#                     100MiB 数据配 128MB 默认值只会切出一个分区，路由代码根本不被执行，
#                     跑出来的"无退化"是假的；验收要显式调小（如 16）。
#   PUT_CLIENTS=100 GET_CLIENTS=100   并发度（用户 2026-09-09 定：PUT/GET 100，SCAN 单线程）
#   GET_OPS=200000 GET_TESTS=10       GET 总请求 = 两者之积（默认 200 万）
#   SCAN_DNUMS=250 SCAN_TESTS=4       总扫描次数 = 两者之积（默认 1000）
#   REST_SEC=5                        bench 客户端的轮间静置秒数
#
#   **轮数要少、每轮要大。** 分位数是把所有单次延迟汇总后算的，与轮数无关；轮数只影响
#   "每轮吞吐"这个平均值的样本数。而每两轮之间要静置 REST_SEC 秒，所以轮数直接乘进空等：
#   旧默认（GET 100 轮 / SCAN 20 轮）每格空等 590 秒，而真正在测的只有约 57 秒——
#   一格 11 分钟里 88% 是 time.Sleep。改成 10 轮 / 4 轮后操作总数与分位数样本完全不变，
#   空等降到 60 秒，每格约 2.5 分钟。
#   SCAN_SPAN_PARTS=2                 单次扫描横跨几个分区
#   SCAN_PART_MB=                     算 gapkey 用的分区大小；默认取 PARTITION_MB。
#                                     两者分开是必须的：对照组跑的是不认识
#                                     -partitionTargetMB 的旧二进制（PARTITION_MB 必须为空），
#                                     但它的扫描规模必须与实验组**逐字相同**，否则比的就不是同一件事
#   SCAN_FRAC=                        或按"覆盖 记录总数/SCAN_FRAC 条"定
#   SCAN_GAP=                         或直接指定 gapkey
#   三者优先级 SCAN_GAP > SCAN_SPAN_PARTS > SCAN_FRAC
#   GC_STABLE_CHECKS=4  读之前要求 GC 轮数连续几次检查不变（见 GC 一节的注释）
#   LOST_KEYS=warn      每格读之前数一遍盘上丢了多少 key：warn 记录并继续 / fail 立即中止 / off 不查。
#                       GC 搬丢记录不会报任何错，只会在某次 GET 上变成一个 NOKEY，命中率看不出来。
#                       改动了 GC 或恢复路径时应当用 fail——**跑几小时之后才发现搬丢了才是真浪费**。
#   OUT=/tmp/maintable-<标签>.csv
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

# REPO_DIR 让脚本对着另一棵工作树跑（默认是脚本自己所在的仓库）。
# 用途是"改进前基线必须用改进后的脚本重跑"：检出旧 commit 会把 scripts/ 一并退回旧版，
# 于是"改进前 vs 改进后"的差异里混进了**脚本行为的变化**。把脚本从工作树外的副本运行、
# 用 REPO_DIR 指向旧代码，两边口径才真的一致。
# 脚本自身所在目录要在 cd 之前定下来：cd 之后 $0 若是相对路径就指不回来了，
# 而 REPO_DIR 指向别的工作树时更是完全对不上。
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "${REPO_DIR:-$SCRIPT_DIR/../..}" || die "无项目目录"
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

LABEL="${1:-$(date +%m%d-%H%M)}"
TOTAL_MB="${TOTAL_MB:-100}"
ROUNDS="${ROUNDS:-3}"
VSIZES="${VSIZES:-64 256 1024}"
SYSTEMS="${SYSTEMS:-baseline nezha-nogc nezha nezha-avp}"
SYNC_WAL="${SYNC_WAL:-0}"
PARTITION_MB="${PARTITION_MB:-}"
PUT_CLIENTS="${PUT_CLIENTS:-100}"
GET_CLIENTS="${GET_CLIENTS:-100}"
GET_OPS="${GET_OPS:-200000}"
GET_TESTS="${GET_TESTS:-10}"
REST_SEC="${REST_SEC:-5}"
# 总扫描次数 = SCAN_DNUMS × SCAN_TESTS，默认 1000。
# 下限由分位数的样本量定：最近秩下 N 个样本的 p99 就是第 N 个，样本太少时 p99 直接等于最大值。
# 冒烟曾用 80 个查询，p99 与 p999 双双落在 max 上，那个分位数没有任何意义。
# 1000 次让 p99 有 10 个样本垫底，是能报出去的最低线（用户 2026-09-09 定）。
# 之所以不取更高：gapkey 改为数据量的 1/4 之后单次扫描涨到约 25MB，5000 次会让
# 整套实验的 SCAN 部分从约 3 小时涨到约 14 小时，而 p99 的轮间噪声本就有 5~13%，
# 主判据是 p50，为 p99 多花 11 小时不划算。
SCAN_DNUMS="${SCAN_DNUMS:-250}"
SCAN_TESTS="${SCAN_TESTS:-4}"
# gapkey 不写死绝对条数：同样的 gapkey 在 64B 档只覆盖数据集的百分之几、在 1024B 档
# 却覆盖一大片，三档测的根本不是同一件事。（这个值曾被写死成 400 万，等于每轮扫全库——
# 是同一个错误的另一个方向。）两种派生方式：
#
#   SCAN_SPAN_PARTS  按"横跨几个分区"定。参数名直接就是实验意图——要测的正是分区化
#                    让一次范围查询从读一个连续文件变成横跨若干文件，代价有多大。
#                    注意语义是**区间宽度等于 N 个分区**：起点随机，所以实际触及
#                    N 或 N+1 个分区（只有恰好对齐边界时才是 N）。取 2 即至少跨 2 个、
#                    多数情况 3 个。
#   SCAN_FRAC        按"覆盖数据量的几分之一"定。分区大小未定或不关心分区时用它。
#
# **扫描分区大小时必须改用 SCAN_FRAC 或 SCAN_GAP**：若 gapkey 跟着 PARTITION_MB 变，
# 那么"16MB 分区 vs 32MB 分区"的对比里扫描规模也一起变了，测出来的差异无法归因。
SCAN_SPAN_PARTS="${SCAN_SPAN_PARTS:-2}"
SCAN_PART_MB="${SCAN_PART_MB:-$PARTITION_MB}"
SCAN_FRAC="${SCAN_FRAC:-}"
SCAN_GAP="${SCAN_GAP:-}"
# 连续这么多次（每次间隔 5s）检查 GC 轮数不变，才认为布局已稳定、可以开始读
GC_STABLE_CHECKS="${GC_STABLE_CHECKS:-4}"
LOST_KEYS="${LOST_KEYS:-warn}"
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
  echo "commit,label,system,syncwal,vsize,entries,total_mb,round,op,n,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms,ops,bytes,elapsed_s,ops_per_s,mb_per_s,extra,gc_done,lost_keys,space_amp,peak_rss_mb" > "$OUT"
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
  # 只有 nezha/nezha-avp 有 GC 产物；另外两个系统传了也无害，但基线二进制不认识这个 flag
  [ -n "$PARTITION_MB" ] && flags="$flags -partitionTargetMB $PARTITION_MB"
  rm -rf "$d"; mkdir -p "$d"
  # shellcheck disable=SC2086
  nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
      -data "$d" -gap 100000000 -gcThresholdGB "$gcgb" -commitTimeoutS 60 $flags \
      < /dev/null > "$d/n.log" 2>&1 &
  PID=$!
  sleep 8
  kill -0 "$PID" 2>/dev/null || { tail -20 "$d/n.log"; die "节点未启动 ($sys)"; }
}

# 空间放大 = 数据目录占用 / 用户逻辑字节。
#
# **这里不再算写放大。** 曾经用 /proc/PID/io 的 write_bytes 作分子，但那个计数器只统计
# 由进程**自己的上下文**提交到块层的字节：缓冲写由内核回写线程刷盘，不记在写入进程头上，
# 只有进程自己 fsync（例如 RocksDB 的后台 compaction 线程）才计入。于是关掉 fsync 时它恒为 0，
# 而旧版脚本因为每格空等约 10 分钟、compaction 趁机跑了几轮，才凑出非零值——那一列测的是
# "这段时间里 compaction 恰好跑了多少"，空等时长一变数字就变，不是写放大。
# 正式的写放大走 scripts/bench/amplification.sh：设备级计数器 + 阶段边界 sync + 等静默。
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
    RSSF="$DATA/rss.txt"
    ( while kill -0 "$PID" 2>/dev/null; do awk '/^VmRSS:/{print $2}' "/proc/$PID/status" 2>/dev/null; sleep 3; done > "$RSSF" ) & SAMPLER=$!
    disown "$SAMPLER" 2>/dev/null || true   # 否则 kill 之后 shell 会往 stderr 打一行 Terminated

    # ---- PUT ----
    /tmp/mt-randwrite_goroutine -cnums "$PUT_CLIENTS" -dnums "$N" -vsize "$vs" -servers "$ADDR" > "$DATA/put.out" 2>&1
    PUTL=$(grep '^\[LATENCY\]' "$DATA/put.out" | tail -1)
    PUTT=$(grep '^\[THROUGHPUT\]' "$DATA/put.out" | tail -1)
    [ -n "$PUTL" ] || { tail -20 "$DATA/put.out"; die "PUT 无分位数输出 ($sys/$vs/$round)"; }

    # ---- GC ----
    # 只有 nezha / nezha-avp 会回收；另外两个恒为 0，是设计不是失败。
    #
    # 必须等到轮数**稳定**，不能只等"至少一轮"。GC 每 5 秒检查一次阈值，第一轮搬完后
    # 新 valuelog 会继续增长、可能再触发一轮；写得越慢，GC 越有时间多跑。实测
    # no-fsync 组多数格子停在 1 轮，而 fsync 组（写入慢 50 倍、PUT 阶段几十分钟）
    # 全部跑满 2 轮——于是两组的读性能是在**不同的存储布局**上测的，
    # "fsync 的影响"里混进了"GC 轮数的影响"，SCAN 那几列因此不可比。
    # 等到连续 GC_STABLE_CHECKS 次检查轮数不变，读取时的布局才是确定的。
    GC=0
    case "$sys" in nezha|nezha-avp)
      prev=-1; stable=0
      for _ in $(seq 1 60); do
        GC=$(grep -c '轮垃圾回收完成' "$DATA/n.log") || GC=0
        if [ "$GC" = "$prev" ] && [ "$GC" -ge 1 ]; then
          stable=$((stable+1))
          [ "$stable" -ge "$GC_STABLE_CHECKS" ] && break
        else
          stable=0; prev=$GC
        fi
        sleep 5
      done
      [ "$GC" -ge 1 ] || { ls -l "$DATA"/data/valuelog/ 2>/dev/null | tail -3; die "GC 未触发（阈值 ${GCGB}GB）——读路径不走 sortedFile，这一格测的不是要测的东西"; }
      [ "$stable" -ge "$GC_STABLE_CHECKS" ] || warn "GC 轮数在 300s 内未稳定（当前 $GC 轮），本格的读数据与其它格可能不可比"
      ;;
    esac
    kill -0 "$PID" 2>/dev/null || { tail -30 "$DATA/n.log"; die "节点在 GC 中崩溃 ($sys/$vs/$round)"; }

    # ---- 搬丢了没有 ----
    # 放在读之前：丢了 key 会让后面的 GET/SCAN 数字失去意义，早一格发现就少浪费几小时。
    LOST=NA
    if [ "$LOST_KEYS" != off ]; then
      # nezha-avp 的小值被内联进 RocksDB、不在 valuelog 里，本工具数不准，会误报几十条。
      # 传入内联阈值让它自己判定不适用；那种配置的正确性由 readonly/scanverify 逐条校验保证。
      INLINE_TH=0
      [ "$sys" = nezha-avp ] && INLINE_TH="${INLINE_THRESHOLD:-512}"
      LOST=$(python3 "$SCRIPT_DIR/lost-keys.py" "$DATA" "$N" "$vs" "$INLINE_TH" 2>/dev/null | grep -o '丢失 [0-9]*' | grep -o '[0-9]*')
      LOST="${LOST:-NA}"
      if [ "$LOST" != NA ] && [ "$LOST" -gt 0 ]; then
        if [ "$LOST_KEYS" = fail ]; then
          die "GC 搬丢了 $LOST 条记录（$sys/$vs/$round）——先修再测"
        fi
        warn "GC 搬丢了 $LOST 条记录（$sys/$vs/$round）"
      fi
    fi

    # 空间放大：数据目录占用 / 用户逻辑字节。这是一次 du，含义明确。
    DIRB=$(dir_bytes "$DATA")
    SAMP=$(awk -v d="$DIRB" -v l="$LOGICAL" 'BEGIN{ if(l>0 && d!="") printf "%.4f", d/l; else print "NA" }')

    # ---- GET ----
    # keyspace 必须等于实际写入量，否则读的大多是从未写入的 key。
    /tmp/mt-zipf_read -cnums "$GET_CLIENTS" -dnums "$GET_OPS" -tests "$GET_TESTS" -rest "$REST_SEC" \
        -keyspace "$N" -servers "$ADDR" > "$DATA/get.out" 2>&1
    GETL=$(grep '^\[LATENCY\]' "$DATA/get.out" | tail -1)
    GETT=$(grep '^\[THROUGHPUT\]' "$DATA/get.out" | tail -1)
    HIT=$(grep '^\[HITRATE\]' "$DATA/get.out" | tail -1)
    [ -n "$GETL" ] || { tail -20 "$DATA/get.out"; die "GET 无分位数输出 ($sys/$vs/$round)"; }

    # ---- SCAN ----
    # SCAN 单线程是刻意的：一次范围查询本身读取量就大，再叠并发只会让各 goroutine 的
    # 随机起点互相冲刷缓存，结果不稳定且难以归因。
    GAP="$SCAN_GAP"
    if [ -z "$GAP" ] && [ -n "$SCAN_SPAN_PARTS" ] && [ -n "$SCAN_PART_MB" ]; then
      # 一个分区装 分区字节数/每条字节数 条；每条 = 20B 头 + 10B key + value
      GAP=$(( SCAN_SPAN_PARTS * (SCAN_PART_MB * 1048576) / (20 + 10 + vs) ))
    fi
    if [ -z "$GAP" ] && [ -n "$SCAN_FRAC" ]; then GAP=$(( N / SCAN_FRAC )); fi
    [ -n "$GAP" ] || die "gapkey 无法确定：SCAN_GAP/SCAN_SPAN_PARTS(+PARTITION_MB)/SCAN_FRAC 都没给"
    # 起点上界是 keyspace-gapkey，gapkey 超过记录总数就没有合法起点了
    [ "$GAP" -ge 1 ] && [ "$GAP" -lt "$N" ] || die "gapkey=$GAP 不在 [1,$N) 内（vsize=$vs）"
    /tmp/mt-scan_pro -cnums 1 -dnums "$SCAN_DNUMS" -tests "$SCAN_TESTS" -rest "$REST_SEC" \
        -gapkey "$GAP" -keyspace "$N" -servers "$ADDR" > "$DATA/scan.out" 2>&1
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
        "$X" "$GC" "$LOST" "$SAMP" "$RSS" >> "$OUT"
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
awk -F, 'NR>1{printf "%-11s %-5s %-5s v=%-5s p50=%-9s p99=%-9s ops/s=%-10s lost=%-5s samp=%s\n", $3,$9,$8,$5,$12,$15,$22,$26,$27}' "$OUT" | tail -40
