#!/bin/bash
# 三节点规模跑：PUT / GET / SCAN × 若干档 value，一行一次跑的 CSV，全程采样。
#
# 与 scripts/bench/maintable.sh 的关系：那个是**单节点**的论文主表，节点启动那行写死了
# `-peers "$IADDR"`（只有自己），所以它测不到复制、选举、慢 follower、快照补齐。
# 这个脚本把同一套负载放到三节点拓扑上，节点由 three-node.sh 管、拓扑由 gate.sh 给。
# 两个脚本**不可互换**：单节点的数字与三节点的不可比（多了一轮共识往返），
# 主表仍然由 maintable.sh 出，这里出的是"多节点下会不会出问题"。
#
# 用法: bash scripts/multinode/maintable3.sh [标签]
#   只能在 tikv240 上跑（理由见 gate.sh 的 require_driver_host）。
#
# 环境变量:
#   TOTAL_MB=4096     每档 value 写入的总字节数（MiB）。条数随 value 反比派生，
#                     所以跨档比较的是同样多的数据。
#   VSIZES="64 256"
#   SYSTEM=nezha      nezha | nezha-nogc | original | nezha-avp（avp 会加 -inlinePlacement）
#   GCGB=0.3          gcThresholdGB。**必须显式给。** three-node.sh 的派生式是"总量的
#                     1/3"，只保证至少触发一轮；4GB 要跑十几轮 GC 就得给一个小值。
#   SYNC_WAL=0        出性能数字必须 0（每条 fsync 慢约 50 倍，4GB 是几十小时）。
#   PARTITION_MB=     GC 产物中单个分区的目标大小。留空用节点默认（128MB）。
#   PUT_CLIENTS=100 GET_CLIENTS=100        并发度（SCAN 恒为 1，理由见下）
#   GET_OPS=200000 GET_TESTS=10            GET 总请求 = 两者之积
#   SCAN_DNUMS=250 SCAN_TESTS=4            总扫描次数 = 两者之积（默认 1000）
#   SCAN_FRAC=4       单次扫描覆盖 记录总数/SCAN_FRAC 条。4 即每次约 TOTAL_MB/4。
#   SCAN_GAP=         或直接指定 gapkey（优先于 SCAN_FRAC）
#   MIXED_SEC=300     混合阶段时长；0 关掉。见下面"为什么要有混合阶段"。
#   WATCH_IV=60       看门狗轮询间隔秒
#   SAMPLE_IV=15      节点采样间隔秒
#   FLOOR_GB=40       任一节点所在盘的**剩余空间**低于此值就中止。
#                     按剩余字节而不是百分比：node55 常态就在 93%（别人的 1.4T 占着）。
#   LAG_ENTRIES=200000  压缩点跨节点的最大允许落差，超了记一条警告（不直接判失败，
#                     理由见 lag_check）。
#   LAG_STREAK=3      连续这么多次检查都超阈值才报。一轮 GC 的错位会产生一次尖峰而
#                     马上收回，实测两次（见 lag_check），报它只会训练人忽略这个警告。
#   LOST_KEYS=fail    读之前数一遍盘上丢了多少 key。GC 搬丢记录不报任何错。
#   PHASES="A B C"    A=冒烟(TOTAL_MB 缩到 SMOKE_MB) B=正式 C=kill9 重启再校验
#   SMOKE_MB=400     A 阶段的数据量
#   C_MB=$TOTAL_MB C_VSIZES=<VSIZES 的第一档>   C 阶段的规模与档位
#   OUT=              CSV 路径
#
# 为什么要有混合阶段：`scanNewFile` 整段迭代都握着 kvs.mu，而 applyLoop 用的是同一把锁
# （internal/kvstore/read.go 里那段注释记了这件事，是已知未修）。分阶段跑的话 SCAN 期间
# 没有写入，这个问题只表现成"GC 轮数在 SCAN 期间不涨"；要让它变成**客户端能看见的延迟**，
# 必须在扫描的同时有写和点读。单次扫描 1GB 时按住 applyLoop 的时间会很可观，所以这一段
# 是本次跑法里最可能抓到东西的地方。
set -u
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR=${REPO_DIR:-$(cd "$SCRIPT_DIR/../.." && pwd)}
cd "$PROJECT_DIR" || { echo "无项目目录"; exit 1; }
# shellcheck source=scripts/multinode/gate.sh
. "$SCRIPT_DIR/gate.sh"
# shellcheck source=scripts/lib/bench-common.sh
. "$SCRIPT_DIR/../lib/bench-common.sh"
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

require_driver_host || exit 1

LABEL="${1:-$(date +%m%d-%H%M)}"
TOTAL_MB="${TOTAL_MB:-4096}"
VSIZES="${VSIZES:-64 256}"
SYSTEM="${SYSTEM:-nezha}"
GCGB="${GCGB:-0.3}"
SYNC_WAL="${SYNC_WAL:-0}"
PARTITION_MB="${PARTITION_MB:-}"
PUT_CLIENTS="${PUT_CLIENTS:-100}"
GET_CLIENTS="${GET_CLIENTS:-100}"
GET_OPS="${GET_OPS:-200000}"
GET_TESTS="${GET_TESTS:-10}"
SCAN_DNUMS="${SCAN_DNUMS:-250}"
SCAN_TESTS="${SCAN_TESTS:-4}"
SCAN_FRAC="${SCAN_FRAC:-4}"
SCAN_GAP="${SCAN_GAP:-}"
REST_SEC="${REST_SEC:-5}"
MIXED_SEC="${MIXED_SEC:-300}"
# 跑客户端的那几个 `r` 调用必须自带一个**大得多**的超时：gate.sh 的 r() 默认
# SSH_TIMEOUT=600s，那是给"写 2 万条"量级的调用定的。4GB 的 PUT 是几十分钟、
# 1000 次 1GB 的 SCAN 可能是几小时，用默认值的话每个阶段都会在第 10 分钟被
# with_timeout 掐掉，而回执是空的——判据会报成"客户端没有输出"，指向别处。
CLIENT_TIMEOUT="${CLIENT_TIMEOUT:-86400}"
WATCH_IV="${WATCH_IV:-60}"
SAMPLE_IV="${SAMPLE_IV:-15}"
FLOOR_GB="${FLOOR_GB:-40}"
LAG_ENTRIES="${LAG_ENTRIES:-200000}"
LOST_KEYS="${LOST_KEYS:-fail}"
GC_STABLE_CHECKS="${GC_STABLE_CHECKS:-4}"
PHASES="${PHASES:-A B C}"
SMOKE_MB="${SMOKE_MB:-400}"
# 阶段 C 默认在**正式规模**上做崩溃恢复：恢复时间随 key 数量走（要重放 valuelog 并重建
# RocksDB），400MB 上通过说明不了 4GB 上通过。代价是要先把数据重写一遍，所以默认只做
# VSIZES 的第一档（64B 那档 key 最多，是最坏情形）。
C_MB="${C_MB:-$TOTAL_MB}"
C_VSIZES="${C_VSIZES:-$(printf '%s' "$VSIZES" | awk '{print $1}')}"
OUT="${OUT:-$HOME/work/maintable3-$LABEL.csv}"
LOG="$HOME/work/maintable3-$LABEL.log"
CLIENT_HOST=${CLIENT_HOST:-tikv240}
COMMIT=$(git rev-parse --short HEAD)
ALL=$(servers_str)
PEERSTR=$(peers_str)

# fail 必须在任何可能置位它的语句之前初始化（gate-audit 第十七节）。
fail=0
: > "$LOG"
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
warn() { say "[注意] $*"; }

# **采样器也要收掉，不能只用 gate.sh 的 cleanup_nodes。**
#
# cleanup_nodes 只发 `three-node.sh stop`，而采样循环的退出条件是
# `while [ -f "$D/sampler.pid" ]`——节点死了它照样接着跑，往 CSV 里写一堆 rss=0 的行，
# 而且**三台共用机器上各留一个后台循环**。2026-09-18 实测：驱动中途失败退出之后，
# 三个采样器仍在跑，是手工停掉的。共用机器上不留自己的残留是硬约束。
#
# 顺序是先停采样器再停节点：反过来的话，节点没了而采样器还活着的那几秒会写进几行
# 全零，看起来像"进程崩了"。
mt3_cleanup() {
    local i
    for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh samplestop $i" >/dev/null 2>&1 || true; done
    cleanup_nodes
}
trap mt3_cleanup EXIT

# 启动前再清一遍：trap 覆盖不了 SIGKILL，残留会把下一轮的启动顶掉。
cleanup_nodes

# ---------------------------------------------------------------------------
# 采样、看门狗、落后判定
# ---------------------------------------------------------------------------

# last_sample <节点号> —— 取该节点 sample.csv 的最后一行（采样器写在节点本机）。
# 用它而不是 `three-node.sh report`：report 要对整份节点日志做十几遍 cat|grep，
# 一次 10 小时的跑法按 60 秒轮询就是上千次全量读日志。
last_sample() { rq "$(host_of "$1")" "tail -1 ~/work/three-$1/sample.csv 2>/dev/null; [ -f ~/work/three-$1/DISK_LOW ] && echo DISK_LOW"; }

scol() { printf '%s' "$1" | head -1 | cut -d, -f"$2"; }

# lag_check —— 跨节点比压缩点，回答"有没有某台明显落后"。
#
# 为什么用 base_index（压缩点）而不是活动日志的字节数：三个节点各自独立触发 GC 与压缩，
# 某一台刚把 valuelog 切成有序文件时它的活动日志会瞬间变短，于是字节数在**健康**的
# 运行里也会大幅分叉——拿它判会假失败。压缩点是单调的，且一个真正掉队的副本压缩点必然
# 跟着落后。
#
# 这里只记警告不判失败：压缩点的推进还受本机负载影响（node55 只有 40 核），一次落差
# 说明不了是 bug。真正的权威信号是 leader 打出的 [LOG-PINNED]/[LOG-TRUNCATE]/[LOG-STUCK]
# ——那几行指名是哪个 peer、复制到了哪个位点，收尾时逐行打出来。
# 结果写全局 LAG_SPREAD，**不 echo**。第一版是 echo 出来给调用方取，而 watchdog 里写的是
# `lag_check >/dev/null`——那把 warn 的那一行也一起吞了，于是"有节点落后"的警告在终端上
# 一次都看不见。这正是本仓库反复踩的那个形态：判据自己坏了。
LAG_SPREAD=NA
LAG_OVER=0
lag_check() {
    local i s b mn="" mx="" who="" lines=""
    for i in 0 1 2; do
        s=$(last_sample "$i"); lines="$lines
    node$i $(printf '%s' "$s" | head -1)"
        b=$(scol "$s" 6); case "$b" in ''|*[!0-9]*) continue;; esac
        [ -z "$mn" ] && { mn=$b; mx=$b; who=$i; }
        [ "$b" -lt "$mn" ] && { mn=$b; who=$i; }
        [ "$b" -gt "$mx" ] && mx=$b
    done
    [ -n "$mn" ] || { LAG_SPREAD=NA; return 0; }
    LAG_SPREAD=$((mx - mn))
    # **只报"持续"的落差，一次尖峰不报。**
    #
    # 一轮 GC 的错位必然产生一次尖峰：某台先跑完一轮，它就删了旧库旧日志、压缩点往前跳，
    # 而另两台还揣着两份。2026-09-18 的 4GB 跑里出现过两次，都自己收了：
    #     64B 写入期   峰值 717 万 -> 41 万（node1 多跑完一轮）
    #     256B 混合期  峰值 277 万 -> 95 万（leader 少跑一轮，混合负载全压在它身上）
    # 两次都在轮数追平后收敛，**都是良性的**。一个每次都报、每次都良性的警告只会
    # 训练人去忽略它——而这个判据存在的意义正是"某台真的掉队了"。
    # 所以要求连续 LAG_STREAK 次检查都超阈值才报。真掉队的副本压缩点会一直落后，
    # 尖峰不会。
    if [ "$LAG_SPREAD" -gt "$LAG_ENTRIES" ]; then
        LAG_OVER=$((LAG_OVER + 1))
        if [ "$LAG_OVER" -ge "${LAG_STREAK:-3}" ]; then
            warn "压缩点跨节点落差连续 ${LAG_OVER} 次超阈值，现为 ${LAG_SPREAD} 条（最慢 node${who} = ${mn}，最快 = ${mx}，阈值 ${LAG_ENTRIES}）"
            echo "$lines" | tee -a "$LOG"
        fi
    else
        # 收回阈值之内就清零，并且把刚才那串尖峰说出来——"出现过又收了"本身是有用的
        # 信息（它就是 GC 错位的指纹），但它不该长得像一次失败。
        if [ "$LAG_OVER" -gt 0 ]; then
            say "压缩点落差曾连续 ${LAG_OVER} 次超阈值，现已收回 ${LAG_SPREAD} 条（GC 轮数错位的常见形态）"
        fi
        LAG_OVER=0
    fi
}

# watchdog —— 每 WATCH_IV 秒一次。返回非 0 表示必须中止当前阶段。
# 三件事：节点死了、盘要满了、采样器停了（停了就等于失去全部观测）。
watchdog() {
    local label=$1 i s rss bad=0
    for i in 0 1 2; do
        s=$(last_sample "$i")
        if [ -z "$s" ]; then
            warn "$label: node$i 取不到采样（ssh 超时或采样器死了）"
            bad=1; continue
        fi
        case "$s" in *DISK_LOW*)
            say "[中止] $label: node$i 所在盘剩余空间低于 ${FLOOR_GB}GB"
            bad=1;;
        esac
        rss=$(scol "$s" 2)
        case "$rss" in ''|0) say "[中止] $label: node$i 的 RSS 为 0——进程已经不在了"; bad=1;; esac
    done
    lag_check
    return $bad
}

# run_watched <标签> <输出文件> <要在客户端机器上跑的命令>
# 客户端放后台，主脚本在前台轮询看门狗。这样一个跑几小时的阶段不会变成"什么都看不见"。
run_watched() {
    local label=$1 out=$2; shift 2
    local cpid rc
    SSH_TIMEOUT=$CLIENT_TIMEOUT r "$CLIENT_HOST" "$*" > "$out" 2>&1 &
    cpid=$!
    while kill -0 "$cpid" 2>/dev/null; do
        sleep "$WATCH_IV"
        kill -0 "$cpid" 2>/dev/null || break
        if ! watchdog "$label"; then
            say "[中止] $label: 看门狗判定必须停下，杀掉客户端"
            kill "$cpid" 2>/dev/null; wait "$cpid" 2>/dev/null
            fail=1; return 1
        fi
    done
    wait "$cpid"; rc=$?
    return $rc
}

# ---------------------------------------------------------------------------
# 起停节点
# ---------------------------------------------------------------------------

# three-node.sh 自己就有 SYSTEM 环境变量并据此传 `-system`，所以这里**只能**决定
# 传给它的 SYSTEM 值与额外的 flag，不能把 `-system` 再塞进 EXTRA——那会让命令行上
# 出现两个 -system。Go 的 flag 是后者覆盖前者，不报错，于是 nezha-avp 会静默地按
# 一个意料之外的顺序生效。
node_system() {
    case "$SYSTEM" in
        nezha|nezha-nogc|original|lsm-raft) printf '%s' "$SYSTEM" ;;
        nezha-avp) printf '%s' "nezha" ;;
        *) echo "未知 SYSTEM=$SYSTEM" >&2; return 1 ;;
    esac
}
node_extra() {
    local e=""
    [ "$SYSTEM" = nezha-avp ] && e="-inlinePlacement"
    [ -n "$PARTITION_MB" ] && e="$e -partitionTargetMB $PARTITION_MB"
    printf '%s' "$e"
}

start_cluster() { # $1=vsize $2=entries
    local vs=$1 n=$2 i P IP OUT w rc NS EX started=0
    NS=$(node_system) || return 1
    EX=$(node_extra)
    for i in 0 1 2; do
        read -r P IP <<<"$(port_of "$i")"
        OUT=$(rq "$(host_of "$i")" \
            "BIN=normal SYSTEM=$NS SYNC_WAL=$SYNC_WAL GCGB=$GCGB PEERS='$PEERSTR' EXTRA='$EX' ~/three-node.sh start $i $P $IP $vs $n" \
            | tail -1)
        say "${OUT:-（无回执）}"
        w=$(require_out "$OUT" "start node$i"); rc=$?
        [ -n "$w" ] && echo "$w" | tee -a "$LOG"
        [ $rc = 0 ] || { fail=1; return 1; }
        case "$OUT" in STARTED*) started=$((started+1)) ;;
            *) say "start node$i 回执不是 STARTED"; fail=1; return 1 ;;
        esac
        [ "$i" = 0 ] && sleep 3
    done
    [ "$started" = 3 ] || { say "只起来了 $started 个节点"; fail=1; return 1; }
    sleep 12
    for i in 0 1 2; do
        rq "$(host_of "$i")" "~/three-node.sh sample $i $SAMPLE_IV $FLOOR_GB" | tee -a "$LOG"
    done
    return 0
}

stop_sampling() {
    local i
    for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh samplestop $i" | tee -a "$LOG"; done
}

# collect_cell —— 把三个节点的采样峰值、落后原文、REPORT 行都取回来，并归档整份 sample.csv。
collect_cell() { # $1=本格的归档目录名
    local dir="$HOME/work/mt3-$LABEL/$1" i rep why
    mkdir -p "$dir"
    for i in 0 1 2; do
        rq "$(host_of "$i")" "~/three-node.sh samplepeak $i" | sed "s/^PEAK/PEAK node$i/" | tee -a "$LOG"
        rq "$(host_of "$i")" "~/three-node.sh lagsay $i 12" | tee -a "$LOG"
        r "$(host_of "$i")" "cat ~/work/three-$i/sample.csv" > "$dir/sample-node$i.csv" 2>/dev/null
        r "$(host_of "$i")" "cat ~/work/three-$i/n.log" > "$dir/n-node$i.log" 2>/dev/null
        rep=$(r "$(host_of "$i")" "~/three-node.sh report $i")
        echo "$rep" | sed 's/^/    /' | tee -a "$LOG"
        why=$(gate_report_ok "$rep" yes "node$i") || fail=1
        [ -n "$why" ] && echo "$why" | tee -a "$LOG"
    done
}

# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------

if [ ! -f "$OUT" ]; then
  echo "commit,label,phase,topo,system,syncwal,gcgb,vsize,entries,total_mb,op,n,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,min_ms,max_ms,ops,bytes,elapsed_s,ops_per_s,mb_per_s,extra,gc_max,lost_keys,rss_peak_mb,fd_peak,lag_spread" > "$OUT"
fi
# field <文本> <键>：取不到给 NA。空字段在汇总表里看起来只是"这列没测"，很容易被读过去。
field(){ local v; v=$(grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2); echo "${v:-NA}"; }

# ---------------------------------------------------------------------------
# 一格（一个 value 档）
# ---------------------------------------------------------------------------

run_cell() { # $1=phase $2=total_mb $3=vsize
    local phase=$1 mb=$2 vs=$3
    local n rec gap cell d gcmax lost rsspk fdpk lagsp
    rec=$(record_bytes "$vs")
    n=$(awk -v mb="$mb" -v r="$rec" 'BEGIN{printf "%d", mb*1048576/r}')
    gap="$SCAN_GAP"
    [ -z "$gap" ] && gap=$(( n / SCAN_FRAC ))
    [ "$gap" -ge 1 ] && [ "$gap" -lt "$n" ] || { say "gapkey=${gap} 不在 [1,${n}) 内（vsize=${vs}）"; fail=1; return 1; }
    cell="$phase-${vs}B"
    d="$HOME/work/mt3-$LABEL/$cell"; mkdir -p "$d"

    say "===== [${cell}] value=${vs}B entries=${n} total=${mb}MB gapkey=${gap}（单次约 $((gap*rec/1048576))MB）====="
    start_cluster "$vs" "$n" || return 1

    # ---- PUT ----
    say "[$cell] PUT $n 条 × ${vs}B，并发 $PUT_CLIENTS"
    run_watched "$cell/PUT" "$d/put.out" \
        "source ~/env.sh; /tmp/mt3-randwrite_goroutine -cnums $PUT_CLIENTS -dnums $n -vsize $vs -servers $ALL" \
        || return 1
    local PUTL PUTT
    PUTL=$(grep '^\[LATENCY\]' "$d/put.out" | tail -1)
    PUTT=$(grep '^\[THROUGHPUT\]' "$d/put.out" | tail -1)
    [ -n "$PUTL" ] || { tail -20 "$d/put.out" | tee -a "$LOG"; say "[$cell] PUT 无分位数输出"; fail=1; return 1; }
    say "[$cell] PUT $(printf '%s' "$PUTT" | cut -c1-120)"

    # ---- 等 GC 轮数稳定 ----
    # 不能只等"至少一轮"：GC 每 5 秒查一次阈值，写完之后新 valuelog 还会继续被回收，
    # 而读性能取决于**布局**。两格在不同轮数的布局上测，差异就无法归因。
    local prev=-1 stable=0 k g cur
    gcmax=0
    for k in $(seq 1 120); do
        cur=0
        for i in 0 1 2; do
            g=$(scol "$(last_sample "$i")" 5); case "$g" in ''|*[!0-9]*) g=0;; esac
            [ "$g" -gt "$cur" ] && cur=$g
        done
        gcmax=$cur
        if [ "$cur" = "$prev" ] && [ "$cur" -ge 1 ]; then
            stable=$((stable+1)); [ "$stable" -ge "$GC_STABLE_CHECKS" ] && break
        else
            stable=0; prev=$cur
        fi
        sleep 10
    done
    say "[${cell}] GC 轮数 = ${gcmax}（连续 ${stable} 次不变）"
    # **轮数不变 ≠ 没有一轮在途中。** 计数数的是"轮垃圾回收完成"这行，而 numGC 在一轮
    # **开始**时就自增、产物文件也随之改名。于是完成数可以连续 40 秒不变，而下一轮正在
    # 往盘上写：2026-09-18 实测，第 8 轮完成后判了稳定，而第 9 轮在途，紧接着的
    # lost-keys.py 读到一个写了一半的布局、报错退出，回执为空 -> NA。
    # 所以再等 gc_in_progress 落回 false，这个字段就是节点自己写的、口径不会错。
    local k2 inflight
    for k2 in $(seq 1 60); do
        inflight=0
        for i in 0 1 2; do
            rq "$(host_of "$i")" "grep -c '\"gc_in_progress\": true' ~/work/three-$i/data/kv_state.json 2>/dev/null" \
                | grep -q '^1' && inflight=1
        done
        [ "$inflight" = 0 ] && break
        sleep 10
    done
    if [ "$inflight" != 0 ]; then
        warn "[${cell}] 600s 内仍有节点的 gc_in_progress 为 true，后面读盘的检查可能读到半成品"
    else
        say "[${cell}] 三个节点的 gc_in_progress 都已落回 false（等了 $((k2*10))s）"
    fi
    if [ "$gcmax" -lt 1 ] && [ "$SYSTEM" != nezha-nogc ] && [ "$SYSTEM" != original ]; then
        say "[$cell] GC 一轮都没跑（阈值 ${GCGB}GB）——读路径不走有序文件，这一格测的不是要测的东西"
        fail=1; return 1
    fi

    # ---- 搬丢了没有 ----
    # 放在读之前：丢了 key 会让后面的 GET/SCAN 数字失去意义。GC 搬丢记录不报任何错，
    # 只在某次 GET 上变成一个 NOKEY，而命中率看不出来（CLAUDE.md 记了这一条）。
    lost=NA
    if [ "$LOST_KEYS" != off ]; then
        local INLINE_TH=0
        [ "$SYSTEM" = nezha-avp ] && INLINE_TH="${INLINE_THRESHOLD:-512}"
        # **超时要给够。** 这个脚本把盘上出现过的 key 全收进一个 Python 集合再做差集，
        # 4570 万个 key 要跑十几分钟，而 r() 默认 SSH_TIMEOUT=600s 是按"写 2 万条"那个
        # 量级定的。2026-09-18 实测：4GB 那一格因此被 with_timeout 掐断、回执为空。
        # stderr 要留下来。r() 把 stderr 丢进 /dev/null（那是为了让控制类调用安静），
        # 于是这个检查失败时只剩一个 NA，**看不出是超时、是 python 报错、还是路径不对**。
        # 2026-09-18 就为此查了半天：真因是 GC 在途、读到半成品，而 NA 本身什么都没说。
        # 所以这里用 2>&1 收进文件，拿不到数字时把最后几行打出来。
        local lostlog="$d/lost-keys.out"
        SSH_TIMEOUT=${LOST_TIMEOUT:-7200} r "$(host_of 0)" \
            "cd ~/work/Nezha && python3 scripts/bench/lost-keys.py ~/work/three-0 $n $vs $INLINE_TH 2>&1" \
            > "$lostlog" 2>&1
        lost=$(grep -o '丢失 [0-9]*' "$lostlog" | grep -o '[0-9]*' | tail -1)
        lost="${lost:-NA}"
        # **NA 在 LOST_KEYS=fail 下必须判失败。**
        #
        # 第一版的 else 分支把 NA 和 0 一起打成"盘上丢失 NA 条"就往下走了。于是 4GB
        # 那一格在超时之后**照常跑完 GET 与 SCAN**，而唯一能发现"GC 搬丢了记录"的检查
        # 一条都没做——GC 搬丢记录不报任何错，只会在某次 GET 上变成一个 NOKEY，
        # 而命中率看不出来（CLAUDE.md 记着这一条）。
        # "拿不到判据不等于判据通过"是 gate.sh 里反复写的规则，这里却违反了它。
        if [ "$lost" = NA ]; then
            say "[$cell] 丢 key 检查没有回执——拿不到判据不等于通过。它自己的输出末尾："
            tail -6 "$lostlog" 2>/dev/null | sed 's/^/        /' | tee -a "$LOG"
            [ "$LOST_KEYS" = fail ] && { fail=1; return 1; }
            warn "[$cell] LOST_KEYS=warn，继续，但这一格**没有**做过丢 key 检查"
        elif [ "$lost" -gt 0 ]; then
            say "[$cell] node0 盘上丢了 ${lost} 条记录"
            [ "$LOST_KEYS" = fail ] && { fail=1; return 1; }
        else
            say "[$cell] node0 盘上丢失 0 条"
        fi
    fi

    # ---- GET ----
    say "[$cell] GET $((GET_OPS*GET_TESTS)) 次，并发 $GET_CLIENTS"
    run_watched "$cell/GET" "$d/get.out" \
        "source ~/env.sh; /tmp/mt3-zipf_read -cnums $GET_CLIENTS -dnums $GET_OPS -tests $GET_TESTS -rest $REST_SEC -keyspace $n -servers $ALL" \
        || return 1
    local GETL GETT HIT
    GETL=$(grep '^\[LATENCY\]' "$d/get.out" | tail -1)
    GETT=$(grep '^\[THROUGHPUT\]' "$d/get.out" | tail -1)
    HIT=$(grep '^\[HITRATE\]' "$d/get.out" | tail -1)
    [ -n "$GETL" ] || { tail -20 "$d/get.out" | tee -a "$LOG"; say "[$cell] GET 无分位数输出"; fail=1; return 1; }
    say "[$cell] GET $(printf '%s' "$GETT" | cut -c1-120)"

    # ---- SCAN ----
    # 并发恒为 1：一次范围查询本身读取量就大，再叠并发只会让各 goroutine 的随机起点
    # 互相冲刷缓存，结果不稳定且难以归因。
    say "[${cell}] SCAN $((SCAN_DNUMS*SCAN_TESTS)) 次 × gapkey=${gap}，并发 1"
    run_watched "$cell/SCAN" "$d/scan.out" \
        "source ~/env.sh; /tmp/mt3-scan_pro -cnums 1 -dnums $SCAN_DNUMS -tests $SCAN_TESTS -rest $REST_SEC -gapkey $gap -keyspace $n -servers $ALL" \
        || return 1
    local SCANL SCANT YIELD
    SCANL=$(grep '^\[LATENCY\]' "$d/scan.out" | tail -1)
    SCANT=$(grep '^\[THROUGHPUT\]' "$d/scan.out" | tail -1)
    YIELD=$(grep '^\[SCANYIELD\]' "$d/scan.out" | tail -1)
    [ -n "$SCANL" ] || { tail -20 "$d/scan.out" | tee -a "$LOG"; say "[$cell] SCAN 无分位数输出"; fail=1; return 1; }
    say "[$cell] SCAN $(printf '%s' "$SCANT" | cut -c1-120)"

    # ---- 混合：扫描的同时写和点读 ----
    # 立意见文件开头。判据不是吞吐，而是"混合时的 PUT/GET 延迟比单独跑时差多少"，
    # 以及 GC 轮数在这段里是否停滞。
    local MIXL="" MIXT="" MIXPL="" MIXPT="" MIXSL="" MIXST=""
    if [ "$MIXED_SEC" -gt 0 ]; then
        # **规模按条数定，不能用 timeout 掐。** 第一版是 `timeout $MIXED_SEC <客户端>`，
        # 而三个 bench 工具都是在**跑完之后**才打 [LATENCY]/[THROUGHPUT]——被 SIGTERM
        # 掐掉的话三份输出全是空的，于是这一段只剩"跑过了"，一个数字都拿不到。
        # 改成用刚测出来的 p50 反推条数：每个客户端各跑约 MIXED_SEC 秒的量。
        # 竞争之下它们都会比单跑慢，所以实际时长会超过 MIXED_SEC；先跑完的那个退出后
        # 竞争就减弱了，这一点写在这里，不要把混合期的数字当成稳态。
        local sp50 gp50 pp50 mscan mget mput
        sp50=$(field "$SCANL" p50); gp50=$(field "$GETL" p50); pp50=$(field "$PUTL" p50)
        mscan=$(awk -v s="$MIXED_SEC" -v l="$sp50" 'BEGIN{ if(l+0>0) printf "%d", s*1000/l; else print 0 }')
        mget=$(awk -v s="$MIXED_SEC" -v l="$gp50" -v c="$((GET_CLIENTS/5))" 'BEGIN{ if(l+0>0) printf "%d", s*1000/l*c; else print 0 }')
        mput=$(awk -v s="$MIXED_SEC" -v l="$pp50" -v c="$((PUT_CLIENTS/5))" 'BEGIN{ if(l+0>0) printf "%d", s*1000/l*c; else print 0 }')
        [ "$mscan" -lt 1 ] && mscan=1
        [ "$mget" -lt 1000 ] && mget=1000
        [ "$mput" -lt 1000 ] && mput=1000
        [ "$mput" -gt "$n" ] && mput=$n
        say "[${cell}] 混合：SCAN(1)×${mscan} 同时 PUT($((PUT_CLIENTS/5)))×${mput} + GET($((GET_CLIENTS/5)))×${mget}"
        local gcbefore=$gcmax
        SSH_TIMEOUT=$CLIENT_TIMEOUT r "$CLIENT_HOST" "source ~/env.sh; /tmp/mt3-scan_pro -cnums 1 -dnums $mscan -tests 1 -rest 0 -gapkey $gap -keyspace $n -servers $ALL" \
            > "$d/mixed-scan.out" 2>&1 &
        local SP=$!
        SSH_TIMEOUT=$CLIENT_TIMEOUT r "$CLIENT_HOST" "source ~/env.sh; /tmp/mt3-randwrite_goroutine -cnums $((PUT_CLIENTS/5)) -dnums $mput -vsize $vs -keyspace $n -dist uniform -servers $ALL" \
            > "$d/mixed-put.out" 2>&1 &
        local WP=$!
        SSH_TIMEOUT=$CLIENT_TIMEOUT r "$CLIENT_HOST" "source ~/env.sh; /tmp/mt3-zipf_read -cnums $((GET_CLIENTS/5)) -dnums $mget -tests 1 -rest 0 -keyspace $n -servers $ALL" \
            > "$d/mixed-get.out" 2>&1 &
        local GP=$!
        while kill -0 "$SP" 2>/dev/null || kill -0 "$WP" 2>/dev/null || kill -0 "$GP" 2>/dev/null; do
            sleep "$WATCH_IV"
            watchdog "${cell}/MIXED" || { say "[中止] ${cell}/MIXED 看门狗判定必须停下"
                kill "$SP" "$WP" "$GP" 2>/dev/null; fail=1; break; }
        done
        wait "$SP" 2>/dev/null; wait "$WP" 2>/dev/null; wait "$GP" 2>/dev/null
        MIXL=$(grep '^\[LATENCY\]' "$d/mixed-get.out" | tail -1)
        MIXT=$(grep '^\[THROUGHPUT\]' "$d/mixed-get.out" | tail -1)
        # 混合阶段的 **PUT** 才是那个发现的关键数字（扫描按住 apply → PUT 的 max 爆掉），
        # 而第一版只把混合 GET 写进 CSV，PUT 与 SCAN 只打进日志。于是画图要用的那一列
        # 根本不在表里，只能回去翻 mixed-put.out。三个都进表。
        MIXPL=$(grep '^\[LATENCY\]' "$d/mixed-put.out" | tail -1)
        MIXPT=$(grep '^\[THROUGHPUT\]' "$d/mixed-put.out" | tail -1)
        MIXSL=$(grep '^\[LATENCY\]' "$d/mixed-scan.out" | tail -1)
        MIXST=$(grep '^\[THROUGHPUT\]' "$d/mixed-scan.out" | tail -1)
        say "[${cell}] 混合 GET  $(printf '%s' "${MIXL:-无输出}" | cut -c1-130)"
        say "[${cell}] 混合 PUT  $(grep '^\[LATENCY\]' "$d/mixed-put.out" | tail -1 | cut -c1-130)"
        say "[${cell}] 混合 SCAN $(grep '^\[LATENCY\]' "$d/mixed-scan.out" | tail -1 | cut -c1-130)"
        say "[${cell}] 单跑对照 GET p50=${gp50}ms PUT p50=${pp50}ms SCAN p50=${sp50}ms"
        local gcafter=0 g2
        for i in 0 1 2; do
            g2=$(scol "$(last_sample "$i")" 5); case "$g2" in ''|*[!0-9]*) g2=0;; esac
            [ "$g2" -gt "$gcafter" ] && gcafter=$g2
        done
        say "[${cell}] 混合期间 GC 轮数 ${gcbefore} -> ${gcafter}"
        # **只在"写入量足够触发一轮"时才提这件事。**
        #
        # 第一版无条件地写"GC 一轮都没推进——扫描按住 kvs.mu 的迹象"。2026-09-18 实测：
        # 64B 档确实是 1 -> 1，而真实原因是混合阶段只覆盖写了 n/20 条 × 94B ≈ 21MB，
        # 离 GCGB=0.3GB 差着一个数量级——阈值压根没被跨过。同一轮里 256B 档推进了
        # 1 -> 2，因为同样条数的字节数是 4 倍。
        # 于是那条警告把**观察**与**归因**绑在一起，而归因指向被测系统。这正是本项目
        # 反复踩的形态，所以判据改成：先看写入量够不够，够了才谈"为什么没推进"。
        local mixbytes need
        mixbytes=$(awk -v n="$mput" -v r="$rec" 'BEGIN{printf "%.0f", n*r}')
        need=$(awk -v g="$GCGB" 'BEGIN{printf "%.0f", g*1073741824}')
        if [ "$gcafter" = "$gcbefore" ]; then
            if awk -v a="$mixbytes" -v b="$need" 'BEGIN{exit !(a >= b)}'; then
                warn "[${cell}] 混合期间写了 $((mixbytes/1048576))MB（阈值 $((need/1048576))MB）却一轮 GC 都没推进——值得查"
            else
                say "[${cell}] 混合期间 GC 未推进属正常：只写了 $((mixbytes/1048576))MB，阈值 $((need/1048576))MB"
            fi
        fi
    fi

    # ---- 采样峰值与收尾 ----
    # 采样器必须在**所有等待窗口之后**才停（gate-audit 第二十节）：先停再等的话，
    # "峰值"只覆盖写入阶段，而压缩期那个数会变成一次孤立采样、读起来像压缩把内存搞大了。
    lag_check; lagsp=$LAG_SPREAD
    stop_sampling
    collect_cell "$cell"
    rsspk=$(awk -F, 'NR>1{if($2>m)m=$2} END{printf "%.1f", m/1024}' "$HOME/work/mt3-$LABEL/$cell/sample-node0.csv" 2>/dev/null)
    fdpk=$(awk -F, 'NR>1{if($3>m)m=$3} END{print m+0}' "$HOME/work/mt3-$LABEL/$cell/sample-node0.csv" 2>/dev/null)

    emit() { # emit <op> <latency 行> <throughput 行> <extra>
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$COMMIT" "$LABEL" "$phase" "$TOPO" "$SYSTEM" "$SYNC_WAL" "$GCGB" "$vs" "$n" "$mb" "$1" \
            "$(field "$2" n)" "$(field "$2" mean)" "$(field "$2" p50)" "$(field "$2" p90)" \
            "$(field "$2" p95)" "$(field "$2" p99)" "$(field "$2" p999)" "$(field "$2" min)" "$(field "$2" max)" \
            "$(field "$3" ops)" "$(field "$3" bytes)" "$(field "$3" elapsed)" "$(field "$3" ops_per_s)" "$(field "$3" mb_per_s)" \
            "$4" "$gcmax" "$lost" "${rsspk:-NA}" "${fdpk:-NA}" "$lagsp" >> "$OUT"
    }
    emit PUT  "$PUTL"  "$PUTT"  "NA"
    emit GET  "$GETL"  "$GETT"  "$(field "$HIT" ratio)"
    emit SCAN "$SCANL" "$SCANT" "$(field "$YIELD" pairs_per_query)"
    [ -n "$MIXL" ]  && emit MIXGET  "$MIXL"  "$MIXT"  "NA"
    [ -n "$MIXPL" ] && emit MIXPUT  "$MIXPL" "$MIXPT" "NA"
    [ -n "$MIXSL" ] && emit MIXSCAN "$MIXSL" "$MIXST" "NA"

    cleanup_nodes
    return 0
}

# ---------------------------------------------------------------------------
# 阶段 C：kill -9 三台 → 重启 → 重新校验
# ---------------------------------------------------------------------------

phase_c() { # $1=vsize
    local vs=$1 n rec i out w rc wins cell d
    rec=$(record_bytes "$vs")
    n=$(awk -v mb="$C_MB" -v r="$rec" 'BEGIN{printf "%d", mb*1048576/r}')
    cell="C-${vs}B"; d="$HOME/work/mt3-$LABEL/$cell"; mkdir -p "$d"
    say "===== [$cell] 崩溃恢复：写 $n 条 → kill -9 三台 → 重启 → 逐条校验 ====="
    start_cluster "$vs" "$n" || return 1

    out=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/scanverify -servers $ALL -leader 0 -dnums $n -vsize $vs -span 50 -sample 20" | grep -vE 'new pool success')
    echo "$out" | tail -3 | sed 's/^/    /' | tee -a "$LOG"
    echo "$out" | grep -q VERIFY_OK || { say "[$cell] 写入阶段就没校验通过"; fail=1; return 1; }

    wins=$(leader_wins)
    for i in 0 1 2; do
        out=$(rq "$(host_of "$i")" "~/three-node.sh kill9 $i" | tail -1)
        say "${out:-（无回执）}"
        w=$(require_out "$out" "kill9 node$i"); rc=$?
        [ -n "$w" ] && echo "$w" | tee -a "$LOG"
        [ $rc = 0 ] || { fail=1; return 1; }
        case "$out" in KILLED*) ;; *) say "kill9 node$i 回执不是 KILLED"; fail=1; return 1;; esac
    done
    for i in 0 1 2; do
        out=$(rq "$(host_of "$i")" "~/three-node.sh restart $i" | tail -1)
        say "${out:-（无回执）}"
        w=$(require_out "$out" "restart node$i"); rc=$?
        [ -n "$w" ] && echo "$w" | tee -a "$LOG"
        [ $rc = 0 ] || { fail=1; return 1; }
        case "$out" in RESTARTED*) ;; *) say "restart node$i 回执不是 RESTARTED"; fail=1; return 1;; esac
    done
    # 不用固定 sleep 等选举：minElectionTimeout=10s + 1s 抖动只是"察觉 leader 没了"的时间，
    # 还要加投票往返和可能的分票重选。固定 sleep 会把脚本常量钉在源码常量上
    # （gate-audit 第十一节与第二十三(b) 节都钉着这条）。
    wait_new_leader "$wins" 90 || return 1

    out=$(r "$CLIENT_HOST" "source ~/env.sh; /tmp/scanverify -servers $ALL -leader 0 -dnums $n -vsize $vs -span 50 -sample 40" | grep -vE 'new pool success')
    echo "$out" | tail -3 | sed 's/^/    /' | tee -a "$LOG"
    echo "$out" | grep -q VERIFY_OK || { say "[$cell] 重启后校验没通过"; fail=1; return 1; }
    say "[$cell] 重启后逐条校验通过"
    stop_sampling
    collect_cell "$cell"
    cleanup_nodes
    return 0
}

# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

say "拓扑 TOPO=${TOPO}：node0=$(host_of 0) node1=$(host_of 1) node2=$(host_of 2)"
say "驱动跑在 $(hostname 2>/dev/null)（ON_SERVER=${ON_SERVER}）；客户端在 $CLIENT_HOST"
say "commit=$COMMIT system=$SYSTEM syncWAL=$SYNC_WAL gcThresholdGB=$GCGB 阶段=[$PHASES]"
say "输出 $OUT / 日志 $LOG / 归档 $HOME/work/mt3-$LABEL/"

# 开跑前先确认三台机器上没有别人的进程。这三台是共用的，撞端口会同时毁掉两边的实验，
# 而 TOPO=three 用的 3088/30881 正是别人先前开的那对口子（gate.sh 里写了这件事）。
for i in 0 1 2; do
    h=$(host_of "$i")
    others=$(rq "$h" "pgrep 'nezha' 2>/dev/null | head -10 | xargs -r ps -o user=,pid=,etime=,args= -p 2>/dev/null | grep -v 'three-' | cut -c1-140")
    [ -z "$others" ] && continue
    say "[中止] $h 上有不属于我们的 nezha 进程（我们的数据目录一律是 ~/work/three-*）："
    echo "$others" | sed 's/^/    /' | tee -a "$LOG"
    say "       → 不要停别人的实验。等它跑完，或换时间。"
    exit 1
done

# bench 工具按 mt3- 前缀单独建一份：deploy.sh 建的 /tmp/scanverify 等是共用的，
# 正在跑别的验证时覆盖它们会把那一轮也换掉。
say "构建节点与客户端工具"
for t in randwrite_goroutine zipf_read scan_pro; do
    r "$CLIENT_HOST" "source ~/env.sh; cd ~/work/Nezha && go build -o /tmp/mt3-$t ./cmd/bench/$t/ && echo BUILD_OK_$t" | tee -a "$LOG"
done
for t in randwrite_goroutine zipf_read scan_pro; do
    grep -q "BUILD_OK_$t" "$LOG" || { say "$t 没建出来"; exit 1; }
done

for ph in $PHASES; do
    case "$ph" in
        A) for vs in $VSIZES; do run_cell A "$SMOKE_MB" "$vs" || break; done ;;
        B) for vs in $VSIZES; do run_cell B "$TOTAL_MB" "$vs" || break; done ;;
        C) for vs in $C_VSIZES; do phase_c "$vs" || break; done ;;
        *) say "未知阶段 $ph"; fail=1 ;;
    esac
done

say "===== 汇总 ====="
awk -F, 'NR>1{printf "%-3s %-5s v=%-5s p50=%-9s p99=%-9s ops/s=%-10s gc=%-3s lost=%-4s rss=%-8s fd=%-5s lag=%s\n", $3,$11,$8,$14,$17,$24,$27,$28,$29,$30,$31}' "$OUT" | tee -a "$LOG"
if [ "$fail" = 0 ]; then
    say "全部通过"
else
    say "有判据未通过（fail=${fail}），见上面的 [中止]/[闸门]/[注意] 行"
    exit 1
fi
