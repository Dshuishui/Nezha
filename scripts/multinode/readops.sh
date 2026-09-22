#!/bin/bash
# 只测读：GET 与 SCAN，四系统 × 六档，提交条件可切换。
#
# 为什么要有它：B3 那轮的读数据是**多数派**提交下测的，而 figs-leader/ 那套图里的
# GET/SCAN 直接复用了它（CSV 的 label 写着 aq-leader-PUT+B3-READ）。
# 复用的理由本身是对的——读路径不看日志提交条件，租约由**心跳**的多数派回执驱动
# （raft.go 的 leaseRound），与 updateCommitIndex 是两套独立机制。
#
# **但它掩盖了一个方向对异步档不利的风险**：异步档不等 follower，follower 就持续
# 掉队、leader 转而发快照（实测约 0.6 次/秒，是 baseline 的 34 倍），而发快照要抢
# leader 的盘和 CPU。读与补副本同时发生时，异步档的读可能**比多数派更差**——
# 而这一点一个数据都没有。既然异步档要当主配置，这个假设就是承重墙，必须实测。
#
# 也测点读的重复性：现有四个 AVP 点读数据分属 8c65866 / 85561c0 / 907812b 三个提交，
# 而 85561c0 改的正是点读路径。逐提交配对的 avp÷nezha 是 +9.4% / +7.2% / +1.1%（64B），
# 跨度 8 倍以上，而 GET 的跑间噪声是 ±5%：**幅度定不下来，只有方向站得住。**
#
# 为什么要单独写一个：现有的四个 AVP 点读数据分别来自 8c65866 / 85561c0 / 907812b
# 三个提交，而 85561c0 改的正是点读路径（-inlinePlacement 下每个库查两次）。
# 跨提交的三个数不是重复测量，是三次不同的实验——把它们算中位数没有意义。
# 逐提交配对的 avp÷nezha 是 +9.4% / +7.2% / +1.1%（64B），跨度 8 倍以上，
# 而 GET 的跑间噪声是 ±5%：**幅度完全定不下来，只有方向站得住。**
#
# 两层重复，量的是两件不同的事：
#   GET_REPEATS  同一次装载里连跑几遍   → 纯测量噪声（客户端、网络、page cache）
#   LOADS        重新灌一次数据再测     → 布局噪声（GC 分区边界、LSM 层次、盘上碎片）
# 后者才是论文里"重复实验"该有的口径，而它贵在灌数据（64B 档一次 27 分钟），
# 所以每次装载里顺手多跑几遍 GET——那几乎不要钱。
#
# 不跑丢 key 检查：64B 档要十几分钟，而正确性由 zipf_read 自己的 [HITRATE] 兜着，
# 比值不是 1.0000 就判这一格作废。
#
# SCAN 的口径**必须与 B3 一字不差**，否则异步档与多数派就不可比：
# 每格 40 次（SCAN_DNUMS=10 × SCAN_TESTS=4）、并发 1、gapkey = 条数/SCAN_FRAC(10)。
# SCAN_FRAC=10 让**六档扫到的字节数恒为约 1 GiB/次**——不变量是字节不是条数。
set -u
cd ~/work/Nezha || exit 1
source ~/env.sh 2>/dev/null || true
# **TOPO 要在 source gate.sh 之前设**：gate.sh 在被 source 的那一刻就读它，
# 漏了会静默退回默认的 two（端口 3099/3100），整轮跑在两节点拓扑上而日志里
# 没有任何迹象——2026-09-21 实测踩过。
TOPO=${TOPO:-three}
. scripts/multinode/gate.sh
. scripts/lib/bench-common.sh
require_driver_host || exit 1

LABEL=${1:-getonly}
TOTAL_MB=${TOTAL_MB:-10240}
# 默认只跑内联阈值以下的两档：那是 AVP 唯一可能有优势的地方（512B 以上
# nezha-avp 与 nezha 走同一条路径，按机制不该有差）。
VSIZES=${VSIZES:-"64 256"}
SYSTEMS=${SYSTEMS:-"nezha nezha-avp"}
LOADS=${LOADS:-1}
GET_REPEATS=${GET_REPEATS:-3}
# 要测哪些读操作。SCAN 很贵（B3 实测 64B 档一格 29 分钟，四系统 2 小时），
# 只测点读时设 OPS=GET。
OPS=${OPS:-"GET SCAN"}
PUT_CLIENTS=${PUT_CLIENTS:-100}
GET_CLIENTS=${GET_CLIENTS:-100}
GET_OPS=${GET_OPS:-200000}
GET_TESTS=${GET_TESTS:-10}
REST_SEC=${REST_SEC:-0}
# SCAN 参数照抄 B3：40 次 = 10 × 4，gapkey = 条数/10。
# **不要为了省时间改这三个数**，改了就不能和 B3 比，而这一轮存在的意义就是和它比。
SCAN_DNUMS=${SCAN_DNUMS:-10}
SCAN_TESTS=${SCAN_TESTS:-4}
SCAN_FRAC=${SCAN_FRAC:-10}
# **提交条件默认 majority，不默认 leader。** leader 档会确认没有任何 follower 拿到
# 的写，故障切换丢已确认数据（实测 72.60%，多数派对照 0.0000%）——一个会丢数据的
# 档位不该靠默认值生效，必须在调用处写出来。
COMMIT_QUORUM=${COMMIT_QUORUM:-majority}
GCGB=${GCGB:-0.3}
PARTITION_MB=${PARTITION_MB:-128}
SYNC_WAL=${SYNC_WAL:-0}
# 0 = 不开块缓存，与主表那 72 格一致。给了非 0 就**所有系统一起给**——
# 只给一边开缓存量出来的不是机制差异，是配置差异。
BLOCK_CACHE_MB=${BLOCK_CACHE_MB:-0}
GC_STABLE_CHECKS=${GC_STABLE_CHECKS:-3}
# set -u 下漏了这个变量，脚本会在第一格的写入那一步直接死，而且因为那句话在
# $( ) 里，set -u 只打死子 shell、父进程拿到空串继续跑——静默。实测踩过两次。
CLIENT_HOST=${CLIENT_HOST:-tikv240}

OUT=$HOME/work/readops-$LABEL.csv
LOG=$HOME/work/readops-$LABEL.log
: > "$LOG"
COMMIT=$(git rev-parse --short HEAD)
ALL=$(servers_str)
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
warn(){ echo "[$(date +%H:%M:%S)] ** $* **" | tee -a "$LOG"; }

echo "commit,label,quorum,block_cache_mb,load,system,vsize,entries,total_mb,op,rep,clients,gapkey,ops,bytes,elapsed_s,ops_per_s,mb_per_s,mean_ms,p50_ms,p90_ms,p95_ms,p99_ms,p999_ms,max_ms,hitrate,gc_rounds,snapshots" > "$OUT"

# **退出时一定停掉三个节点。** 上一版的 PUT-only 脚本没有这个：它死在第一格的写入上、
# 节点留着不动，下一段起来时端口全被占（address already in use），四格连锁失败。
# 一个会在任何出口都收尾的 trap 比"记得在每条失败路径上停"可靠。
stop_all(){ local i; for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh stop $i" >/dev/null 2>&1; done; }
trap stop_all EXIT INT TERM

# wait_gc_stable —— 等 GC 轮数连续不变，再等在途的那一轮落地。
#
# **轮数不变 ≠ 没有一轮在途中。** 计数数的是"轮垃圾回收完成"这行，而 numGC 在一轮
# **开始**时就自增、产物文件随之改名。于是完成数可以连续 40 秒不变而下一轮正在写盘，
# 紧接着的读就落在一个半成品布局上——2026-09-18 实测过一次。
# 所以再等三个节点的 gc_in_progress 落回 false，那个字段是节点自己写的、口径不会错。
wait_gc_stable(){
    local prev=-1 cur stable=0 i g k
    for k in $(seq 1 120); do
        cur=0
        for i in 0 1 2; do
            g=$(rq "$(host_of "$i")" "grep -c '轮垃圾回收完成' ~/work/three-$i/n.log 2>/dev/null" | head -1)
            # grep -c 无匹配时既打印 0 又返回 1，所以判的是内容不是退出码
            case "$g" in ''|*[!0-9]*) g=0;; esac
            [ "$g" -gt "$cur" ] && cur=$g
        done
        if [ "$cur" = "$prev" ] && [ "$cur" -ge 1 ]; then
            stable=$((stable+1)); [ "$stable" -ge "$GC_STABLE_CHECKS" ] && break
        else
            stable=0; prev=$cur
        fi
        sleep 10
    done
    GCMAX=$cur
    say "  GC 轮数=${GCMAX}（连续 ${stable} 次不变）"
    local inflight=1
    for k in $(seq 1 60); do
        inflight=0
        for i in 0 1 2; do
            rq "$(host_of "$i")" "grep -c '\"gc_in_progress\": true' ~/work/three-$i/data/kv_state.json 2>/dev/null" \
                | grep -q '^1' && inflight=1
        done
        [ "$inflight" = 0 ] && break
        sleep 10
    done
    [ "$inflight" = 0 ] || warn "600s 内仍有节点 gc_in_progress=true，这一格的读可能落在半成品布局上"
    # GC 一轮都没跑，读路径就不走有序文件——测的不是要测的东西。
    [ "$GCMAX" -ge 1 ] || { warn "GC 一轮都没跑（阈值 ${GCGB}GB），本格作废"; return 1; }
    return 0
}

# has_gc —— 这个系统会不会跑 GC。**baseline 与 nezha-nogc 的完成轮数恒为 0，
# 是设计不是失败。**不加这个守卫的后果实测过：wait_gc_stable 的条件是
# "轮数不变 且 ≥ 1"，对它们永远不成立，于是空转满 120 次 × 10 秒 = **每格白等 20 分钟**，
# 然后还会因为 GCMAX=0 把这一格判作废。CLAUDE.md 里记着这一条。
has_gc() { case "$1" in nezha|nezha-avp) return 0 ;; *) return 1 ;; esac; }

f(){ grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2; }

say "提交=$COMMIT  拓扑=three  系统=[$SYSTEMS]  档位=[$VSIZES]"
say "操作=[${OPS}]  提交条件=${COMMIT_QUORUM}  装载 ${LOADS} 轮 × 每轮 ${GET_REPEATS} 遍 GET（每遍 $((GET_OPS*GET_TESTS)) 次，并发 ${GET_CLIENTS}）"
say "每档 ${TOTAL_MB}MB，写入并发 ${PUT_CLIENTS}，GCGB=${GCGB}，PARTITION_MB=${PARTITION_MB}，BLOCK_CACHE_MB=${BLOCK_CACHE_MB}"

# 顺序：装载轮 → value 档 → 系统。
# **nezha 与 nezha-avp 相邻**，同一档同一轮里两者只隔一次装载，机器状态漂移最小——
# 这个对比是配对的，配对越紧越好。
for LOAD in $(seq 1 "$LOADS"); do
for VS in $VSIZES; do
for SY in $SYSTEMS; do
    case $SY in
        baseline)   NS=original;   EX="" ;;
        nezha-nogc) NS=nezha-nogc; EX="" ;;
        nezha)      NS=nezha;      EX="-partitionTargetMB $PARTITION_MB" ;;
        nezha-avp)  NS=nezha;      EX="-inlinePlacement -partitionTargetMB $PARTITION_MB" ;;
        *) warn "未知系统 ${SY}，跳过"; continue ;;
    esac
    EX="$EX -blockCacheMB ${BLOCK_CACHE_MB} -commitQuorum ${COMMIT_QUORUM}"
    REC=$(record_bytes "$VS")
    N=$(awk -v mb="$TOTAL_MB" -v r="$REC" 'BEGIN{printf "%d", mb*1048576/r}')
    CELL="L$LOAD-$SY-${VS}B"
    say "===== $CELL  记录长=$REC  条数=$N ====="
    stop_all   # 每格自带一次收尾，与上一格的结局无关
    ok=1
    for i in 0 1 2; do
        read -r P IP <<<"$(port_of "$i")"
        o=$(rq "$(host_of "$i")" "BIN=normal SYSTEM=$NS SYNC_WAL=$SYNC_WAL GCGB=$GCGB PEERS='$(peers_str)' EXTRA='$EX' ~/three-node.sh start $i $P $IP $VS $N" | tail -1)
        case "$o" in STARTED*) ;; *) warn "$CELL node$i 启动失败: ${o:-（无回执）}"; ok=0 ;; esac
    done
    [ "$ok" = 1 ] || { stop_all; continue; }
    sleep 12

    say "  灌数据 $N 条 × ${VS}B"
    SSH_TIMEOUT=86400 r "$CLIENT_HOST" \
        "source ~/env.sh; /tmp/mt3-randwrite_goroutine -cnums $PUT_CLIENTS -dnums $N -vsize $VS -servers $ALL" \
        > "$HOME/work/readops-$LABEL-$CELL-put.out" 2>&1
    PT=$(grep '^\[THROUGHPUT\]' "$HOME/work/readops-$LABEL-$CELL-put.out" | tail -1)
    if [ -z "$PT" ]; then
        warn "$CELL 灌数据没有吞吐输出，本格作废"
        tail -12 "$HOME/work/readops-$LABEL-$CELL-put.out" | tee -a "$LOG"
        stop_all; continue
    fi
    say "  $PT"

    GCMAX=0
    if has_gc "$SY"; then
        wait_gc_stable || { stop_all; continue; }
    else
        say "  ${SY} 不跑 GC（完成轮数恒为 0，是设计），跳过等待稳定"
    fi

    GAP=$(( N / SCAN_FRAC ))
    # 快照数只对 leader 有意义，而且要在读之前记一次基线：异步档在读的过程中
    # 还在给掉队的 follower 补副本，那份抢盘抢 CPU 的开销正是这一轮要看的东西。
    SNAP0=$(rq "$(host_of 0)" "grep -c '开始给它发快照' ~/work/three-0/n.log 2>/dev/null" | head -1)
    case "$SNAP0" in ''|*[!0-9]*) SNAP0=0 ;; esac

    for OP in $OPS; do
    case $OP in
      GET) REPS=$GET_REPEATS; CL=$GET_CLIENTS; GK=0 ;;
      # SCAN 的并发恒为 1：一次范围查询本身读取量就大，再叠并发只会让各 goroutine
      # 的随机起点互相冲刷缓存，结果不稳定且难以归因（maintable3.sh 里同一条理由）。
      # 也只跑一遍——B3 是一遍，而且一遍已经 40 次扫描。
      SCAN) REPS=1; CL=1; GK=$GAP ;;
      *) warn "未知操作 ${OP}，跳过"; continue ;;
    esac
    for REP in $(seq 1 "$REPS"); do
        OUTF="$HOME/work/readops-$LABEL-$CELL-$OP$REP.out"
        case $OP in
          GET)  CMD="/tmp/mt3-zipf_read -cnums $GET_CLIENTS -dnums $GET_OPS -tests $GET_TESTS -rest $REST_SEC -keyspace $N -servers $ALL" ;;
          SCAN) CMD="/tmp/mt3-scan_pro -cnums 1 -dnums $SCAN_DNUMS -tests $SCAN_TESTS -rest $REST_SEC -gapkey $GAP -keyspace $N -servers $ALL" ;;
        esac
        [ "$OP" = SCAN ] && say "  SCAN $((SCAN_DNUMS*SCAN_TESTS)) 次 × gapkey=${GAP}，并发 1"
        SSH_TIMEOUT=86400 r "$CLIENT_HOST" "source ~/env.sh; $CMD" > "$OUTF" 2>&1
        T=$(grep '^\[THROUGHPUT\]' "$OUTF" | tail -1)
        L=$(grep '^\[LATENCY\]'    "$OUTF" | tail -1)
        H=$(grep '^\[HITRATE\]'    "$OUTF" | tail -1)
        HR=$(f "$H" ratio); [ -n "$HR" ] || HR=NA
        SNAP=$(rq "$(host_of 0)" "grep -c '开始给它发快照' ~/work/three-0/n.log 2>/dev/null" | head -1)
        case "$SNAP" in ''|*[!0-9]*) SNAP=NA ;; *) SNAP=$((SNAP - SNAP0)) ;; esac
        if [ -z "$T" ] || [ -z "$L" ]; then
            # **NA 不是 0。**写 0 等于声称"测过了、就是零"，而实际是没拿到回执。
            warn "$CELL $OP rep$REP 没有吞吐或分位数输出，这一遍记 NA"
            tail -12 "$OUTF" | tee -a "$LOG"
            printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,NA,%s,%s,%s\n' \
                "$COMMIT" "$LABEL" "$COMMIT_QUORUM" "$BLOCK_CACHE_MB" "$LOAD" "$SY" "$VS" "$N" "$TOTAL_MB" \
                "$OP" "$REP" "$CL" "$GK" "$HR" "$GCMAX" "$SNAP" >> "$OUT"
            continue
        fi
        printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$COMMIT" "$LABEL" "$COMMIT_QUORUM" "$BLOCK_CACHE_MB" "$LOAD" "$SY" "$VS" "$N" "$TOTAL_MB" \
            "$OP" "$REP" "$CL" "$GK" \
            "$(f "$T" ops)" "$(f "$T" bytes)" "$(f "$T" elapsed)" "$(f "$T" ops_per_s)" "$(f "$T" mb_per_s)" \
            "$(f "$L" mean)" "$(f "$L" p50)" "$(f "$L" p90)" "$(f "$L" p95)" \
            "$(f "$L" p99)" "$(f "$L" p999)" "$(f "$L" max)" "$HR" "$GCMAX" "$SNAP" >> "$OUT"
        say "  $OP rep$REP ops/s=$(f "$T" ops_per_s) mb/s=$(f "$T" mb_per_s) p50=$(f "$L" p50) p99=$(f "$L" p99) 命中=$HR 读期间快照=$SNAP"
        # 命中率不是 1.0000 说明有 key 读不到——GC 搬丢记录不报任何错，只在某次
        # GET 上变成一个 NOKEY，所以这是这一格唯一的正确性哨兵。SCAN 不出这一行。
        case "$OP:$HR" in GET:1.0000|GET:1) ;; GET:*) warn "$CELL GET rep$REP 命中率 $HR ≠ 1.0000，这一格的数字不可用" ;; esac
    done
    done

    for i in 0 1 2; do
        r "$(host_of "$i")" "cat ~/work/three-$i/n.log" > "$HOME/work/readops-$LABEL-$CELL-node$i.log" 2>/dev/null
    done
    stop_all
    sleep 5
done
done
done

say "===== 汇总 ====="
column -s, -t < "$OUT" | tee -a "$LOG"
echo "########## 全部结束 ##########" | tee -a "$LOG"
