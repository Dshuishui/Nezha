#!/bin/bash
# 点读到底读没读物理盘：在 GET 阶段前后读 leader 的 /proc/diskstats，看块设备
# 实际给了多少字节。
#
# 为什么需要这个脚本：一直有个推断在支撑所有关于点读的解释——「10GB 的数据集在
# 251GB 内存的机器上整个躺在页缓存里，所以 pread 是 memcpy 而不是寻道」。
# 那个推断的证据一直是**间接的**（NoGC 多出的那一跳只要 0.028ms，物理盘读不可能
# 这么快）。而它决定了一整串结论：
#   - 「100GB 就能逼出磁盘」成不成立（不成立，100GB 仍小于空闲内存）
#   - 块缓存那个 flag 有没有用（没用，miss 落回页缓存）
#   - 要不要写 fadvise 工具去把页缓存扔掉
# 间接证据撑不住三层推论，所以直接量。
#
# 判据很硬：200 万次 GET × 1KB = **请求了 2GB 的 value**。
#   设备读 ≈ 0      → 页缓存全应答，前面的推断成立
#   设备读 ≳ 2GB    → 真在打盘，推断错了，点读的结论要重做
set -u
cd ~/work/Nezha || exit 1
source ~/env.sh 2>/dev/null || true
# TOPO 要在 source gate.sh 之前设，它在被 source 的那一刻就读。
TOPO=${TOPO:-three}
. scripts/multinode/gate.sh
. scripts/lib/bench-common.sh
require_driver_host || exit 1

LABEL=${1:-diskio}
TOTAL_MB=${TOTAL_MB:-10240}
VSIZES=${VSIZES:-"1024"}
SYSTEMS=${SYSTEMS:-"baseline nezha"}
PUT_CLIENTS=${PUT_CLIENTS:-100}
GET_CLIENTS=${GET_CLIENTS:-100}
GET_OPS=${GET_OPS:-200000}
GET_TESTS=${GET_TESTS:-10}
GCGB=${GCGB:-0.3}
PARTITION_MB=${PARTITION_MB:-128}
SYNC_WAL=${SYNC_WAL:-0}
BLOCK_CACHE_MB=${BLOCK_CACHE_MB:-0}
GC_STABLE_CHECKS=${GC_STABLE_CHECKS:-3}
CLIENT_HOST=${CLIENT_HOST:-tikv240}

OUT=$HOME/work/diskio-$LABEL.csv
LOG=$HOME/work/diskio-$LABEL.log
: > "$LOG"
COMMIT=$(git rev-parse --short HEAD)
ALL=$(servers_str)
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
warn(){ echo "[$(date +%H:%M:%S)] ** $* **" | tee -a "$LOG"; }

echo "commit,label,block_cache_mb,system,vsize,entries,total_mb,get_n,ops_per_s,p50_ms,hitrate,dev,read_bytes,read_ios,write_bytes,cache_before_gb,cache_after_gb,rss_mb" > "$OUT"

# has_gc —— 这个系统会不会跑 GC。baseline 与 nezha-nogc 的完成轮数恒为 0 是设计，
# 等它会空转满上限（120 次 × 10 秒）。写成函数而不是就地 case，是为了让
# gate-audit-multinode.sh 第三十五节看得见——它查的就是这个调用点，
# 而就地 case 在功能上等价、在判据眼里等于没有守卫（本脚本第一版实测被判漏报）。
has_gc() { case "$1" in nezha|nezha-avp) return 0 ;; *) return 1 ;; esac; }

stop_all(){ local i; for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh stop $i" >/dev/null 2>&1; done; }
trap stop_all EXIT INT TERM

# leader 的数据盘。**设备名要问机器，不要写死**：240 的 ~/work 在 /dev/sdc3，
# 另两台不一定一样，而写死一个不存在的名字会让采样恒为空——那正是「一个恒为 0
# 的列读起来像没问题」那类坑。
LEADER=$(host_of 0)
DEV=$(rq "$LEADER" "df --output=source ~/work 2>/dev/null | tail -1 | sed 's#/dev/##'")
[ -n "$DEV" ] || { warn "问不出 $LEADER 上 ~/work 的设备名，没法采样"; exit 1; }
rq "$LEADER" "grep -qE ' $DEV ' /proc/diskstats" || { warn "/proc/diskstats 里没有 $DEV"; exit 1; }
say "leader=$LEADER  数据盘=$DEV"

# diskstats 的字段（Documentation/admin-guide/iostats.rst）：
#   1 major  2 minor  3 name  4 读完成数  5 读合并数  6 **读扇区数**  7 读耗时ms
#   8 写完成数  9 写合并数  10 **写扇区数**
# 扇区恒为 512 字节，与文件系统块大小无关。
# **不要走 awk 的数值路径**：node55 是 mawk，printf "%d" 会把大数截断成 2^31-1，
# 看起来还像个正常数字。所以只用 awk 取字段，算术交给 bash。
dstat(){ rq "$LEADER" "awk '\$3==\"$DEV\"{print \$6, \$10, \$4}' /proc/diskstats" | head -1; }
cache_gb(){ rq "$LEADER" "free -g | awk '/^Mem/{print \$6}'" | head -1; }

for SY in $SYSTEMS; do
    case $SY in
        baseline)   NS=original;   EX="" ;;
        nezha-nogc) NS=nezha-nogc; EX="" ;;
        nezha)      NS=nezha;      EX="-partitionTargetMB ${PARTITION_MB}" ;;
        nezha-avp)  NS=nezha;      EX="-inlinePlacement -partitionTargetMB ${PARTITION_MB}" ;;
        *) warn "未知系统 ${SY}，跳过"; continue ;;
    esac
    EX="$EX -blockCacheMB ${BLOCK_CACHE_MB}"
for VS in $VSIZES; do
    REC=$(record_bytes "$VS")
    N=$(awk -v mb="$TOTAL_MB" -v r="$REC" 'BEGIN{printf "%d", mb*1048576/r}')
    CELL="$SY-${VS}B"
    say "===== $CELL  条数=$N  块缓存=${BLOCK_CACHE_MB}MB ====="
    stop_all
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
        > "$HOME/work/diskio-$LABEL-$CELL-put.out" 2>&1
    PT=$(grep '^\[THROUGHPUT\]' "$HOME/work/diskio-$LABEL-$CELL-put.out" | tail -1)
    [ -n "$PT" ] || { warn "$CELL 灌数据没有吞吐输出，本格作废"; stop_all; continue; }
    say "  $PT"

    # baseline / nezha-nogc 的 GC 完成轮数恒为 0，是设计——等它会空转满上限。
    if has_gc "$SY"; then
        prev=-1; stable=0; GCMAX=0
        for k in $(seq 1 120); do
            cur=0
            for i in 0 1 2; do
                g=$(rq "$(host_of "$i")" "grep -c '轮垃圾回收完成' ~/work/three-$i/n.log 2>/dev/null" | head -1)
                case "$g" in ''|*[!0-9]*) g=0;; esac
                [ "$g" -gt "$cur" ] && cur=$g
            done
            if [ "$cur" = "$prev" ] && [ "$cur" -ge 1 ]; then
                stable=$((stable+1)); [ "$stable" -ge "$GC_STABLE_CHECKS" ] && break
            else stable=0; prev=$cur; fi
            sleep 10
        done
        GCMAX=$cur
        say "  GC 轮数=${GCMAX}（连续 ${stable} 次不变）"
        for k in $(seq 1 60); do
            inf=0
            for i in 0 1 2; do
                rq "$(host_of "$i")" "grep -c '\"gc_in_progress\": true' ~/work/three-$i/data/kv_state.json 2>/dev/null" | grep -q '^1' && inf=1
            done
            [ "$inf" = 0 ] && break
            sleep 10
        done
    else
        say "  ${SY} 不跑 GC（完成轮数恒为 0，是设计），跳过等待稳定"
    fi

    # **采样要紧贴 GET。**中间任何一步（等 GC、数 key、抓 RSS）都会自己读盘，
    # 算进去就把答案污染了。所以取样—跑 GET—取样，之间不插别的远端调用。
    CB=$(cache_gb)
    read -r S0 W0 R0 <<<"$(dstat)"
    [ -n "${S0:-}" ] || { warn "$CELL 取不到 GET 前的 diskstats，本格作废"; stop_all; continue; }

    say "  GET $((GET_OPS*GET_TESTS)) 次，并发 $GET_CLIENTS"
    SSH_TIMEOUT=86400 r "$CLIENT_HOST" \
        "source ~/env.sh; /tmp/mt3-zipf_read -cnums $GET_CLIENTS -dnums $GET_OPS -tests $GET_TESTS -rest 0 -keyspace $N -servers $ALL" \
        > "$HOME/work/diskio-$LABEL-$CELL-get.out" 2>&1

    read -r S1 W1 R1 <<<"$(dstat)"
    CA=$(cache_gb)
    [ -n "${S1:-}" ] || { warn "$CELL 取不到 GET 后的 diskstats，本格作废"; stop_all; continue; }

    f(){ grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2; }
    T=$(grep '^\[THROUGHPUT\]' "$HOME/work/diskio-$LABEL-$CELL-get.out" | tail -1)
    L=$(grep '^\[LATENCY\]'    "$HOME/work/diskio-$LABEL-$CELL-get.out" | tail -1)
    H=$(grep '^\[HITRATE\]'    "$HOME/work/diskio-$LABEL-$CELL-get.out" | tail -1)
    HR=$(f "$H" ratio); [ -n "$HR" ] || HR=NA
    # pid 文件叫 `pid`，不是 `node.pid`——写错名字只会让 RSS 恒为 NA，不报错。
    RSS=$(rq "$LEADER" "ps -o rss= -p \$(cat ~/work/three-0/pid 2>/dev/null) 2>/dev/null" | head -1)
    case "$RSS" in ''|*[!0-9]*) RSS=NA ;; *) RSS=$((RSS/1024)) ;; esac

    RB=$(( (S1 - S0) * 512 ))
    WB=$(( (W1 - W0) * 512 ))
    RIO=$(( R1 - R0 ))
    WANT=$(( GET_OPS * GET_TESTS * VS ))
    say "  请求的 value 共 $((WANT/1048576)) MiB"
    say "  **设备 $DEV 实际读了 $((RB/1048576)) MiB（$RIO 次读 I/O），写了 $((WB/1048576)) MiB**"
    say "  页缓存 ${CB}GB -> ${CA}GB   节点 RSS=${RSS}MB   ops/s=$(f "$T" ops_per_s) p50=$(f "$L" p50) 命中=$HR"
    if [ "$RB" -lt $((WANT / 20)) ]; then
        say "  → 设备读不到请求量的 5%：**这一轮点读基本没碰盘，是页缓存应答的**"
    else
        say "  → 设备读达到请求量的 $((RB * 100 / WANT))%：**真在打盘**"
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$COMMIT" "$LABEL" "$BLOCK_CACHE_MB" "$SY" "$VS" "$N" "$TOTAL_MB" "$WANT" \
        "$(f "$T" ops_per_s)" "$(f "$L" p50)" "$HR" "$DEV" "$RB" "$RIO" "$WB" "$CB" "$CA" "$RSS" >> "$OUT"

    for i in 0 1 2; do
        r "$(host_of "$i")" "cat ~/work/three-$i/n.log" > "$HOME/work/diskio-$LABEL-$CELL-node$i.log" 2>/dev/null
    done
    stop_all
    sleep 5
done
done

say "===== 汇总 ====="
column -s, -t < "$OUT" | tee -a "$LOG"
echo "########## 全部结束 ##########" | tee -a "$LOG"
