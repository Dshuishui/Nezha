#!/bin/bash
# 只测写入：多系统 × 多档，三节点，提交条件可切换。
#
# 用法：
#   COMMIT_QUORUM=leader bash scripts/multinode/putonly.sh <LABEL>
#   VSIZES="64 256" SYSTEMS="nezha nezha-avp" COMMIT_QUORUM=leader bash .../putonly.sh t1
#
# 与 readops.sh 分工：这个只测 PUT，那个只测 GET / SCAN（读之前还做 GC 排空）。
# 两份 CSV 用 scripts/bench/merge-async-threeops.py 合成三操作的表，它会逐行带上 quorum，
# 两半口径不一致直接拒绝。maintable3.sh 仍可一次跑完三个操作（同样认 COMMIT_QUORUM）。
#
# 原先这个脚本只放在 tikv240 的 ~/work/run-PUTONLY.sh，不在仓库里，2026-09-30 收进来。
#
# COMMIT_QUORUM=leader 时这是一个**持久性档位**，不是优化：
#   - leader 会确认一条没有任何 follower 拿到的写，故障切换会丢已确认的数据
#     （实测 72.60%，多数派 0.0000%）
#   - 后台复制不会停，leader 接收快于排空时 follower 持续掉队、改由快照补齐
#     ——**大量快照同步正是这个档位要承受的代价**，量出来的就是它的真实表现
# 默认 majority：一个会丢数据的档位不该靠默认值生效，必须在调用处写明。
set -u
cd ~/work/Nezha || exit 1
source ~/env.sh 2>/dev/null || true
# **TOPO 要在 source gate.sh 之前设**：gate.sh 在被 source 的那一刻就读它，
# 并据此定下三台主机与端口。漏了会静默退回默认的 two（端口 3099/3100），
# 于是整轮跑在两节点拓扑上而日志里没有任何迹象——2026-09-21 实测踩过。
TOPO=${TOPO:-three}
. scripts/multinode/gate.sh
. scripts/lib/bench-common.sh
require_driver_host || exit 1

LABEL=${1:-putonly}
TOTAL_MB=${TOTAL_MB:-10240}
VSIZES=${VSIZES:-"64 256 1024 4096 16384 262144"}
SYSTEMS=${SYSTEMS:-"baseline nezha-nogc nezha nezha-avp"}
CLIENTS=${PUT_CLIENTS:-100}
GCGB=${GCGB:-0.3}
PARTITION_MB=${PARTITION_MB:-128}
SYNC_WAL=${SYNC_WAL:-0}
COMMIT_QUORUM=${COMMIT_QUORUM:-majority}
case "$COMMIT_QUORUM" in
    majority|leader) ;;
    *) echo "COMMIT_QUORUM 只认 majority | leader，收到 '$COMMIT_QUORUM'"; exit 1 ;;
esac
# set -u 下漏了这个变量脚本会在第一格的写入那一步直接死，而且因为那句话在 $( ) 里，
# set -u 只打死子 shell、父进程拿到空串继续跑——静默。实测踩过两次。
CLIENT_HOST=${CLIENT_HOST:-tikv240}
OUT=$HOME/work/putonly-$LABEL.csv
LOG=$HOME/work/putonly-$LABEL.log
: > "$LOG"
echo "commit,label,quorum,system,vsize,entries,total_mb,clients,ops,elapsed_s,ops_per_s,mb_per_s,mean_ms,p50_ms,p99_ms,max_ms,gc_rounds" > "$OUT"
COMMIT=$(git rev-parse --short HEAD)
ALL=$(servers_str)
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
warn(){ echo "[$(date +%H:%M:%S)] ** $* **" | tee -a "$LOG"; }

# has_gc —— 这个系统会不会跑 GC。baseline 与 nezha-nogc 的完成轮数恒为 0 是设计，
# 不是失败。写成函数而不是就地 case，自审第三十五节查的就是这个调用点。
has_gc() { case "$1" in nezha|nezha-avp|nezha-avp-unl) return 0 ;; *) return 1 ;; esac; }

# **退出时一定停掉三个节点。** 上一版没有这个：脚本死在第一格的写入上，
# 节点留着不动，下一段起来时端口全被占（address already in use），四格连锁失败。
stop_all(){ local i; for i in 0 1 2; do rq "$(host_of "$i")" "~/three-node.sh stop $i" >/dev/null 2>&1; done; }
trap stop_all EXIT INT TERM

# nodes_alive —— 三个节点的进程都还在吗。**异步档下少一个节点是静默的**：
# 写入不等 follower，于是灌数据照样"成功"，数字却是两节点跑出来的
# （2026-09-23 node55 被关、驱动一路照跑）。与 readops.sh 同一个实现：
# pid 文件 + /proc/<pid>/cmdline，不用 pgrep -f（远端 bash -c 的命令行会自匹配）。
nodes_alive(){
    local i dead=""
    for i in 0 1 2; do
        rq "$(host_of "$i")" "p=\$(cat ~/work/three-$i/pid 2>/dev/null) && kill -0 \$p 2>/dev/null && grep -qa 'work/three-$i' /proc/\$p/cmdline && echo ALIVE" \
            | grep -q '^ALIVE' || dead="$dead node$i"
    done
    [ -z "$dead" ] && return 0
    warn "$1：节点不在了 ——${dead}"
    return 1
}

f(){ grep -o "$2=[0-9.]*" <<<"$1" | head -1 | cut -d= -f2; }
na_row(){ # 这一格没有可信的数：记 NA，不是 0
    printf '%s,%s,%s,%s,%s,%s,%s,%s,NA,NA,NA,NA,NA,NA,NA,NA,%s\n' \
        "$COMMIT" "$LABEL" "$COMMIT_QUORUM" "$SY" "$VS" "$N" "$TOTAL_MB" "$CLIENTS" "$1" >> "$OUT"
}

say "提交=$COMMIT  拓扑=three  commitQuorum=$COMMIT_QUORUM  并发=$CLIENTS  每档 ${TOTAL_MB}MB"
say "系统=[$SYSTEMS]  档位=[$VSIZES]"

for SY in $SYSTEMS; do
  case $SY in
    baseline)      NS=original;   EX="" ;;
    nezha-nogc)    NS=nezha-nogc; EX="" ;;
    nezha)         NS=nezha;      EX="-partitionTargetMB $PARTITION_MB" ;;
    nezha-avp)     NS=nezha;      EX="-inlinePlacement -partitionTargetMB $PARTITION_MB" ;;
    # 显式不设内联额度的 AVP。节点默认已改回 0，现在它与 nezha-avp 相同；留着是为了
        # 复现 results/avp-inline-budget/ 那次对照（那时 nezha-avp 带 48MB 额度）。
    nezha-avp-unl) NS=nezha;      EX="-inlinePlacement -inlineBudgetMB 0 -partitionTargetMB $PARTITION_MB" ;;
    *) warn "未知系统 ${SY}，跳过"; continue ;;
  esac
  EX="$EX -commitQuorum $COMMIT_QUORUM"
  for VS in $VSIZES; do
    REC=$(record_bytes "$VS")
    N=$(awk -v mb="$TOTAL_MB" -v r="$REC" 'BEGIN{printf "%d", mb*1048576/r}')
    say "===== $SY  ${VS}B  ${N} 条 ====="
    stop_all   # 每格自带一次收尾，与上一格的结局无关
    ok=1
    for i in 0 1 2; do
      read -r P IP <<<"$(port_of "$i")"
      o=$(rq "$(host_of "$i")" "BIN=normal SYSTEM=$NS SYNC_WAL=$SYNC_WAL GCGB=$GCGB PEERS='$(peers_str)' EXTRA='$EX' ~/three-node.sh start $i $P $IP $VS $N" | tail -1)
      case "$o" in STARTED*) ;; *) warn "node$i 启动失败: ${o:-（无回执）}"; ok=0 ;; esac
    done
    [ "$ok" = 1 ] || { stop_all; na_row 0; continue; }
    sleep 12
    say "  写入中"
    SSH_TIMEOUT=86400 r "$CLIENT_HOST" \
      "source ~/env.sh; /tmp/mt3-randwrite_goroutine -cnums $CLIENTS -dnums $N -vsize $VS -servers $ALL" \
      > "$HOME/work/putonly-$LABEL-$SY-$VS.out" 2>&1
    T=$(grep '^\[THROUGHPUT\]' "$HOME/work/putonly-$LABEL-$SY-$VS.out" | tail -1)
    L=$(grep '^\[LATENCY\]'    "$HOME/work/putonly-$LABEL-$SY-$VS.out" | tail -1)
    say "  $T"
    G=0
    if has_gc "$SY"; then
      for i in 0 1 2; do
        g=$(rq "$(host_of "$i")" "grep -c '轮垃圾回收完成' ~/work/three-$i/n.log 2>/dev/null" | head -1)
        # grep -c 无匹配时既打印 0 又返回 1，所以判的是内容不是退出码
        case "$g" in ''|*[!0-9]*) g=0;; esac
        [ "$g" -gt "$G" ] && G=$g
      done
    fi
    if ! nodes_alive "$SY ${VS}B 写完之后"; then
      warn "这一格作废：数字会是少节点跑出来的"
      na_row "$G"
    elif [ -z "$T" ]; then
      warn "没有吞吐输出——这一格记 NA，不要当成 0"
      tail -12 "$HOME/work/putonly-$LABEL-$SY-$VS.out" | tee -a "$LOG"
      na_row "$G"
    else
      printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$COMMIT" "$LABEL" "$COMMIT_QUORUM" "$SY" "$VS" "$N" "$TOTAL_MB" "$CLIENTS" \
        "$(f "$T" ops)" "$(f "$T" elapsed)" "$(f "$T" ops_per_s)" "$(f "$T" mb_per_s)" \
        "$(f "$L" mean)" "$(f "$L" p50)" "$(f "$L" p99)" "$(f "$L" max)" "$G" >> "$OUT"
    fi
    say "  GC 轮数=$G  快照=$(rq "$(host_of 0)" "grep -c '开始给它发快照' ~/work/three-0/n.log 2>/dev/null" | head -1)"
    for i in 0 1 2; do
      r "$(host_of "$i")" "cat ~/work/three-$i/n.log" > "$HOME/work/putonly-$LABEL-$SY-$VS-node$i.log" 2>/dev/null
    done
    stop_all
    sleep 5
  done
done
say "===== 汇总 ====="
column -s, -t < "$OUT" | tee -a "$LOG"
echo "########## 全部结束 ##########" | tee -a "$LOG"
