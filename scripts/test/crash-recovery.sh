#!/bin/bash
# 单节点崩溃恢复验证：GC 改造之后，重启还能不能把数据完整读回来。
#
# 为什么要单独测。P1 把 GC 的产物从一个有序文件换成 N 个按 key 区间的分区，磁盘格式与恢复
# 路径都变了：分区清单进了 kv_state.json、重启按清单重建、字节数与清单不符就拒绝启动、
# 崩溃重做前要清掉上次写了一半的 .pN。这些逻辑只有崩溃一次才走得到，性能实验一次都碰不着。
#
# 而且这类问题**不会报错**：读路径把"这一处没有"当作常态，恢复少还原一个分区只会让某些 key
# 在某次 GET 上变成 NOKEY。所以每个场景都逐条校验值，并直接数盘上的 key。
#
# 三个场景：
#   A 干净重启      GC 完整跑完 → kill -9 → 重启按清单重建分区
#   B GC 中途崩溃   切换已完成、搬运未开始时 kill -9（NEZHA_GC_PAUSE_MS 窗口）→ 重启重做搬运
#   C 清单校验      故意截断一个分区文件 → 重启**必须拒绝**，不能按错误的边界静默读
#   D 写分区途中崩   第 2 轮吸收正在写分区时 kill -9 → 盘上留下**半个**分区组 → 重启必须
#                    认出它是半成品、清掉重做。B 停在"搬运尚未开始"，盘上什么也没有，
#                    恰好绕开了这个窗口——两个真 bug 就是从这里漏过去的：恢复删的分区名
#                    对不上（删不掉），而吸收看见自己的 p0 就当成已完成直接返回，于是
#                    anotherPartitions 为 nil、恢复 log.Fatalf，**每次重启都一样，节点
#                    再也起不来**。
#
# 规模刻意取小：跑通一次 GC 就够，堆数据量只是浪费时间。
#
# 用法: bash scripts/test/crash-recovery.sh [场景...]   默认全跑
# 环境变量: ENTRIES=30000 VSIZE=256 PARTITION_MB=2 DATA=<目录，各场景在其下各占一个子目录>
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[TEST]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
fail(){ echo -e "${RED}[FAIL]${NC} $*"; FAILED=$((FAILED+1)); }
ok(){   echo -e "${GREEN}[ OK ]${NC} $*"; }

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "${REPO_DIR:-$SCRIPT_DIR/../..}" || { echo "无项目目录"; exit 1; }
source ~/env.sh 2>/dev/null || true
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

ENTRIES="${ENTRIES:-30000}"
VSIZE="${VSIZE:-256}"
PARTITION_MB="${PARTITION_MB:-2}"
DATA_BASE="${DATA:-$TMPDIR/crash-recovery}"
DATA="$DATA_BASE"
ADDR=127.0.0.1:3097
IADDR=127.0.0.1:30971
BIN=/tmp/nezha-crash
FAILED=0
PID=""

# GC 阈值取写入总量的三分之一，确保跑得起来又不会一直跑
BYTES=$(( ENTRIES * (20 + 10 + VSIZE) ))
GCGB=$(awk -v b="$BYTES" 'BEGIN{printf "%.9f", b/3/1073741824}')

info "构建 $(git rev-parse --short HEAD)"
go build -o "$BIN" ./cmd/nezha/ || { echo "节点编译失败"; exit 1; }
# 必须用 scanverify 写：它把 value 从 key 派生，readonly 才能逐条独立校验。
# randwrite_goroutine 写的是通用填充串，用它写、用 readonly 读，会得到"每一条值都错"
# 的假失败——那是工具不配套，不是系统出错。
go build -o /tmp/crash-scanverify ./cmd/bench/scanverify/ || exit 1
go build -o /tmp/crash-readonly ./cmd/bench/readonly/ || exit 1

cleanup(){ [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null; }
trap cleanup EXIT

# start_node [额外的环境变量赋值...]；成功返回后 PID 已就绪。
# 每次启动写**独立**的日志文件：追加到同一个文件的话，上一次启动留下的 [SYSTEM] 行会让
# "启动成功"的判据立刻为真，节点死了也照样判成功。
NLOG=""
start_node(){
  NSEQ=$((${NSEQ:-0} + 1))
  NLOG="$DATA/n$NSEQ.log"
  # shellcheck disable=SC2086
  env "$@" nohup "$BIN" -address "$ADDR" -internalAddress "$IADDR" -peers "$IADDR" \
      -data "$DATA" -gap 100000000 -commitTimeoutS 60 \
      -system nezha -gcThresholdGB "$GCGB" -partitionTargetMB "$PARTITION_MB" \
      < /dev/null > "$NLOG" 2>&1 &
  PID=$!
  for _ in $(seq 1 30); do
    sleep 1
    kill -0 "$PID" 2>/dev/null || return 1
    grep -q '\[SYSTEM\]' "$NLOG" && break
  done
  # 还要等它**当选**才算能服务读。[SYSTEM] 是启动时就打的，那之后还有一个选举超时
  # （minElectionTimeout 3 秒）才会有 leader；而验证工具 readonly 走的是 GetFrom，
  # 故意不跟随重定向，所以打在 follower 上会被 ErrWrongLeader 全部挡回——实测
  # 300 个 GET 全部"取不到"、SCAN 30 次全部范围失败，看起来像丢数据，其实是还没选出 leader。
  for _ in $(seq 1 30); do
    kill -0 "$PID" 2>/dev/null || return 1
    grep -q -- '-> Leader' "$NLOG" && return 0
    sleep 1
  done
  warn "节点起来了但 30 秒内没有当选，读会被 leaderCheck 挡回"
  return 0
}

# kill_node 之后必须**等端口真的放开**再启动下一个节点。只 sleep 1 秒是不够的：
# 各场景共用 127.0.0.1:3097，端口没释放时新节点绑不上就退出，表现成"重启失败"——
# 一个与被测逻辑毫无关系的假失败（2026-09-09 全套跑里场景 A 就这样挂过一次，
# 单独重跑即通过）。
kill_node(){
  [ -n "$PID" ] && kill -9 "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  PID=""
  local port="${ADDR##*:}"
  for _ in $(seq 1 60); do
    ss -ltn 2>/dev/null | grep -q ":$port " || return 0
    sleep 0.5
  done
  warn "端口 $port 30 秒后仍被占用"
}

# fresh_data <场景名>：给这个场景一个**独立**的数据目录。
# 之前全部场景共用一个目录、各自开头 rm -rf，于是前一个场景失败时留下的日志会被
# 后一个场景删掉——今天就因此丢过一次失败现场，只能重跑才拿到原因。
fresh_data(){
  DATA="$DATA_BASE/$1"
  rm -rf "$DATA"; mkdir -p "$DATA"
  NSEQ=0
}

# wait_gc_done <轮数>：等日志里出现至少这么多轮完成
wait_gc_done(){
  for _ in $(seq 1 60); do
    [ "$(cat "$DATA"/n*.log 2>/dev/null | grep -c '轮垃圾回收完成')" -ge "$1" ] && return 0
    sleep 2
  done
  return 1
}

write_data(){
  /tmp/crash-scanverify -servers "$ADDR" -leader 0 -dnums "$ENTRIES" -vsize "$VSIZE" \
      -span 50 -sample 20 > "$DATA/put.out" 2>&1
  grep -q VERIFY_OK "$DATA/put.out"
}

verify(){ # verify <场景名>
  local out; out=$(/tmp/crash-readonly -servers "$ADDR" -dnums "$ENTRIES" -vsize "$VSIZE" \
      -span 50 -sample 30 -check 300 2>&1 | grep -v 'new pool success')
  echo "$out" | grep -E '校验|VERIFY' | sed 's/^/       /'
  echo "$out" | grep -q FAILOVER_VERIFY_OK || { fail "$1: 逐条校验未通过"; return 1; }
  local lost; lost=$(python3 "$SCRIPT_DIR/../bench/lost-keys.py" "$DATA" "$ENTRIES" "$VSIZE" 2>/dev/null \
      | grep -o '丢失 [0-9]*' | grep -o '[0-9]*')
  [ "${lost:-NA}" = 0 ] || { fail "$1: 盘上丢了 ${lost:-?} 条记录"; return 1; }
  ok "$1: 逐条校验通过，盘上丢失 0"
}

# ---------- 场景 A：GC 完整跑完之后重启 ----------
scenario_a(){
  info "A 干净重启：GC 跑完 → kill -9 → 重启按清单重建"
  fresh_data a
  start_node || { fail "A: 节点未启动"; return; }
  write_data || { fail "A: 写入或即时校验未通过"; return; }
  wait_gc_done 1 || { fail "A: GC 未触发（阈值 ${GCGB}GB）"; return; }
  local parts; parts=$(ls "$DATA"/data/valuelog/*.p* 2>/dev/null | wc -l)
  [ "$parts" -ge 2 ] || warn "A: 只产出 $parts 个分区，路由代码没被充分执行"
  info "  产出 $parts 个分区文件，kill -9"
  kill_node
  start_node || { fail "A: 重启失败，见 $NLOG"; tail -3 "$NLOG" | sed 's/^/       /'; return; }
  grep -q 'partition set rebuilt' "$NLOG" || fail "A: 日志里没有按清单重建分区的记录"
  verify A
  kill_node
}

# ---------- 场景 B：GC 切换之后、搬运之前崩溃 ----------
scenario_b(){
  info "B GC 中途崩溃：切换已完成、搬运未开始时 kill -9 → 重启重做搬运"
  fresh_data b
  # NEZHA_GC_PAUSE_MS 让 GC 在"切换已持久化、搬运尚未开始"处停住，正是最难恢复的那一刻
  start_node NEZHA_GC_PAUSE_MS=20000 || { fail "B: 节点未启动"; return; }
  write_data || { fail "B: 写入或即时校验未通过"; return; }
  local hit=0
  for _ in $(seq 1 60); do
    grep -q '\[GC-PAUSE\]' "$NLOG" && { hit=1; break; }
    sleep 1
  done
  [ "$hit" = 1 ] || { fail "B: 没有进入 GC 暂停窗口，无法制造中途崩溃"; kill_node; return; }
  info "  已进入 GC 暂停窗口，kill -9"
  kill_node
  start_node || { fail "B: 重启失败，见 $NLOG"; tail -3 "$NLOG" | sed 's/^/       /'; return; }
  wait_gc_done 1 || { fail "B: 重启后没有重做搬运"; kill_node; return; }
  verify B
  kill_node
}

# ---------- 场景 C：分区文件被截断，重启必须拒绝 ----------
scenario_c(){
  info "C 清单校验：截断一个分区文件 → 重启必须拒绝启动"
  fresh_data c
  start_node || { fail "C: 节点未启动"; return; }
  write_data || { fail "C: 写入或即时校验未通过"; return; }
  wait_gc_done 1 || { fail "C: GC 未触发"; return; }
  kill_node
  local victim; victim=$(ls "$DATA"/data/valuelog/*.p0 2>/dev/null | head -1)
  [ -n "$victim" ] || { fail "C: 找不到分区文件"; return; }
  local sz; sz=$(stat -c %s "$victim")
  truncate -s $((sz - 1)) "$victim"
  info "  截断 $(basename "$victim") 一个字节，尝试重启"
  # 拒绝的方式是 log.Fatalf，所以进程应当立刻退出
  start_node
  sleep 3
  if kill -0 "$PID" 2>/dev/null; then
    fail "C: 分区被截断后节点照常启动了——会按错误的边界静默判定 key 不存在"
    kill_node
  else
    grep -q 'manifest says' "$NLOG" \
      && ok "C: 重启被拒绝，且报出了清单与实际长度不符" \
      || fail "C: 节点退出了，但日志里没有清单校验失败的原因"
    PID=""
  fi
}

# ---------- 场景 D：第 2 轮吸收正在写分区时崩溃 ----------
#
# 为什么要先干净地跑完第 1 轮再挂钩子：第 1 轮和第 2 轮走的是两条不同的重做路径
# （finishFirstGC 对 absorbTail），出 bug 的是后者。钩子从第二次启动才生效，
# 那时下一轮必然是第 2 轮，窗口就落在吸收里。
scenario_d(){
  info "D 写分区途中崩溃：第 2 轮吸收写到一半 kill -9 → 重启必须清掉半成品并重做"
  fresh_data d
  start_node || { fail "D: 节点未启动"; return; }
  write_data || { fail "D: 写入或即时校验未通过"; return; }
  wait_gc_done 1 || { fail "D: 第 1 轮 GC 未触发（阈值 ${GCGB}GB）"; return; }
  kill_node

  # 钩子在每个分区封口后停住，此刻本轮产物在盘上但不完整、也没提交
  start_node NEZHA_GC_WRITE_PAUSE_MS=15000 || { fail "D: 带钩子重启失败"; return; }
  write_data || { fail "D: 第二遍写入未通过"; return; }
  local hit=0
  for _ in $(seq 1 90); do
    grep -q 'partition written; more to go' "$NLOG" && { hit=1; break; }
    sleep 1
  done
  [ "$hit" = 1 ] || { fail "D: 没能停在写分区的窗口里，无法制造半成品"; kill_node; return; }

  local partial; partial=$(ls "$DATA"/data/valuelog/RaftState_sorted_2.p* 2>/dev/null | wc -l | tr -d ' ')
  [ "${partial:-0}" -ge 1 ] || { fail "D: 窗口里盘上没有第 2 轮的分区文件，场景没成立"; kill_node; return; }
  info "  第 2 轮已写出 $partial 个分区（未提交），kill -9"
  kill_node

  # 这一步就是回归判据本身：修之前这里必然 log.Fatalf，而且每次重启都一样
  start_node || { fail "D: 重启失败——半成品把节点卡死了"; tail -5 "$NLOG" | sed 's/^/       /'; return; }
  sleep 2
  kill -0 "$PID" 2>/dev/null || { fail "D: 节点启动后随即退出"; tail -5 "$NLOG" | sed 's/^/       /'; PID=""; return; }
  wait_gc_done 2 || { fail "D: 重启后没有重做第 2 轮吸收"; kill_node; return; }
  verify D
  kill_node
}

WANT="${*:-a b c d}"
for sc in $WANT; do
  case "$sc" in
    a|A) scenario_a ;;
    b|B) scenario_b ;;
    c|C) scenario_c ;;
    d|D) scenario_d ;;
    *) echo "未知场景 $sc（可选 a b c d）"; exit 1 ;;
  esac
done

echo
if [ "$FAILED" = 0 ]; then ok "全部通过"; else fail "共 $FAILED 项失败"; exit 1; fi
