#!/bin/bash
# follower 冲突截断的端到端验证：构造一段**未提交的尾巴**，再让新 leader 覆盖它。
#
# 为什么要单独写。这条路此前只有单元测试覆盖（truncateLogFrom 的字节计量、
# AppendEntriesInRaft 的分叉处理），而集群脚本一个都测不到它：它们全都是
# "写完 → 等提交 → 再杀 leader"，从不留下未提交的条目，于是新 leader 的日志
# 与旧 leader 的日志永远不冲突。
#
# 这里构造的正是那个形态：
#   1. 三节点起来，写一批数据并确认提交（基准）
#   2. SIGSTOP 掉两个 follower —— leader 再也凑不出多数派
#   3. 继续往 leader 写 —— 这些条目进了它的内存日志和 valuelog，但**永远提交不了**
#   4. kill -9 掉 leader
#   5. SIGCONT 两个 follower，它们两个就是多数派，选出新 leader
#   6. 通过新 leader 写**不同的**数据 —— 占用的正是旧 leader 那段未提交尾巴的下标，
#      但 term 更高
#   7. 把旧 leader 拉起来 —— 它必须能恢复、必须截掉自己的尾巴、必须与集群收敛
#
# 判据有四条，缺一不可：
#   a) 旧节点**起得来**。它盘上的日志尾部是被覆盖过的那一段，恢复要顺序回放整个文件；
#      覆盖没截干净的话这里报 "log not contiguous" 直接起不来。
#   b) 日志里出现 [LOG-CONFLICT] —— 截断这条路真的走到了。没有它，后面三条都没意义。
#   c) 集群逐条校验 0 丢失（基准那批 + 新 leader 那批）。
#   d) 直接读旧节点（-leaderCheck=false 绕过重定向），拿到的必须是**新** leader 的值，
#      不是它自己那段未提交的。
#
# 用法: bash scripts/test/conflict-truncation.sh
# 环境变量: BASE=20000 TAIL=64 VSIZE=256 NEW_VSIZE=300 DATA=<目录>
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[TEST]${NC} $*"; }
warn(){ echo -e "${YEL}[WARN]${NC} $*"; }
fail(){ echo -e "${RED}[FAIL]${NC} $*"; FAILED=$((FAILED+1)); }
ok(){   echo -e "${GREEN}[ OK ]${NC} $*"; }

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PROJECT_DIR="${REPO_DIR:-$SCRIPT_DIR/../..}"
cd "$PROJECT_DIR" || { echo "无项目目录"; exit 1; }
# shellcheck source=scripts/lib/cgo-env.sh
. "$PROJECT_DIR/scripts/lib/cgo-env.sh"
setup_cgo_env || { echo "cgo 环境准备失败"; exit 1; }
export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"

# 规模必须让**新 leader 的日志总长**留在压缩阈值（internal/raft: logThreshold=20000 条）以内。
# 否则这个场景根本构造不出来：leader 一压缩，分叉点就落在它保留的窗口之前，日志路径被
# 整体绕开（走 start<0 那支），冲突截断一次都不会发生。
# 2026-09-17 实测：BASE=20000（leader 日志 40002 条）连跑三轮，node0 既没被拒
# （[LOG-REJECT] 0 条）也没进重叠区（[LOG-OVERLAP] 0 条）却收敛了，而 node2 压缩了两次
# （lastIncludedIndex 15001 → 35066）；BASE=2000 一次就过，[LOG-OVERLAP] 紧跟 [LOG-CONFLICT]。
# 所以默认值取 2000，而不是"看起来更像真实负载"的 20000——这个用例要验的是那条分支，
# 不是吞吐。
BASE_N="${BASE:-2000}"       # 基准那批（会提交）
TAIL_N="${TAIL:-64}"         # 未提交的尾巴：并发在途请求数，每个贡献一条
VSIZE="${VSIZE:-256}"        # 基准那批的 value 大小
NEW_VSIZE="${NEW_VSIZE:-300}" # 新 leader 那批：换个大小，于是同一个 key 的新旧值可区分
DATA_BASE="${DATA:-$TMPDIR/conflict-truncation}"
BIN=/tmp/nezha-conflict
FAILED=0

# 三个节点，端口互不相同。3101~3103 这一段没有别的脚本在用。
PORTS=(3101 3102 3103)
IPORTS=(31011 31012 31013)
PEERS="127.0.0.1:31011,127.0.0.1:31012,127.0.0.1:31013"
PIDS=("" "" "")

# 判据用到的这些函数必须真的解析得到：trap 里调用一个不存在的函数会静默什么都不做，
# 于是节点被留在机器上。所以先自检一遍。
for f in info fail ok setup_cgo_env; do
  [ "$(type -t "$f")" = "function" ] || { echo "函数 $f 没解析到"; exit 1; }
done

cleanup_all(){
  local i
  for i in 0 1 2; do
    [ -n "${PIDS[$i]}" ] && kill -9 "${PIDS[$i]}" 2>/dev/null
  done
  # 兜底：trap 收不到 SIGKILL，也可能有上一轮的残留。不带 -f，否则 pgrep 会匹配到自己。
  pkill -9 -x nezha-conflict 2>/dev/null
  return 0
}
trap cleanup_all EXIT

info "构建 $(git rev-parse --short HEAD)"
go build -o "$BIN" ./cmd/nezha/ || { echo "节点编译失败"; exit 1; }
# 必须用 scanverify 写：它把 value 从 key 派生，readonly 才能逐条独立校验。
go build -o /tmp/conflict-scanverify ./cmd/bench/scanverify/ || exit 1
go build -o /tmp/conflict-readonly ./cmd/bench/readonly/ || exit 1
# 未提交的尾巴必须用 randwrite_goroutine 写，不能用 scanverify：后者的 Put 一失败
# 就 return，而这一步每一个 Put 都注定失败（多数派不在），于是只会留下一条尾巴、
# 还要白等一个 PutTimeout。randwrite_goroutine 是并发的、失败也继续
# （"失败的请求也要记：它们正是尾部"），-cnums 个在途请求就是 -cnums 条未提交条目。
go build -o /tmp/conflict-randwrite ./cmd/bench/randwrite_goroutine/ || exit 1

# 预清理：上一轮如果被 SIGKILL 掉，trap 不会执行，端口和进程都还在。
cleanup_all
rm -rf "$DATA_BASE"; mkdir -p "$DATA_BASE"

# start_node <编号>：拉起一个节点。-leaderCheck=false 是为了判据 d 能直接读它的本地状态。
start_node(){
  local i=$1
  local d="$DATA_BASE/n$i"
  mkdir -p "$d"
  local seq="${NSEQ[$i]:-0}"
  seq=$((seq + 1)); NSEQ[$i]=$seq
  LOGS[$i]="$d/run$seq.log"
  nohup "$BIN" -address "127.0.0.1:${PORTS[$i]}" \
      -internalAddress "127.0.0.1:${IPORTS[$i]}" \
      -peers "$PEERS" -data "$d" -gap 100000000 -commitTimeoutS 8 \
      -leaderCheck=false -system nezha-nogc \
      < /dev/null > "${LOGS[$i]}" 2>&1 &
  PIDS[$i]=$!
  local _
  for _ in $(seq 1 30); do
    sleep 1
    kill -0 "${PIDS[$i]}" 2>/dev/null || return 1
    grep -q '\[SYSTEM\]' "${LOGS[$i]}" && return 0
  done
  return 1
}
NSEQ=(0 0 0)
LOGS=("" "" "")

# won_count <编号>：这个节点当选过几次。用它的**增加**来判断有没有新 leader，
# 不要 sleep 一个固定秒数：选举超时 10 秒 + 最多 1 秒抖动，实测过 18 秒。
# grep -c 在 0 匹配时**既打印 0 又返回 1**。写成 `grep -c ... || echo 0` 会输出两行 0，
# 后面的 -gt 报 "integer expression expected"，而那看起来像判据本身出错。
won_count(){
  local n
  n=$(grep -c -- '-> Leader' "${LOGS[$1]}" 2>/dev/null) || n=0
  echo "${n:-0}"
}

# find_leader：轮询到某个节点当选，回显它的编号。
find_leader(){
  local _ i
  for _ in $(seq 1 60); do
    for i in 0 1 2; do
      [ -n "${PIDS[$i]}" ] || continue
      kill -0 "${PIDS[$i]}" 2>/dev/null || continue
      if [ "$(won_count "$i")" -gt 0 ] && grep -q -- '-> Leader' "${LOGS[$i]}"; then
        echo "$i"; return 0
      fi
    done
    sleep 1
  done
  return 1
}

# servers_str：所有节点的客户端地址，供 bench 工具做重定向。
servers_str(){ echo "127.0.0.1:${PORTS[0]},127.0.0.1:${PORTS[1]},127.0.0.1:${PORTS[2]}"; }

# vlog_bytes <编号>：这个节点 valuelog 的总字节数。用它证明"未提交的尾巴真的写进去了"。
vlog_bytes(){
  local d="$DATA_BASE/n$1/data/valuelog"
  [ -d "$d" ] || { echo 0; return 0; }
  local n
  n=$(find "$d" -type f -name 'RaftState*' -printf '%s\n' 2>/dev/null | awk '{t+=$1} END{print t+0}')
  echo "${n:-0}"
}

# 前提自检：新 leader 会写 BASE_N 条覆盖 + 一条 TermLog，加上基准那批，
# 总长必须小于压缩阈值。超了就直接说清楚"这一轮验不到那条分支"，不要等到判据 b
# 才以"没构造出冲突"的形式暴露——那看起来像被测系统的问题。
LOG_THRESHOLD=$(grep -oE 'logThreshold[[:space:]]*=[[:space:]]*[0-9]+' \
                "$PROJECT_DIR/internal/raft/compact.go" | grep -oE '[0-9]+' | head -1)
LOG_THRESHOLD=${LOG_THRESHOLD:-20000}
PROJECTED=$((BASE_N * 2 + TAIL_N + 2))
info "压缩阈值 ${LOG_THRESHOLD} 条；本轮新 leader 日志预计 ${PROJECTED} 条"
if [ "$PROJECTED" -ge "$LOG_THRESHOLD" ]; then
  fail "预计日志 ${PROJECTED} 条 >= 压缩阈值 ${LOG_THRESHOLD}：leader 会压缩过分叉点，"
  echo "      日志路径被整体绕开，冲突截断这条分支验不到。把 BASE 调小到 $(( (LOG_THRESHOLD - TAIL_N - 2) / 2 - 100 )) 以下。"
  exit 1
fi

# ---------- 1. 三节点起来 ----------
info "拉起三节点"
for i in 0 1 2; do
  start_node "$i" || { fail "node$i 起不来"; exit 1; }
done
LEADER=$(find_leader) || { fail "60 秒内没有选出 leader"; exit 1; }
ok "leader = node$LEADER"

# ---------- 2. 基准那批，必须提交 ----------
# scanverify 写入 key 0..dnums-1，value 由 key **和 vsize** 共同派生。
# 这一步与第 6 步取**不同的** vsize，于是同一个 key 的"旧值"和"新值"可区分——
# 不需要给工具加 -kstart 这类不存在的参数。
info "写基准 $BASE_N 条（value ${VSIZE}B）"
if ! /tmp/conflict-scanverify -servers "$(servers_str)" \
    -dnums "$BASE_N" -vsize "$VSIZE" > "$DATA_BASE/write-base.log" 2>&1; then
  fail "基准写入返回非 0"; tail -10 "$DATA_BASE/write-base.log"; exit 1
fi
if ! grep -q 'VERIFY_OK' "$DATA_BASE/write-base.log"; then
  fail "基准写入没有 VERIFY_OK——前提不成立，后面的判据都没有意义"
  tail -10 "$DATA_BASE/write-base.log"; exit 1
fi
ok "基准 $BASE_N 条写入并校验通过"

# ---------- 3. 按住两个 follower ----------
FOLLOWERS=()
for i in 0 1 2; do [ "$i" != "$LEADER" ] && FOLLOWERS+=("$i"); done
info "SIGSTOP follower node${FOLLOWERS[0]} 与 node${FOLLOWERS[1]}——leader 从此凑不出多数派"
kill -STOP "${PIDS[${FOLLOWERS[0]}]}" || { fail "STOP 失败"; exit 1; }
kill -STOP "${PIDS[${FOLLOWERS[1]}]}" || { fail "STOP 失败"; exit 1; }

# ---------- 4. 往 leader 写未提交的尾巴 ----------
# 客户端一定会超时——这是**预期**行为，不是失败：多数派不在，这些条目提交不了。
# 它们仍然进了 leader 的内存日志和 valuelog，这正是我们要的那段尾巴。
# key 从 [0,BASE_N) 里抽，与基准那批**重叠**；value 是通用填充串，与任何 vsize 的
# 派生值都不同。所以这段尾巴一旦泄漏成可见状态，第 6 步之后的校验会报"值错"。
OLD_LEADER_CAND=$LEADER
VLOG_BEFORE=$(vlog_bytes "$OLD_LEADER_CAND")
info "往 leader 打 $TAIL_N 个并发 Put（提交不了，全部会超时，这是预期的）"
timeout 180 /tmp/conflict-randwrite -servers "127.0.0.1:${PORTS[$LEADER]}" \
    -cnums "$TAIL_N" -dnums "$TAIL_N" -keyspace "$BASE_N" -dist uniform -vsize "$VSIZE" \
    > "$DATA_BASE/write-tail.log" 2>&1
TAIL_RC=$?
info "尾巴写入返回 ${TAIL_RC}（非 0 或超时都是预期的）"
# 前提检查：leader 的 valuelog 必须真的变长了。没变长就说明这一轮压根没写进未提交的
# 条目，后面即便"通过"也什么都没验证到。
VLOG_AFTER=$(vlog_bytes "$OLD_LEADER_CAND")
info "leader valuelog: $VLOG_BEFORE -> $VLOG_AFTER 字节"
if [ "$VLOG_AFTER" -le "$VLOG_BEFORE" ]; then
  fail "leader 的 valuelog 没有变长——这一轮没有未提交的尾巴，判据全部失效"
  exit 1
fi
ok "未提交的尾巴已写入（valuelog 多了 $((VLOG_AFTER - VLOG_BEFORE)) 字节）"

# ---------- 5. 杀掉 leader，放开 follower ----------
info "kill -9 leader node$LEADER"
OLD_LEADER=$LEADER
kill -9 "${PIDS[$OLD_LEADER]}" 2>/dev/null
wait "${PIDS[$OLD_LEADER]}" 2>/dev/null
PIDS[$OLD_LEADER]=""

WON_BEFORE_0=$(won_count "${FOLLOWERS[0]}")
WON_BEFORE_1=$(won_count "${FOLLOWERS[1]}")
info "SIGCONT 两个 follower（当选次数 ${WON_BEFORE_0}/${WON_BEFORE_1}）"
kill -CONT "${PIDS[${FOLLOWERS[0]}]}"
kill -CONT "${PIDS[${FOLLOWERS[1]}]}"

# 轮询"当选次数变多"，而不是睡一个固定秒数
NEW_LEADER=""
for _ in $(seq 1 60); do
  if [ "$(won_count "${FOLLOWERS[0]}")" -gt "$WON_BEFORE_0" ]; then NEW_LEADER=${FOLLOWERS[0]}; break; fi
  if [ "$(won_count "${FOLLOWERS[1]}")" -gt "$WON_BEFORE_1" ]; then NEW_LEADER=${FOLLOWERS[1]}; break; fi
  sleep 1
done
[ -n "$NEW_LEADER" ] || { fail "两个 follower 放开后 60 秒内没有选出新 leader"; exit 1; }
ok "新 leader = node$NEW_LEADER"

# ---------- 6. 通过新 leader 写不同的数据 ----------
# key 段与那段未提交的尾巴**重叠**：同样的日志下标，不同的内容与更高的 term。
# 覆盖**全部**基准 key，用不同的 vsize，于是"最终状态"只有一个说法：
# 每个 key 都该是 NEW_VSIZE 派生出的那个值。
# -servers 给全三个、用 -leader 指到新 leader：单条 -servers 的客户端没法重定向。
info "通过新 leader 重写全部 $BASE_N 条（vsize ${NEW_VSIZE}B，占用的正是那段未提交尾巴的下标）"
/tmp/conflict-scanverify -servers "$(servers_str)" -leader "$NEW_LEADER" \
    -dnums "$BASE_N" -vsize "$NEW_VSIZE" \
    > "$DATA_BASE/write-new.log" 2>&1 || warn "新 leader 写入返回非 0，摘要见 write-new.log"
if ! grep -q 'VERIFY_OK' "$DATA_BASE/write-new.log"; then
  fail "新 leader 那批没有 VERIFY_OK"; tail -10 "$DATA_BASE/write-new.log"
fi

# ---------- 7. 把旧 leader 拉起来 ----------
info "重启旧 leader node$OLD_LEADER"
start_node "$OLD_LEADER" || { fail "判据 a：旧节点起不来（日志尾部没截干净？）"; tail -20 "${LOGS[$OLD_LEADER]}"; exit 1; }
ok "判据 a：旧节点起来了"

if grep -q 'log not contiguous' "${LOGS[$OLD_LEADER]}"; then
  fail "判据 a：恢复报 log not contiguous——覆盖写没把陈旧字节截掉"
  grep 'log not contiguous' "${LOGS[$OLD_LEADER]}" | head -3
fi

# 等它认出新 leader 并截断。轮询 [LOG-CONFLICT]，不睡固定秒数。
CONFLICT=0
for _ in $(seq 1 60); do
  if grep -q '\[LOG-CONFLICT\]' "${LOGS[$OLD_LEADER]}"; then CONFLICT=1; break; fi
  kill -0 "${PIDS[$OLD_LEADER]}" 2>/dev/null || { fail "旧节点起来之后又死了"; break; }
  sleep 1
done
if [ "$CONFLICT" = 1 ]; then
  ok "判据 b：$(grep -c '\[LOG-CONFLICT\]' "${LOGS[$OLD_LEADER]}") 条 [LOG-CONFLICT]，截断这条路走到了"
  grep '\[LOG-CONFLICT\]' "${LOGS[$OLD_LEADER]}" | head -3
else
  fail "判据 b：60 秒内没有 [LOG-CONFLICT]——这一轮没构造出冲突，后面的判据都没有意义"
  # 没构造出冲突有好几种走法，光说"没看到"下次还得从头查。把能区分它们的现场一起打出来：
  #   恢复出的日志区间   —— 未提交的尾巴到底在不在盘上
  #   有没有 [SNAPSHOT]  —— 是不是被快照整体替换掉了（那条路不经过冲突分支）
  #   有没有 compactLog  —— 分叉点是不是先被压缩掉了
  #   有没有"底层执行了Put请求" —— 本节点是不是把自己那段未提交的尾巴应用了
  #     （那是 commitIndex 被推过确认前缀的症状，见 advanceCommitLocked）
  echo "  --- 现场 ---"
  grep -E 'recovery complete|\[SNAPSHOT\]|compactLog|底层执行了Put请求' "${LOGS[$OLD_LEADER]}" \
      | head -6 | sed 's/^/  /'
  echo "  新 leader 侧："
  grep -E '\[LOG-TRUNCATE\]|\[LOG-STUCK\]|\[SNAPSHOT\]' "${LOGS[$NEW_LEADER]}" \
      | head -4 | sed 's/^/  /'
fi

# ---------- 判据 c：集群逐条校验 ----------
# 判据取 readonly：它只读不写，判据是 "GET 校验: 正确 N, 错误 M" 里的 M 和
# "SCAN 校验" 里的错误数，以及末行的 FAILOVER_VERIFY_OK。
info "判据 c：通过集群逐条校验（期望值按 vsize ${NEW_VSIZE}B 派生）"
# readonly 只有 -servers，**没有 -leader**（那是 scanverify 独有的）。Go 的 flag
# 遇到未声明的参数会打 usage 然后退出，回执是空的，判据于是报"集群校验未通过"，
# 而集群是好的——2026-09-17 第一轮就这么误判了一次。
# 它走 GetFrom、本来就不做重定向，所以直接给新 leader 的那一个地址才是对的。
/tmp/conflict-readonly -servers "127.0.0.1:${PORTS[$NEW_LEADER]}" \
    -dnums "$BASE_N" -vsize "$NEW_VSIZE" -check 2000 \
    > "$DATA_BASE/verify-cluster.log" 2>&1 || true
if grep -q 'FAILOVER_VERIFY_OK' "$DATA_BASE/verify-cluster.log"; then
  ok "判据 c：集群逐条校验通过"
else
  fail "判据 c：集群校验未通过"
fi
grep -E 'GET 校验|SCAN 校验|FAILOVER_VERIFY' "$DATA_BASE/verify-cluster.log" || true

# ---------- 判据 d：直接读旧节点 ----------
# -leaderCheck=false 启动的，所以能读到它自己的本地状态；读到的必须是新 leader 的值。
info "判据 d：直接读重启后的旧节点"
# 先等它真的追上：截断只是第一步，新 leader 那批还要复制过来并 apply。
# 轮询"小样本校验干净"，不要睡一个固定秒数。
for _ in $(seq 1 90); do
  /tmp/conflict-readonly -servers "127.0.0.1:${PORTS[$OLD_LEADER]}" \
      -dnums "$BASE_N" -vsize "$NEW_VSIZE" -check 50 -sample 2 \
      > "$DATA_BASE/catchup.log" 2>&1 || true
  if grep -qE 'GET 校验: 正确 [0-9]+, 错误 0, 取不到 0' "$DATA_BASE/catchup.log"; then
    ok "旧节点已追上"
    break
  fi
  sleep 1
done
/tmp/conflict-readonly -servers "127.0.0.1:${PORTS[$OLD_LEADER]}" \
    -dnums "$BASE_N" -vsize "$NEW_VSIZE" -check 2000 \
    > "$DATA_BASE/verify-old-node.log" 2>&1 || true
# "GET 校验: 正确 N, 错误 M, 取不到 K, 非leader L"——取 M。
BADGET=$(grep -oE 'GET 校验: 正确 [0-9]+, 错误 [0-9]+' "$DATA_BASE/verify-old-node.log" \
         | tail -1 | grep -oE '错误 [0-9]+' | grep -oE '[0-9]+' || true)
if [ -z "$BADGET" ]; then
  fail "判据 d：回执里没有 GET 校验这一行，拿不到判据"
  tail -10 "$DATA_BASE/verify-old-node.log"
elif [ "$BADGET" != 0 ]; then
  fail "判据 d：旧节点上有 $BADGET 条值不符——它那段未提交的尾巴变成了可见状态"
else
  ok "判据 d：旧节点上 0 条值不符"
fi
grep -E 'GET 校验|SCAN 校验|未持有 leader|FAILOVER_VERIFY' "$DATA_BASE/verify-old-node.log" || true

# ---------- 收尾 ----------
echo
for i in 0 1 2; do
  if [ -n "${PIDS[$i]}" ] && kill -0 "${PIDS[$i]}" 2>/dev/null; then
    echo "node$i 存活 pid=${PIDS[$i]}"
  else
    echo "node$i 已退出"
  fi
done

if [ "$FAILED" = 0 ]; then
  echo -e "${GREEN}CONFLICT_TRUNCATION_OK${NC}"
  exit 0
fi
echo -e "${RED}CONFLICT_TRUNCATION_FAIL ($FAILED 条判据未过)${NC}"
exit 1
