#!/bin/bash
# 多节点闸门自审：往节点日志里注入已知故障，看闸门是不是真的会判失败。
#
# 方法与 gate-audit.sh 相同，那一轮抓到 4 个真缺陷。这里审的是另一批闸门：
# 多节点驱动脚本（failover / recover / lsmraft / two-node-rounds）的判定全部建立在
# 节点脚本的 REPORT 行上，而 REPORT 行里的计数与"什么算错误"的定义是**纯文本判据**——
# 定错了不会报错，只会静默地永远判过或永远判失败。
#
# 审的是**真实代码**，不是它的副本：
#   - 直接跑 scripts/multinode/three-node.sh report 与 rep-node.sh report，
#     只是把 HOME 指到一个临时目录、数据目录里放一份合成的 n.log；
#   - 直接调 scripts/multinode/gate.sh 里的 gate_report_ok。
# 唯一没被覆盖到的是 ssh 那一跳（本机没有那三台机器），所以"拿不到 REPORT"这一种
# 用空串模拟。
#
# 为什么值得单独审：这批闸门在改造之后有两处**已经失效**，都是这次审出来的——
#   1. ERRPAT 把 `LOG-STUCK` 当错误，而它在快照路径接手之后是良性的 → 任何真的跑到
#      快照的运行都会被判失败（假失败，比漏报更能训练人去忽略闸门）。
#   2. 三个驱动脚本都不看 `alive=`，于是一个**静默死掉**的节点照样判过——而那正是
#      这次改造要防的死法（leader 内存被慢 follower 钉住，最后被 OOM 杀掉）。
#
# 用法: bash scripts/test/gate-audit-multinode.sh
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[AUDIT]${NC} $*"; }
good(){ echo -e "${GREEN}[ 好 ]${NC} $*"; }
bad(){  echo -e "${RED}[漏报]${NC} $*"; FAILED=$((FAILED+1)); }
warn(){ echo -e "${YEL}[注意]${NC} $*"; }

PROJECT_DIR="${PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"; cd "$PROJECT_DIR" || exit 1
FAILED=0
# 用 TMPDIR 下的临时目录，并且**必须检查**它真的建出来了。
# 不检查的后果实测过一次：mktemp 在 mac 的沙箱里失败，FAKE 成了空串，
# 后面每个 "$FAKE/work/..." 都变成绝对路径 /work/...，一连串 Operation not permitted，
# 而脚本照样往下跑、报出一堆毫无意义的判定结果。
FAKE=$(mktemp -d "${TMPDIR:-/tmp}/gate-audit.XXXXXX" 2>/dev/null) || FAKE=""
[ -n "$FAKE" ] && [ -d "$FAKE" ] || { echo "建不了临时目录（TMPDIR=${TMPDIR:-/tmp}）"; exit 1; }
trap 'rm -rf "$FAKE"' EXIT

# three-node.sh 与 rep-node.sh 都 `source ~/env.sh` 并 `cd ~/work/Nezha`，
# 所以造一个假 HOME：env.sh 给个空文件，Nezha 指向真仓库（key_width 要读 client.go）。
mkdir -p "$FAKE/work/tmp"
: > "$FAKE/env.sh"
ln -s "$PROJECT_DIR" "$FAKE/work/Nezha"
IDX=9
ND="$FAKE/work/three-$IDX"
RD="$FAKE/work/rep-leader"
mkdir -p "$ND" "$RD"

# 一份"干净"的节点日志：有一轮完整的 GC（两行横幅，只能算一轮）、
# 有角色变迁、有良性的瞬时错误。
clean_log() {
    cat <<'LOG'
[SYSTEM] nezha | kvSeparation=true gcEnabled=true
RaftNode[0] Follower -> Candidate
RaftNode[0] Candidate -> Leader
Starting garbage collection
垃圾回收完成，共花费了1.23s
第 1 轮垃圾回收完成，等待下 1 轮垃圾回收，且已删除 oldLog 指向的文件
[LSM-Raft] ship [1,100] to 192.168.1.241:3099 failed 1 time(s): peer is down
currentTerm[7]
LOG
}

# report <期望 alive> —— 跑真实的 three-node.sh report，回声 REPORT 行
run_report() {
    HOME="$FAKE" bash "$PROJECT_DIR/scripts/multinode/three-node.sh" report "$IDX" 2>/dev/null \
        | grep '^REPORT'
}
run_rep_report() {
    HOME="$FAKE" bash "$PROJECT_DIR/scripts/multinode/rep-node.sh" report leader 2>/dev/null \
        | grep '^REPORT'
}

# 让节点"活着"：pid 指向一个真的在跑的进程。
#
# `sleep 600 &` 的 stdout 必须重定向掉。不重定向的话，后台任务会继承命令替换那个
# 子 shell 的 stdout，于是 `PID=$(alive_pid)` 要一直等到 sleep 结束——600 秒。
# （第一版就是这么写的，整个自审卡了 10 分钟才被超时掐断。）
alive_pid() { sleep 600 >/dev/null 2>&1 & echo $! > "$ND/pid"; echo $!; }
dead_pid()  { echo 999999 > "$ND/pid"; }   # 不存在的 pid

# shellcheck source=scripts/multinode/gate.sh
. "$PROJECT_DIR/scripts/multinode/gate.sh"

field_of() { gate_field "$1" "$2"; }

# check <场景名> <注入的日志行，空串表示不注入> <期望 gate_report_ok 结果：pass|fail>
check() {
    local name=$1 inject=$2 want=$3 rep why rc
    clean_log > "$ND/n.log"
    [ -n "$inject" ] && echo "$inject" >> "$ND/n.log"
    rep=$(run_report)
    why=$(gate_report_ok "$rep" yes "node$IDX") && rc=pass || rc=fail
    printf "       %s\n" "$(sed 's/^REPORT //' <<<"$rep" | cut -c1-120)"
    if [ "$rc" = "$want" ]; then
        good "$name → ${rc}（期望 ${want}）"
        [ -n "$why" ] && printf "%s\n" "$why"
    else
        bad "$name → ${rc}，期望 $want"
        [ -n "$why" ] && printf "%s\n" "$why"
    fi
}

PID=$(alive_pid)
cleanup_pid() { kill "$PID" 2>/dev/null; }
trap 'cleanup_pid; rm -rf "$FAKE"' EXIT

info "=== 一、真实故障必须被判出来 ==="
check "干净日志" "" pass
check "注入 DATA RACE" "WARNING: DATA RACE" fail
check "注入 panic" "panic: runtime error: index out of range" fail
check "注入 fatal" "fatal error: concurrent map writes" fail
check "注入 GC 出错" "垃圾回收出现了错误，本轮不推进状态、不删除旧文件:  read error" fail
check "注入恢复失败" "[RECOVER] 装载分区失败: manifest says 100 bytes" fail

info "=== 二、快照路径：良性行不能被当成错误（否则是假失败）==="
check "LOG-STUCK（交给快照，良性）" \
      "[LOG-STUCK] peer[1] 的 nextIndex=40002 已落在压缩点 39679 之前——交给快照补齐" pass
check "LOG-TRUNCATE（按预算截断，良性）" \
      "[LOG-TRUNCATE] peer[1] 只复制到 40001，而内存日志已达 19MB（预算 16MB），压缩点按预算推到 42503——它将通过快照补齐" pass
check "SNAPSHOT 做好/装好（良性）" \
      "[SNAPSHOT] 装好一份：位点=(39679,1) applied=60001 日志到=60001" pass
check "发快照失败（瞬时，不计）" \
      "RaftNode[0] 给 peer[1] 发快照失败（2.3s 之后）: connection refused" pass

info "=== 三、快照路径：静默的真失效必须被判出来 ==="
check "LOG-STUCK（没启用快照，真卡死）" \
      "[LOG-STUCK] peer[1] 的 nextIndex=40002 已落在压缩点 39679 之前，它再也追不上了——补齐需要快照，而本节点没有启用。这个 peer 从此不再接收日志" fail
check "安装失败" \
      "[SNAPSHOT] 安装失败（本节点仍是原来的状态）: ingest 存储引擎导出: corruption" fail
check "制作快照失败" \
      "RaftNode[0] peer[1] 制作快照失败: export store: disk full" fail
check "没装上快照" \
      "RaftNode[0] peer[1] 没装上快照: FAILED" fail

info "=== 四、节点静默死掉必须被判出来 ==="
clean_log > "$ND/n.log"
dead_pid
rep=$(run_report)
printf "       %s\n" "$(sed 's/^REPORT //' <<<"$rep" | cut -c1-120)"
if [ "$(field_of "$rep" alive)" = no ]; then
    good "report 认出节点已死（alive=no）"
else
    bad "节点已死但 report 说 alive=$(field_of "$rep" alive)"
fi
if why=$(gate_report_ok "$rep" yes "node$IDX"); then
    bad "节点已死（日志干净），闸门仍判过——这正是改造之前的漏报"
else
    good "闸门判失败：$(head -1 <<<"$why" | sed 's/^ *//')"
fi
# 故意杀掉的节点该被接受为 alive=no
if gate_report_ok "$rep" no "node$IDX" >/dev/null; then
    good "期望 alive=no 时同一份 REPORT 判过（failover 里 node0 就是这种）"
else
    bad "期望 alive=no 却判失败"
fi
echo "$PID" > "$ND/pid"   # 恢复

info "=== 五、拿不到 REPORT 不等于判据通过 ==="
if gate_report_ok "" yes "node$IDX" >/dev/null; then
    bad "空 REPORT（SSH 超时）被判过"
else
    good "空 REPORT 判失败"
fi
if gate_report_ok "REPORT node9 alive=yes gc_done=1" yes "node$IDX" >/dev/null; then
    bad "REPORT 缺 races/err_lines 字段却被判过——格式变了而判据没跟上"
else
    good "REPORT 缺字段判失败"
fi

info "=== 六、子串匹配的老毛病 ==="
# 旧写法 `grep -qE 'races=0 err_lines=0'` 的两个问题，用同一份 REPORT 对照
FAKEREP="REPORT node9 alive=yes gc_done=1 races=0 err_lines=03"
if grep -qE 'races=0 err_lines=0' <<<"$FAKEREP"; then
    if gate_report_ok "$FAKEREP" yes "node9" >/dev/null; then
        bad "err_lines=03 被判过"
    else
        good "err_lines=03：旧的子串写法会判过，现在判失败"
    fi
else
    warn "旧写法在这条上也不匹配，这一项无从对照"
fi
FAKEREP2="REPORT node9 alive=yes races=0 gc_done=1 err_lines=0"
if grep -qE 'races=0 err_lines=0' <<<"$FAKEREP2"; then
    warn "字段顺序变化对旧写法无影响，这一项无从对照"
else
    if gate_report_ok "$FAKEREP2" yes "node9" >/dev/null; then
        good "字段顺序变化：旧的相邻写法会永久假失败，现在按字段取值仍判过"
    else
        bad "字段顺序变化导致判失败——判据仍然依赖字段相邻"
    fi
fi

info "=== 七、GC 轮数的计数 ==="
clean_log > "$ND/n.log"
rep=$(run_report)
g=$(field_of "$rep" gc_done)
if [ "$g" = 1 ]; then
    good "一轮 GC 的两行横幅只算一轮（gc_done=1）"
else
    bad "一轮 GC 被算成 $g 轮——'垃圾回收完成' 会匹配到它前面那行横幅"
fi
# rep-node.sh 此前少了"轮"字，同一份日志会报 2
clean_log > "$RD/n.log"
echo "$PID" > "$RD/pid"
rrep=$(run_rep_report)
printf "       %s\n" "$(sed 's/^REPORT //' <<<"$rrep" | cut -c1-120)"
rg=$(field_of "$rrep" gc_done)
if [ "$rg" = 1 ]; then
    good "rep-node.sh 的计数与 three-node.sh 一致（gc_done=1）"
else
    bad "rep-node.sh 报 gc_done=${rg}，与 three-node.sh 的 1 不一致——两份节点脚本判据漂移"
fi
# 两份脚本对"错误"的定义也要一致
clean_log > "$ND/n.log"; clean_log > "$RD/n.log"
line="[SNAPSHOT] 安装失败（本节点仍是原来的状态）: corruption"
echo "$line" >> "$ND/n.log"; echo "$line" >> "$RD/n.log"
e1=$(field_of "$(run_report)" err_lines); e2=$(field_of "$(run_rep_report)" err_lines)
if [ "$e1" = "$e2" ] && [ "$e1" != 0 ]; then
    good "同一条错误行在两份节点脚本下都算 $e1 条"
else
    bad "同一条错误行：three-node.sh 报 ${e1}、rep-node.sh 报 $e2"
fi

info "=== 八、驱动脚本是否真的用上了这些判据 ==="
for f in failover.sh recover.sh lsmraft.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    if grep -q 'gate_report_ok' "$p"; then
        good "$f 走共用判定"
    else
        bad "$f 还在自己写判定（三份拷贝各自漂移，就是这么来的）"
    fi
    if grep -qE "grep -qE 'races=0 err_lines=0'" "$p"; then
        bad "$f 里还留着子串写法"
    fi
done
if grep -q 'rep-node.sh' "$PROJECT_DIR/scripts/multinode/deploy.sh"; then
    good "deploy.sh 会把 rep-node.sh 送到服务器"
else
    bad "deploy.sh 不送 rep-node.sh——对它的改动到不了服务器，而且不报错"
fi

echo
if [ "$FAILED" -eq 0 ]; then
    good "自审通过：注入的每一种故障都被判出来了，良性行一条都没被误判"
else
    echo -e "${RED}[FAIL]${NC} $FAILED 项未通过"
    exit 1
fi
