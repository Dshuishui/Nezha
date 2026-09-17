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
fpos(){ echo -e "${RED}[假失败]${NC} $*"; FAILED=$((FAILED+1)); }
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

info "=== 九、直读单节点的闸门（gate_read_ok）==="
# 这一节是 2026-09-16 补的，起因是一个三天没人发现的失效：`-leaderCheck` 自 811ac32
# 起默认开，于是四个驱动脚本里"直读某个指定节点"的步骤**恒判失败**，而上面八节一条
# 都没抓到。原因不是哪一节写错了，而是**整类判据没被审**：直读的判定当时还散在四个
# 脚本里各写一遍，没有共用的一份可审，也没人查过"健康的一轮会不会被判通过"。
OKOUT='GET 校验: 正确 300, 错误 0, 取不到 0, 非leader 0
FAILOVER_VERIFY_OK'
BLOCKED='SCAN 校验: 返回 0 条, 正确 0, 错误 0, 范围失败 0, 非leader 30
目标节点未持有 leader 身份：GET 300 次、SCAN 30 次被 ErrWrongLeader 挡回
FAILOVER_VERIFY_FAIL'
BADDATA='GET 校验: 正确 280, 错误 20, 取不到 0, 非leader 0
FAILOVER_VERIFY_FAIL'

if gate_read_ok "$OKOUT" "健康" >/dev/null; then
    good "健康的直读判过（这正是当时没人查的那一半：特异性）"
else
    fpos "健康的直读被判失败——闸门会永久假失败"
fi
if gate_read_ok "$BLOCKED" "被挡回" >/dev/null; then
    bad "被 ErrWrongLeader 挡回却判过"
else
    w=$(gate_read_ok "$BLOCKED" "被挡回")
    case "$w" in
        *"脚本配置问题"*) good "被挡回判失败，且指明是脚本配置问题、不是数据问题";;
        *) fpos "被挡回判失败了，但原因说成了数据不对——这正是当时查不出来的原因";;
    esac
fi
if gate_read_ok "$BADDATA" "数据错" >/dev/null; then
    bad "数据真的不对却判过"
else
    good "数据不对判失败"
fi
if gate_read_ok "" "空输出" >/dev/null; then
    bad "直读没有任何输出（SSH 超时）被判过"
else
    good "拿不到直读结果不等于判据通过"
fi

info "=== 十、凡是直读单节点的脚本，必须让那个节点关掉 leaderCheck ==="
# 这条静态检查就是本该抓住上面那个 bug 的那一条。判据是**结构性**的而不是措辞性的：
# 只要一个脚本用 `readonly -servers <单个地址>` 去读某个指定节点，那个节点就必须用
# `-leaderCheck=false` 起，否则读一定被挡回。两者缺一不可，所以一起查。
for f in failover.sh recover.sh lsmraft.sh two-node-rounds.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    [ -f "$p" ] || { bad "$f 不存在"; continue; }
    reads=0; grep -q 'readonly -servers' "$p" && reads=1
    [ "$reads" = 1 ] || { warn "$f 不直读单节点，跳过"; continue; }
    if grep -q 'leaderCheck=false' "$p"; then
        good "$f 直读单节点，且节点是用 -leaderCheck=false 起的"
    else
        fpos "$f 直读单节点却没关 leaderCheck——它的直读步骤恒判失败"
    fi
    if grep -q 'gate_read_ok' "$p"; then
        good "$f 走共用的直读判定"
    else
        bad "$f 还在自己写直读判定（四份拷贝各自漂移，这个 bug 就是这么藏住的）"
    fi
done

info "=== 十一、杀掉 leader 之后不许用固定 sleep 等选举 ==="
# 这一节是 2026-09-16 补的，起因与第十节同源但机理不同：recover.sh 在 kill9 leader 之后
# 写的是 `sleep 10`，而 minElectionTimeout=10s + jitter=1s 意味着切换最长 11 秒——
# 固定值**比下界还短**。它不是稳定失败而是**竞态**（当天两轮一过一败），所以比恒判失败
# 更难发现：偶尔过一次就会被当成"偶发抖动"。
#
# 根本毛病是固定 sleep 把**测试脚本的常量**钉在**源码里的常量**上。minElectionTimeout
# 从 3s 改到 10s（811ac32）时没人改脚本，判据就静默失效了。所以这里查的不是"睡够不够久"
# （那等于把同一个耦合再抄一遍），而是**有没有改成轮询**。
ELECT_MIN=$(grep -oE 'minElectionTimeout[[:space:]]*=[[:space:]]*[0-9]+' "$PROJECT_DIR/internal/raft/raft.go" | grep -oE '[0-9]+$')
ELECT_JIT=$(grep -oE 'electionTimeoutJitter[[:space:]]*=[[:space:]]*[0-9]+' "$PROJECT_DIR/internal/raft/raft.go" | grep -oE '[0-9]+$')
if [ -n "$ELECT_MIN" ] && [ -n "$ELECT_JIT" ]; then
    good "源码里的切换上界 = $(( (ELECT_MIN + ELECT_JIT) / 1000 ))s（minElectionTimeout=${ELECT_MIN}ms + jitter=${ELECT_JIT}ms）"
else
    warn "读不到 minElectionTimeout/electionTimeoutJitter，本节只能查写法、不能查数值"
fi
for f in failover.sh recover.sh lsmraft.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    [ -f "$p" ] || { bad "$f 不存在"; continue; }
    grep -q 'kill9 0' "$p" || { warn "$f 不杀 leader，跳过"; continue; }
    if grep -q 'wait_new_leader' "$p"; then
        good "$f 杀掉 leader 后轮询等新 leader 当选（不依赖源码里的超时常量）"
    else
        fpos "$f 杀掉 leader 后用固定 sleep 等选举——源码改超时就会静默变成竞态"
    fi
    # 轮询判据必须看"当选次数增加"而不是"日志里出现过 Candidate -> Leader"：
    # 初始那次选举的记录一直在日志里，按存在性判会立刻返回真，等于没等。
    if grep -q 'wait_new_leader' "$p" && ! grep -q 'leader_wins' "$p"; then
        bad "$f 有 wait_new_leader 但没有 leader_wins：按存在性判会立刻通过，等于没等"
    fi
done

info "=== 十二、\$VAR 后面不许紧跟全角字符（macOS bash 3.2）==="
# macOS 自带 bash 是 3.2.57，它把紧跟在 $VAR 后面的多字节字符**读成变量名的一部分**：
#   cur=7; echo "当选次数 $cur）"   →   bash: cur?: unbound variable
# Linux 的 bash 4.4 没有这个问题（同一段脚本原样输出 7），所以它**只在本机跑的驱动
# 脚本上咬**——failover / recover / gate / maintable / full-compare 这些是在 Mac 上
# 发起的，而 three-node.sh / snapshot-e2e.sh 这些在服务器和 winbox 上跑，属于潜伏。
#
# 为什么必须静态扫：`bash -n` **查不出来**（它只查语法，这是运行时的名字解析），而且
# 只有真的走到那一行才炸。2026-09-16 就是这么丢了一轮：新加的 wait_new_leader 里写了
# `$cur）`，前两个阶段全过，到 S2-a 之后才 unbound variable 退出。
#
# 判据写成 LC_ALL=C 下的 `[^ -~]`（任何非可打印 ASCII 字节），GNU grep 与 BSD grep
# 都支持，不依赖 grep -P。正反对照都验过：`$x）` 抓得到，`${x}）` 不误报。
# 整行注释要排除：bash 对注释**不做变量展开**，所以注释里的 `$x）` 无害（本节自己的
# 说明就举了这种例子）。排除的判据是"行首第一个非空白字符是 #"，不是"这一行含 #"——
# 后者会把 `echo "a # $x）"` 这种真问题一起漏掉。
FW=$(LC_ALL=C grep -rnE '\$[A-Za-z_][A-Za-z0-9_]*[^ -~]' "$PROJECT_DIR/scripts/" 2>/dev/null \
     | grep -vE '^[^:]*:[0-9]+:[[:space:]]*#' || true)
if [ -z "$FW" ]; then
    good "scripts/ 下没有 \$VAR 紧跟全角字符的写法"
else
    echo "$FW" | sed "s#$PROJECT_DIR/##" | sed 's/^/       /'
    fpos "上列位置的 \$VAR 后面紧跟全角字符——在 macOS bash 3.2 上会 unbound variable，加花括号即可"
fi

info "=== 十三、驱动脚本中途死掉必须停掉自己的节点 ==="
# 2026-09-16 实测的一条连锁失效：脚本因一个变量名错误在中途退出，三个节点留在机器上
# 继续跑 → 下一轮启动端口被占、节点 log.Fatalf 退出（e0fdb98 的正确行为）→ 那次失败的
# 启动又把 pid 文件覆盖成死 pid → stop 全部报 STOPPED 而进程还在。一个错误滚成三个，
# 而每一步都"看起来正常"。所以清理必须挂在 EXIT 上，不能只写在正常收尾处。
for f in failover.sh recover.sh lsmraft.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    [ -f "$p" ] || { bad "$f 不存在"; continue; }
    grep -q 'three-node.sh start' "$p" || { warn "$f 不拉节点，跳过"; continue; }
    if grep -qE '^trap .*EXIT' "$p"; then
        good "$f 挂了 EXIT 清理"
    else
        bad "$f 没挂 EXIT 清理——中途死掉会把节点留在机器上"
    fi
    # 两头都要：trap 覆盖不了 SIGKILL（2026-09-16 有一轮被系统因内存不足杀掉，
    # trap 没机会跑，残留把下一轮的启动顶掉），所以启动前必须再清一遍。
    if grep -qE '^cleanup_nodes[[:space:]]*$' "$p"; then
        good "$f 启动前也清一遍（trap 覆盖不了 SIGKILL）"
    else
        bad "$f 启动前不清残留——上一轮被 SIGKILL 的话这一轮起不来"
    fi
done

info "=== 十四、trap/判据用到的共用函数必须真的解析得到 ==="
# 这一类**`bash -n` 查不出来**：bash 在调用时才解析函数名，所以一个调用了不存在函数的
# trap 语法上完全合法，只有 trap 真的触发才炸。2026-09-16 就是这样：给 failover.sh 和
# lsmraft.sh 加 `trap cleanup_nodes EXIT` 时，cleanup_nodes 里用的 host_of 在那两个脚本
# 里根本不存在（只有 recover.sh 有一份），而三份 bash -n 全部通过。
( . "$PROJECT_DIR/scripts/multinode/gate.sh" 2>/dev/null
  missing=""
  for fn in gate_field gate_report_ok gate_read_ok host_of cleanup_nodes leader_wins wait_new_leader; do
      [ "$(type -t "$fn" 2>/dev/null)" = function ] || missing="$missing $fn"
  done
  if [ -n "$missing" ]; then echo "MISSING:$missing"; else echo "ALLOK"; fi
  # 拓扑也顺手钉住：leader_wins 写死了 three-1/three-2 在 tikv241，host_of 必须与之一致。
  echo "TOPO:$(host_of 0)/$(host_of 1)/$(host_of 2)" ) > "$FAKE/fnchk" 2>/dev/null
if grep -q ALLOK "$FAKE/fnchk"; then
    good "gate.sh 提供的七个共用函数全部解析得到"
else
    bad "gate.sh 里缺函数：$(grep MISSING "$FAKE/fnchk" | sed 's/MISSING://')"
fi
if grep -q 'TOPO:tikv240/tikv241/tikv241' "$FAKE/fnchk"; then
    good "host_of 的拓扑与 leader_wins 写死的路径一致（node0 在 240，node1/2 在 241）"
else
    bad "host_of 的拓扑与 leader_wins 不一致：$(grep TOPO "$FAKE/fnchk")"
fi

info "=== 十五、按节点编号访问时不许写死主机名 ==="
# 2026-09-16 实测的最糟一类失效：recover.sh 里 `rq tikv241 "~/three-node.sh kill9 2"`
# 把主机名写成了字面量。TOPO=three 下 node2 在 node55，这条命令发到 241——那里
# ~/work/three-2 根本不存在，pid 读不到、own_pids 为空，于是 three-node.sh **报出
# KILLED**。闸门为一个完全没发生的动作报了成功，节点带着 RocksDB 的 LOCK 继续活着，
# 后面的 restart 因此失败，而失败被归因成"数据不对"。
#
# 判据是结构性的：只要一条远端命令里出现 `three-node.sh <子命令> N` 或 `three-N`，
# 目标主机就必须由 host_of 派生。客户端工具（scanverify/readonly/randwrite）不在此列，
# 它连的是 -servers 地址表，放在哪台机器上只影响网络路径——所以那些用 CLIENT_HOST。
# 准确的不变式是**二择一**：要么所有按编号的访问都经过 host_of（于是任何拓扑都对），
# 要么脚本带着 TOPO=two 守卫（于是字面量与拓扑不可能不一致）。两者都没有才是缺陷。
# 这条规则是完整的：将来谁把某个脚本改成支持 TOPO=three 并摘掉守卫，只要漏一个字面量
# 就会在这里判失败。
for f in failover.sh recover.sh lsmraft.sh slow-follower.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    [ -f "$p" ] || { bad "$f 不存在"; continue; }
    hits=$(grep -nE '\b(r|rq) +(tikv240|tikv241|node55) +.*(three-node\.sh|three-[012])' "$p" || true)
    guarded=no; grep -qE '^\[ "\$TOPO" = two \]' "$p" && guarded=yes
    if [ -z "$hits" ]; then
        good "$f 里按节点编号的访问都经过 host_of（任何拓扑都对）"
    elif [ "$guarded" = yes ]; then
        warn "$f 仍有写死的主机名，但它带 TOPO=two 守卫，所以发不错机器（摘守卫前必须先改成 host_of）"
    else
        echo "$hits" | sed 's/^/       /' | cut -c1-150
        bad "$f 上列行按节点编号访问却写死了主机名，又没有 TOPO=two 守卫——换拓扑会发到错误的机器，且可能报出假成功"
    fi
done

info "=== 十六、良性行的剔除必须与注释一致 ==="
# 2026-09-16：three-node.sh 的注释写着"`发快照失败` 刻意不收进来"，而 ERRPAT 里一个宽泛的
# `EOF` 把它收了回来。SIGSTOP 场景下 leader 给被按住的 follower 发快照**必然**以 EOF 结束
# （对端不处理请求，gRPC 流断开），于是 slow-follower.sh 恒判失败——又一个"假失败"。
# 判据：注释里声明为良性的每一条，必须真的被 errlines 剔掉。
NP="$PROJECT_DIR/scripts/multinode/three-node.sh"
clean_log > "$ND/n.log"
echo "[Error] 2026/09/16 22:42:39 RaftNode[0] 给 peer[1] 发快照失败（6.86s 之后）: EOF" >> "$ND/n.log"
e=$(field_of "$(run_report)" err_lines)
if [ "$e" = 0 ]; then
    good "发快照失败…EOF 不算错误行（与注释一致）"
else
    fpos "发快照失败…EOF 被算成 $e 条错误行，而注释说它是良性的——SIGSTOP 场景必然触发"
fi
# 反向：真损坏仍要被抓到，否则等于把 EOF 整项删了
clean_log > "$ND/n.log"
echo "[RECOVER] failed to read entry at offset 4096: unexpected EOF" >> "$ND/n.log"
e=$(field_of "$(run_report)" err_lines)
if [ "$e" != 0 ]; then
    good "读日志时的意外 EOF 仍算错误行（没有把 EOF 整项删掉）"
else
    bad "读日志时的意外 EOF 不算错误了——剔除剔过头，真损坏会被漏掉"
fi
if grep -q '^BENIGNPAT=' "$NP"; then
    good "良性行集中在 BENIGNPAT 一处"
else
    bad "没有 BENIGNPAT：良性行散在 grep -v 里，加一条就会漏一处"
fi

info "=== 十七、fail 必须在任何置位之前初始化 ==="
# 2026-09-16：slow-follower.sh 把 fail=0 放在第 7 步开头，而第 6 步已经会置 fail=1——
# 第 7 步一执行就把它重置成 0，那一步的失败**静默丢掉**。`bash -n` 查不出来，跑起来也
# 不报错，只是判决变成了通过。判据：fail=0 的行号必须小于任何 fail=1 的行号。
for f in failover.sh recover.sh lsmraft.sh slow-follower.sh; do
    p="$PROJECT_DIR/scripts/multinode/$f"
    [ -f "$p" ] || { bad "$f 不存在"; continue; }
    # 注释行要排除：注释里写着 `fail=1` 的说明文字不是代码（本节的成因说明就是这么写的）。
    # 判据与第十二节一致：行首第一个非空白字符是 # 的算注释。
    init=$(grep -nE '^[[:space:]]*fail=0' "$p" | head -1 | cut -d: -f1)
    first=$(grep -nE 'fail=1' "$p" | grep -vE '^[0-9]+:[[:space:]]*#' | head -1 | cut -d: -f1)
    if [ -z "$first" ]; then warn "$f 没有 fail=1，跳过"; continue; fi
    if [ -z "$init" ]; then bad "$f 有 fail=1 但没有 fail=0 初始化"; continue; fi
    if [ "$init" -lt "$first" ]; then
        good "$f 的 fail=0（第 ${init} 行）在第一处 fail=1（第 ${first} 行）之前"
    else
        bad "$f 的 fail=0 在第 ${init} 行，晚于第一处 fail=1（第 ${first} 行）——会把那之前的失败重置掉"
    fi
done

info "=== 十八、场景之间的清理不许删掉构建产物 ==="
# 2026-09-16：snapshot-crash.sh 的 cleanup 在每个场景之后都跑，而它连 /tmp/sc-node 一起
# 删，于是第二个场景永远起不来——节点日志里只有 "env: '/tmp/sc-node': No such file or
# directory"，外层报的却是"集群没起来或 GC 没跑"。单跑 F 通过、E F 连跑必败，而这个差别
# 一眼看不出来，很容易被当成偶发。
# 判据：多场景脚本里，每场景清理函数不得 rm 构建产物；那属于最终清理（挂 EXIT 的那个）。
for f in snapshot-crash.sh snapshot-e2e.sh; do
    p="$PROJECT_DIR/scripts/test/$f"
    [ -f "$p" ] || { warn "$f 不存在，跳过"; continue; }
    # 只看 `cleanup()` 那个函数体（到下一个顶格 } 为止）
    body=$(awk '/^cleanup\(\)\{/{f=1} f{print} /^\}/{if(f)exit}' "$p")
    if echo "$body" | grep -qE 'rm -f .*/tmp/(sc|sf|se)-'; then
        fpos "$f 的每场景 cleanup 里 rm 了构建产物——后续场景会起不来，且报错指向别处"
    else
        good "$f 的每场景 cleanup 不碰构建产物"
    fi
done

info "=== 十九、项目目录不许写死 ==="
# 2026-09-17：五个脚本（含 B4 要用的 memory-curve.sh）把 PROJECT_DIR 写死成
# $HOME/Github/Nezha，而实验机上仓库在 ~/work/Nezha——在那里直接 "无项目目录" 退出。
# 这类失败响亮，但它把脚本钉在**某一台机器的目录布局**上，而这套脚本现在要在
# Mac、winbox、三台实验机之间搬。判据：按脚本自身位置推导。
# 只看**行首的赋值语句**（可带缩进），这样既排除注释，也排除本节自己那行 grep 模式——
# 第一版没排除，审计把自己的判据当成了被审对象。
BAD=$(grep -rnE '^[[:space:]]*PROJECT_DIR=.*\$HOME/' "$PROJECT_DIR/scripts/" 2>/dev/null || true)
if [ -z "$BAD" ]; then
    good "scripts/ 下没有写死项目目录的脚本"
else
    echo "$BAD" | sed "s#$PROJECT_DIR/##" | sed 's/^/       /' | cut -c1-140
    bad "上列脚本把 PROJECT_DIR 写死在某台机器的布局上——换机器就跑不了"
fi

info "=== 二十、RSS 采样器不许在等待窗口之前就停 ==="
# 2026-09-17：raftlog-memory.sh 与 memory-curve.sh（B4 要用的那个）都是先 kill 采样器、
# 再 sleep 等 compactLog。于是"峰值"只覆盖写入阶段，而压缩后那个数是一次孤立采样；
# compactLog 用 make+copy 会临时再分配等长数组，Go 又不把 RSS 还给 OS，所以"压缩后"
# 必然大于"峰值"（实测 361MB vs 415MB），读起来像**压缩把内存搞大了**。
# 那是测量假象。判据：kill 采样器的行号必须晚于它后面那个 sleep。
for f in test/raftlog-memory.sh bench/memory-curve.sh bench/memory-scale.sh bench/avp-compare.sh; do
    p="$PROJECT_DIR/scripts/$f"
    [ -f "$p" ] || { warn "$f 不存在，跳过"; continue; }
    grep -q 'start_rss_sampler' "$p" || { warn "$f 不采样 RSS，跳过"; continue; }
    kl=$(grep -nE '^[[:space:]]*kill \$(SAMPLER|S)\b' "$p" | head -1 | cut -d: -f1)
    if [ -z "$kl" ]; then warn "$(basename "$f") 没找到 kill 采样器那行，跳过"; continue; fi
    # kill 的前一行是不是 sleep（等压缩），或者 kill 之后 5 行内还有 sleep（说明顺序反了）
    before=$(sed -n "$((kl-1))p" "$p")
    after=$(sed -n "$((kl+1)),$((kl+5))p" "$p")
    if echo "$after" | grep -qE '^[[:space:]]*sleep [0-9]+' && ! echo "$before" | grep -qE 'sleep [0-9]+'; then
        fpos "$(basename "$f") 先 kill 采样器（第 ${kl} 行）再 sleep 等压缩——峰值会漏掉压缩期"
    else
        good "$(basename "$f") 的采样覆盖到等待窗口之后"
    fi
done

info "=== 二十一、注释不许插在 \\ 续行的中间 ==="
# 2026-09-17：往 verify-goodput.sh 的节点启动命令里加说明时，把注释插在了 `\` 续行
# 之后，于是命令被拆成两半——节点只拿到前半截参数（**连 > "$D/n.log" 重定向都丢了**，
# stdout 直接漏进脚本输出），后半截 `-system nezha-nogc ...` 被当成另一条命令执行。
# 症状是"写入不落地 + 节点日志乱漏"，指向别处。`bash -n` 查不出来：拆开的两半各自
# 都是合法语法。
# 判据：一行以 `\` 结尾时，下一行不得是注释。注释块整体位于命令之前是没问题的，
# 所以只看"续行之后"这一种。
CONT=$(awk 'prev ~ /\\$/ && $0 ~ /^[[:space:]]*#/ { printf "%s:%d: %s\n", FILENAME, NR, $0 } { prev = $0 }' \
       $(find "$PROJECT_DIR/scripts" -name '*.sh') 2>/dev/null \
       | grep -v 'collect-results.sh' || true)
if [ -z "$CONT" ]; then
    good "没有注释插在 \\ 续行中间"
else
    echo "$CONT" | sed "s#$PROJECT_DIR/##" | sed 's/^/       /' | cut -c1-130
    bad "上列位置的注释插在 \\ 续行之后——命令会被拆成两半，且 bash -n 查不出来"
fi

info "=== 二十二、守卫的退出码不许被管道吃掉 ==="
# 2026-09-17：七处写成了 `require_out ... | tee -a "$LOG" || die`。`||` 作用在**整个管道**
# 上，而管道的退出码是最后一条命令（tee）的，恒为 0——守卫永远不触发。
# 实测：start node0 没有回执，[闸门] 那行照常打出来，脚本照样往下跑，整轮跑完才发现
# 只有两个节点在跑。我加 require_out 正是为了让静默失败变响，第一版却用一根管子把它废了。
#
# 判据只匹配 `|| ` **紧跟在管道之后**的形式；先赋值再判（`w=$(g); rc=$?; ... [ $rc = 0 ] || ...`）
# 里也有 tee 和 ||，但 || 绑的是 `[ ... ]`，是对的，不能误报。
BADPIPE=$(grep -rnE '^[^#]*\b(require_out|gate_read_ok|gate_report_ok)\b[^|]*\|[[:space:]]*tee[^|;]*\|\|' \
          "$PROJECT_DIR/scripts/" 2>/dev/null || true)
if [ -z "$BADPIPE" ]; then
    good "没有守卫的退出码被管道吃掉"
else
    echo "$BADPIPE" | sed "s#$PROJECT_DIR/##" | sed 's/^/       /' | cut -c1-140
    bad "上列守卫的 || 绑在管道上（退出码是 tee 的，恒为 0）——守卫不会触发。先赋值再判。"
fi

info "=== 二十三、被审的脚本集合必须是**发现**出来的，不能靠手写清单 ==="
# 前面第八、十、十一、十三、十四、十五、十七、十八节都带着一份写死的文件名清单
# （failover.sh recover.sh lsmraft.sh …）。清单本身就是一个失效点：新写的驱动脚本
# 不在清单里，于是**整类判据对它一条都不生效**，而自审照样全绿。
# 2026-09-17 实测：scripts/test/ 下的驱动（crash-recovery.sh、gc-rounds.sh，以及当天
# 新写的 conflict-truncation.sh）从来没有被上面任何一节审过；同一天还发现
# two-node-rounds.sh 起了两个节点却没有 EXIT trap——它不在第十三节的清单里。
#
# 所以这一节先把"驱动脚本"**判定**出来（会起节点的脚本），再对整个集合施加两条
# 判得准的检查。判定规则：出现 nohup 且带节点参数，或者调用了 three-node.sh /
# rep-node.sh 的 start。
discover_drivers() {
    local p
    while IFS= read -r p; do
        if grep -q 'nohup' "$p" && grep -qE -- '-internalAddress|-peers' "$p"; then
            echo "$p"; continue
        fi
        grep -qE '(three-node|rep-node)\.sh[^|]*(start|restart)' "$p" && echo "$p"
    done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
}
DRIVERS=$(discover_drivers)
if [ -z "$DRIVERS" ]; then
    bad "一个驱动脚本都没发现——判定规则本身失效了"
else
    good "发现 $(echo "$DRIVERS" | wc -l | tr -d ' ') 个会起节点的脚本"
fi

# (a) 会起节点的脚本必须有 EXIT trap，否则中途死掉会把节点留在**共用**的机器上。
#     三个例外，都是按设计如此，写在这里而不是靠读者自己判断：
#       three-node.sh / rep-node.sh 是节点管理脚本，stop 是它自己的一个子命令；
#       gate.sh 是被 source 的库，trap 归调用方；
#       gc-election.sh 刻意把节点留下供事后检查（脚本末尾自己说明了）。
for p in $DRIVERS; do
    b=$(basename "$p")
    case "$b" in three-node.sh|rep-node.sh|gate.sh|gc-election.sh) continue;; esac
    rel=${p#"$PROJECT_DIR"/}
    if grep -qE '^[[:space:]]*trap .*EXIT' "$p"; then
        good "$rel 有 EXIT trap"
    else
        bad "$rel 会起节点却没有 EXIT trap——中途死掉会把节点留在共用机器上"
    fi
done

# (b) 凡是等选举的脚本，不许用固定 sleep。判据与第十一节一致，但集合是发现出来的。
#
# 判定要**两个**条件同时成立，只看 kill 是不够的：单节点的 benchmark 收尾时也 kill -9
# 自己的节点，那不是"杀 leader 逼选举"，它压根没有选举可等。第一版只看 kill，于是
# amplification / groupcommit-sweep / maintable / gc-rounds 四个单节点脚本全被误报——
# 这正是本文件开头说的那个毛病：灵敏而不特异。
# 所以再要求它是**多节点**的：peers 列表里有逗号，或者它调用了 three-node.sh /
# rep-node.sh 的 start（那两个脚本天然是多节点拓扑）。
for p in $DRIVERS; do
    b=$(basename "$p")
    case "$b" in three-node.sh|rep-node.sh|gate.sh) continue;; esac
    kills=0
    grep -qE 'kill -9|kill9|kill -CONT|-CONT ' "$p" && kills=1
    [ "$kills" = 1 ] || continue
    multi=0
    grep -qE -- '-peers[^\n]*,' "$p" && multi=1
    grep -qE '(three-node|rep-node)\.sh[^|]*(start|restart)' "$p" && multi=1
    grep -qE 'PEERS=.*,' "$p" && multi=1
    [ "$multi" = 1 ] || continue
    rel=${p#"$PROJECT_DIR"/}
    if grep -qE 'wait_new_leader|leader_wins|won_count|Candidate -> Leader|-> Leader' "$p"; then
        good "$rel 按「当选次数/日志标记」轮询等选举"
    else
        bad "$rel 杀了 leader 却没有轮询判据——固定 sleep 会把脚本常量钉在源码常量上"
    fi
done

info "=== 二十四、脚本里用的 bench 工具参数必须真的存在 ==="
# 工具的参数表与脚本各改各的，而 Go 的 flag 包遇到未声明的参数会**打一段 usage 然后退出**。
# 脚本拿到的是空回执，判据于是报"系统没通过"，而真因是自己写了一个不存在的参数。
# 2026-09-17 连撞两次：给 scanverify 写了 -mode / -kstart（它两个都没有，写入与读取
# 分不开、key 段也没法指定），给 readonly 写了 -leader（只有 scanverify 有）。
# 后者是在真跑完一轮之后才发现的——判据 c 报"集群校验未通过"，而集群是好的。
#
# 判据：对每个 cmd/bench/<工具>，扫出它声明的全部 flag，再把脚本里**调用**它那一行的
# 参数逐个对照。三处容易误报的地方都处理了：
#   词界     —— scan 是 scanverify / scan_pro 的前缀，不整词匹配就会拿错参数表
#   管道之后 —— `... | grep -oE` 里的 -oE 是 grep 的
#   go build —— `go build -o /tmp/scanverify ...` 里的 -o 是 go 的
#   变量名   —— **第四类，2026-09-18 补的。** 前缀类 `[-A-Za-z0-9_./]*` 允许字母，
#              于是 t=scan 时 `$mscan -tests 1` 里的 `mscan ` 也算命中（`m` 被前缀吃掉），
#              审计于本仓库自己的新脚本上报了 5 处不存在的误用。名字里带工具名的
#              shell 变量很常见（mscan / put_scan_n），所以这是个会反复出现的误报。
#              办法是要求工具名前面那个字符是路径分隔符、连字符、空白或引号之一——
#              `/tmp/mt3-scan_pro` 里 scan_pro 前面是 `-`（命中，对），
#              而 `$mscan` 里 scan 前面是 `m`（不命中，对）。
FLAGBAD=0
for d in "$PROJECT_DIR"/cmd/bench/*/; do
    t=$(basename "$d")
    # flag.Float64 这类带数字的类型名：正则必须是 [A-Za-z0-9]+ 而不是 [A-Za-z]+，
    # 否则 randwrite_goroutine 的 -rangeFrac 扫不出来，对它的误用就查不到。
    declared=" $(grep -ohE 'flag\.[A-Za-z0-9]+\("[A-Za-z0-9_]+"' "$d"*.go 2>/dev/null | grep -oE '"[A-Za-z0-9_]+"' | tr -d '"' | sort -u | tr '\n' ' ')"
    [ "$declared" = " " ] && continue
    while IFS= read -r hit; do
        f=${hit%%:*}; rest=${hit#*:}; ln=${rest%%:*}; line=${rest#*:}
        case "$line" in *"go build"*) continue;; esac
        # 整行注释要排除，判据与第十二、十七节一致：行首第一个非空白字符是 #。
        # 本节自己的成因说明里就写着 -mode / -kstart / -leader，不排除就会审到自己。
        printf '%s' "$line" | grep -qE '^[[:space:]]*#' && continue
        args=$(printf '%s' "$line" | grep -oE "(^|[[:space:]\"'/-])${t}[[:space:]\"].*" | head -1)
        [ -n "$args" ] || continue
        args=${args#*"$t"}
        args=${args%%|*}; args=${args%%>*}
        for fl in $(printf '%s' "$args" | grep -oE '[[:space:]]-[A-Za-z0-9_]+' | tr -d ' -'); do
            case "$declared" in
                *" $fl "*) ;;
                *) echo "       ${f#"$PROJECT_DIR"/}:${ln}  $t 没有 -$fl"
                   FLAGBAD=$((FLAGBAD+1));;
            esac
        done
    done < <(grep -rnE -- "$t" "$PROJECT_DIR"/scripts/ --include='*.sh' 2>/dev/null)
done
if [ "$FLAGBAD" -eq 0 ]; then
    good "脚本里用到的 bench 工具参数都真的存在"
else
    bad "上列 $FLAGBAD 处用了工具没有的参数——Go 的 flag 会打 usage 然后退出，回执是空的"
fi

info "=== 二十五、字节数不许走 awk 的数值路径（node55 的 awk 是 32 位的）==="
# 2026-09-18 实测，同一个值 3237657361（3.2GB 的目录占用）在 node55 上：
#     awk '{print $1+0}'      -> 3.23766e+09     走 OFMT（%.6g），科学计数法
#     awk '{printf "%d", $1}' -> 2147483647      走 C 的整数转换，**截断成 2^31-1**
#     awk '{print $1}'        -> 3237657361      打字段（awk 里字段是字符串），对
#     cut -f1                 -> 3237657361      对
# 241/240 上 print $1+0 还能打成整数，所以这个坑**只在 node55 上出现**，而 node55
# 是 TOPO=three 的第三个节点。后一种写法比前一种更糟：科学计数法一眼看得出不对，
# 而 2147483647 看起来像个正常数字，会静默地毁掉那一列（采样器的 data_bytes 就这样
# 被毁过一轮）。
# node55 的 awk 是 mawk（连 --version 都不认，只认 -W version）。
#
# 判据：一个脚本若处理原始字节数（du -sb / df -k / stat -c %s），
# 就不许出现 `$N+0` 的输出或对字段的 `printf "%d"`。整行注释排除，判据与前几节一致。
#
# 正则里**不能要求那对引号**。awk 程序常常写在 shell 的双引号里，于是文件里的字面文本是
# `printf \"%d\", \$1`——带反斜杠。第一版写成 `printf[^,]*"%d"`，遇到 `\"%d\"` 就匹配不上，
# 于是注入一行明确的误用之后本节照样报"好"（2026-09-18 实测）。所以只认 `%d`，
# 并允许字段前面有一个反斜杠。
#
# 还要**把本文件排除掉**。上面那句解释里写着 du -sb，下面这行 grep 的模式里写着
# `print $N+0`，两者都不在注释行上，于是本节会把自己的判据当成被审对象——
# 与第十九、二十四节踩过的是同一个坑。
BYTEBAD=0
while IFS= read -r p; do
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    grep -qE 'du -sb|du -b|df -k|stat -c %s|stat --format' "$p" || continue
    while IFS= read -r hit; do
        ln=${hit%%:*}; line=${hit#*:}
        printf '%s' "$line" | grep -qE '^[[:space:]]*#' && continue
        echo "       ${p#"$PROJECT_DIR"/}:${ln}  $(printf '%s' "$line" | cut -c1-110)"
        BYTEBAD=$((BYTEBAD+1))
    done < <(grep -nE 'awk[^|]*(print[[:space:]]+\\?\$[0-9]+[[:space:]]*\+[[:space:]]*0|printf[^;]*%d[^;]*,[[:space:]]*\\?\$[0-9]+)' "$p" || true)
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
if [ "$BYTEBAD" -eq 0 ]; then
    good "处理字节数的脚本都没走 awk 的数值路径"
else
    bad "上列 $BYTEBAD 处把字节数送进了 awk 的数值路径——node55 上会变成科学计数法或被截断成 2^31-1"
fi

echo
if [ "$FAILED" -eq 0 ]; then
    good "自审通过：注入的每一种故障都被判出来了，良性行一条都没被误判"
else
    echo -e "${RED}[FAIL]${NC} $FAILED 项未通过"
    exit 1
fi
