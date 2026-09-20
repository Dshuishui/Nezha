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

info "=== 二十六、丢 key 检查取不到值时不许当成通过 ==="
# 2026-09-18 实测：4GB 那一格的 lost-keys.py 被 SSH 超时掐断，回执为空 -> NA，
# 而判定写的是
#     [ "$LOST" != NA ] && [ "$LOST" -gt 0 ]
# NA 把整条判定短路掉，那一格**照常跑完 GET 与 SCAN**，日志上只留下
# "盘上丢失 NA 条"一行。而 GC 搬丢记录不报任何错，只会在某次 GET 上变成一个 NOKEY，
# 命中率也看不出来——这个检查是唯一能发现它的地方。
# 同样的写法在 maintable.sh（单节点主表驱动）里也有一份，所以这是一类，不是一处。
#
# 判据：调用 lost-keys.py 的脚本里，不许出现 `!= NA` 与 `-gt 0` 写在**同一条** if 上
# 的形式——那个形式的语义就是"取不到值就跳过"。正确写法是把 NA 单独判一支。
# 本文件要排除：上面这段解释里就写着那个模式。
NABAD=0
while IFS= read -r p; do
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    grep -q 'lost-keys.py' "$p" || continue
    while IFS= read -r hit; do
        ln=${hit%%:*}; line=${hit#*:}
        printf '%s' "$line" | grep -qE '^[[:space:]]*#' && continue
        echo "       ${p#"$PROJECT_DIR"/}:${ln}  $(printf '%s' "$line" | cut -c1-110)"
        NABAD=$((NABAD+1))
    done < <(grep -nE '!=[[:space:]]*"?NA"?[[:space:]]*\]([[:space:]]*&&)' "$p" | grep -E '\-gt|\-ge|\-ne' || true)
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
if [ "$NABAD" -eq 0 ]; then
    good '丢 key 检查没有把「取不到值」当成通过'
else
    # 消息里不能用反引号：bash 在双引号里会把它当成命令替换，第一版因此打出
    # "!=: command not found"，而消息本身变成空的——判据抓到了，却说不出抓到了什么。
    bad "上列 $NABAD 处用 '!= NA' 短路掉了丢 key 判定——检查没做会被读成通过"
fi

info '=== 二十七、内嵌在 bash -c 里的脚本也要过语法检查 ==='  # 标题不用反引号：双引号里会被命令替换
# 2026-09-18：给采样循环加了一句 `case "$datab" in ''|*[!0-9]*) ...`，而那整段在
# `bash -c '...'` 的**单引号**里——那对单引号被外层吃掉，只剩 `in |*[!0-9]*)`。
# 结果：采样循环起来就语法错误、**一条数据都不写**，而 `bash -n` 对外层文件完全通过
# （错误在字符串里面，外层看不见）。整个 C 阶段零采样，是从 sampler.log 里才发现的。
#
# 所以把那些内嵌脚本抽出来单独查。用 python 找 `bash -c '` 到配对单引号之间的那一段
# （内嵌脚本里出现单引号就会提前截断，这种情况会被下面的 bash -n 报出来，
# 属于宁可误报也不漏报的一侧）。
EMBED=0
while IFS= read -r p; do
    # 排除本文件：上面那段成因说明里就写着被检查的那种构造，理由同第二十五、二十六节。
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    n=0
    while IFS= read -r body; do
        n=$((n+1))
        [ -z "$body" ] && continue
        printf '%s' "$body" | base64 -d 2>/dev/null | bash -n 2>"$FAKE/embed.err" && continue
        echo "       ${p#"$PROJECT_DIR"/}  第 ${n} 段内嵌脚本语法错误："
        sed 's/^/           /' "$FAKE/embed.err" | head -3
        EMBED=$((EMBED+1))
    done < <(python3 - "$p" <<'PYEOF'
import base64, re, sys
src = open(sys.argv[1], encoding="utf-8", errors="replace").read()
# 只认 `bash -c '` 这一种写法（本仓库唯一用到的），到下一个单引号为止。
for m in re.finditer(r"bash -c '", src):
    start = m.end()
    end = src.find("'", start)
    if end == -1:
        continue
    body = src[start:end]
    if body.strip():
        print(base64.b64encode(body.encode()).decode())
PYEOF
)
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
if [ "$EMBED" -eq 0 ]; then
    good "内嵌在 bash -c 里的脚本都过语法检查"
else
    bad "上列 $EMBED 段内嵌脚本有语法错误——外层的 bash -n 查不出来，跑起来才炸"
fi

echo
info "=== 二十八、mktemp 不许用 -t（BSD 与 GNU 语义不同）==="
# 2026-09-18 实测：deploy.sh 第 49 行写的是
#     CONNERR=$(mktemp -t deploy-ssh).err
# 在 mac（BSD mktemp）上，`-t 前缀` 是合法的，自己补随机后缀；
# **GNU mktemp 的 -t 却要求模板里自带至少三个 X**，给裸前缀直接报
#     mktemp: too few X's in template 'deploy-ssh'
# 于是脚本在 set -eu 下当场退出，整个部署只留这一行，看着像环境坏了。
# 这个洞藏了很久，因为部署一直只从 mac 发起；那天改成从 WSL（Linux）发起，第一次跑就撞上。
#
# 现在实验机那条链路要求"长命令一律在 Linux 侧发起"，于是**每个脚本都可能在 GNU 上跑**，
# 这就从一处笔误变成了一类问题。
#
# 判据干脆定成"**不许用 -t**"，而不是"用了 -t 就检查模板里有没有 X"：
# 后者判不干净——`mktemp -t foo.XXXXXX` 两边都不报错，但 BSD 把整个 foo.XXXXXX 当前缀、
# 再往后接自己的随机段，GNU 则替换那六个 X，**同一行在两个平台上给出不同形状的名字**。
# 这种"不报错但语义不同"的写法比直接报错更难查，所以一并禁掉，
# 统一要求写成 `mktemp "${TMPDIR:-/tmp}/名字.XXXXXX"`（`mktemp -d` 不带 -t，不受影响）。
# 顺带禁掉 `$(mktemp ...).后缀` 这个写法：mktemp 建的是 X，真正用的是 X.后缀，
# 后者没经过 mktemp——既没有原子性，X 本身也没人删。
# 本文件要排除：上面这段解释里就写着被查的那两种构造。
MKBAD=0
while IFS= read -r p; do
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    while IFS= read -r hit; do
        ln=${hit%%:*}; line=${hit#*:}
        printf '%s' "$line" | grep -qE '^[[:space:]]*#' && continue
        echo "       ${p#"$PROJECT_DIR"/}:${ln}  $(printf '%s' "$line" | cut -c1-110)"
        MKBAD=$((MKBAD+1))
    done < <(grep -nE 'mktemp([[:space:]]+-[a-su-z]+)*[[:space:]]+-[a-z]*t([[:space:]]|$)|\$\(mktemp[^)]*\)\.' "$p" || true)
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
if [ "$MKBAD" -eq 0 ]; then
    good "mktemp 都写了完整模板，没有用两个平台语义不同的 -t"
else
    bad "上列 $MKBAD 处 mktemp 用了 -t 或给结果又拼了后缀——写成 mktemp \"\${TMPDIR:-/tmp}/名字.XXXXXX\""
fi

echo
info "=== 二十九、给了内联阈值就必须认得 \"不适用\" 这个回答 ==="
# 2026-09-18 实测：四系统冒烟跑完了 baseline / nezha-nogc / nezha 六格，
# 在 nezha-avp 第一格上停住，理由是
#     丢 key 检查没有回执——拿不到判据不等于通过
# 但工具其实**回答了**：它看过配置，说"value 64B 小于内联阈值 512B，小值不在 valuelog 里"。
# 驱动只按 `丢失 <数字>` 取值，取不到就记 NA，于是这个结论掉进了"没回执"那一档。
#
# 这是一类：工具有三种回答（数字 / 不适用 / 没回执），前两种是结论，第三种不是，
# 而只认第一种的调用方会把第二种误判成第三种。第二十六节防的是反过来那一半
# （把"没回执"当成通过），两条合起来才完整。
#
# 判据：给 lost-keys.py 传了**可能非零**的内联阈值的脚本，必须 grep LOST_KEYS_VERDICT。
# 传字面 0 的调用不受约束——那种调用永远触发不了"不适用"。
# 本文件要排除：上面这段解释里就写着那个标记。
VERBAD=0
while IFS= read -r p; do
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    grep -q 'lost-keys.py' "$p" || continue
    # 第四个位置参数是内联阈值。写成变量（`$INLINE_TH`）才可能非零；
    # 写字面 0 的调用永远触发不了"不适用"，不用管。
    # **前面那个 `python3` 是必须的**：不带它的话，`good "lost-keys.py 报出 $BROKEN ... $LK ..."`
    # 这种**只是提到工具名字的消息串**也会被算成一次调用（第一版就误报了 gate-audit.sh）。
    nonzero=$(grep -cE 'python3[^|]*lost-keys\.py +[^ ]+ +[^ ]+ +[^ ]+ +\$' "$p" || true)
    [ "$nonzero" -eq 0 ] && continue
    grep -q 'LOST_KEYS_VERDICT' "$p" && continue
    echo "       ${p#"$PROJECT_DIR"/}  传了变量内联阈值却没有认 LOST_KEYS_VERDICT"
    VERBAD=$((VERBAD+1))
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
# 工具那一侧也要还在：标记没了，上面每一处的判定都会静默退回成 NA。
if ! grep -q 'LOST_KEYS_VERDICT=not_applicable' "$PROJECT_DIR/scripts/bench/lost-keys.py"; then
    echo "       scripts/bench/lost-keys.py 不再打印 LOST_KEYS_VERDICT——调用方的判定会静默退回成 NA"
    VERBAD=$((VERBAD+1))
fi
if [ "$VERBAD" -eq 0 ]; then
    good '给了内联阈值的调用都认得「不适用」这个回答'
else
    bad "上列 $VERBAD 处会把「不适用」误判成「没回执」——健康的系统会被判死"
fi

echo
info "=== 三十、GC 的 GET 路径里不许有裸 go func ==="
# 2026-09-18 实测：nezha-avp 256B 那一格 node0 panic 退出（随后触发了一次重新选举）：
#     grocksdb.(*DB).Get(0x0, ...)                     ← DB 句柄是 nil
#       raft.(*Persister).Get_opt(0xc029fadf20, ...)    ← Persister 本身还在
#         kvstore.(*KVServer).anotherGCGet.func3()      read.go:677
#
# 成因：GC 的多路查找把三路并行发出去，按优先级收结果，**第一路答出 value 就 return**。
# `defer kvs.storeRetireMu.RUnlock()` 跟着这次返回执行，剩下的 goroutine 就跑到锁外面
# 还在读；回收路径随后拿到写锁、Close() 掉 RocksDB，孤儿 goroutine 解引用到 nil。
# 这与 storeRetireMu / stateMu 当初修的"长读者钉住锁"正好是相反的一面，所以那两把锁
# 都挡不住——读锁取得对、放得也对，只是没活够久。
#
# 单测（TestSpawnedReaderKeepsStoreAlive）钉的是**租约机制本身**对不对；
# 它管不到"read.go 有没有在用这个机制"——把 st.spawn 改回 go func，单测照样全过。
# 那是一个静态性质，所以在这里查。
#
# 判据：firstGCGet / anotherGCGet 两个函数体内不许出现 `go func(`。
# 扫描路径不在判据内：那几处每个派生点都配了 wg.Wait()，调用方不会先返回。
GOBAD=0
RG="$PROJECT_DIR/internal/kvstore/read.go"
if [ ! -f "$RG" ]; then
    warn "找不到 internal/kvstore/read.go，这一节无从检查"
else
    for fn in firstGCGet anotherGCGet; do
        # 函数体 = 从 `func (kvs *KVServer) <fn>(` 那行到下一个顶层 `func ` 之前。
        body=$(awk -v f="func (kvs \*KVServer) $fn(" '
            index($0, f)==1 {inb=1; next}
            inb && /^func /{exit}
            inb {print NR": "$0}' "$RG")
        if [ -z "$body" ]; then
            warn "read.go 里找不到 ${fn}，判据可能已经过时"
            continue
        fi
        while IFS= read -r hit; do
            [ -z "$hit" ] && continue
            echo "       internal/kvstore/read.go:${hit%%:*}  $fn 里有裸 go func——它会跑到读锁外面"
            GOBAD=$((GOBAD+1))
        done < <(printf '%s\n' "$body" | grep 'go func(' || true)
    done
    # 租约那一侧也要还在：spawn 没了，上面每一处 st.spawn 都会编译不过，
    # 但 beginRead/endRead 被换回裸 RLock 则是静默的。
    # **必须用 -F（定串）。** 这几个签名里带圆括号，而 ERE 把 `(...)` 读成分组：
    # `func (kvs \*KVServer) beginRead()` 在 -E 下匹配的是"func kvs *KVServer beginRead"
    # （括号被当成分组吃掉了），文件里当然没有这个，于是干净的仓库也被报三条——
    # 第一版就是这样，判据自己把好代码判死了。
    for sym in 'func (kvs *KVServer) beginRead()' 'func (st stateSnapshot) spawn(' 'func (st stateSnapshot) endRead()'; do
        grep -qF "$sym" "$PROJECT_DIR/internal/kvstore/storelease.go" 2>/dev/null && continue
        echo "       internal/kvstore/storelease.go 里没有 ${sym}——租约机制被拆了"
        GOBAD=$((GOBAD+1))
    done
    if grep -qE 'kvs\.storeRetireMu\.RLock\(\)' "$PROJECT_DIR/internal/kvstore/service.go" 2>/dev/null; then
        echo "       internal/kvstore/service.go 又直接取 storeRetireMu.RLock() 了——读锁会随调用返回一起放掉"
        GOBAD=$((GOBAD+1))
    fi
fi
if [ "$GOBAD" -eq 0 ]; then
    good "GC 的 GET 路径全部走 st.spawn，读锁活到最后一个读者结束"
else
    bad "上列 $GOBAD 处会让读者跑到读锁外面——回收路径会在它读到一半时 Close() 掉库"
fi

echo
info "=== 三十一、关于 GC 的判据必须先过 has_gc ==="
# 这一类已经咬了五次，每次都是"判据对一个健康的系统报警/判死"：
#   1. "GC 一轮都没跑" 那条判据把 baseline 与 nezha-nogc 判死（gc_done 恒为 0 是设计）
#   2. "等 GC 轮数稳定" 的循环条件要求 ≥ 1，对这两个系统永远不成立 → 每格空转 20 分钟
#   3. -partitionTargetMB 传给 baseline，那个二进制不认识这个 flag，直接退出
#   4. lost-keys.py 对 baseline 一条都找不到，会把健康系统报成丢了全部
#   5. 2026-09-18 smoke4e："混合期间写了 62MB 却一轮 GC 都没推进——值得查"，
#      在 baseline 与 nezha-nogc 的四格各报一条。写入量那道门槛只挡住"写得不够多"，
#      挡不住"根本没有 GC 这回事"。
#
# 判据：scripts/multinode 下任何提到 GC 的 warn，往上 20 行内必须出现 has_gc。
# 20 行是按现有代码里 if/elif 链的长度定的；写得比这更远就该自己重构，而不是放宽判据。
GCGUARD=0
while IFS= read -r p; do
    case "$(basename "$p")" in gate-audit-multinode.sh) continue;; esac
    grep -q 'has_gc' "$p" || continue   # 没有多系统概念的脚本不受约束
    while IFS= read -r hit; do
        ln=${hit%%:*}
        lo=$((ln-20)); [ "$lo" -lt 1 ] && lo=1
        # **找守卫时必须先把注释行剔掉。** 不剔的话，写在这条 warn 上面的那段
        # 成因说明里就有 "has_gc" 三个字，判据于是永远认为守卫在——第一版就是这样，
        # 把守卫改成 `if false` 也照样判过。判据自己被自己的文档骗了。
        sed -n "${lo},${ln}p" "$p" | grep -v '^[[:space:]]*#' | grep -q 'has_gc' && continue
        echo "       ${p#"$PROJECT_DIR"/}:${ln}  提到 GC 的 warn 上游 20 行内没有 has_gc"
        printf '           %s\n' "$(printf '%s' "${hit#*:}" | cut -c1-96)"
        GCGUARD=$((GCGUARD+1))
    done < <(grep -nE '^[[:space:]]*warn "' "$p" | grep -E 'GC|gc_' || true)
done < <(find "$PROJECT_DIR/scripts" -name '*.sh' | sort)
if [ "$GCGUARD" -eq 0 ]; then
    good "关于 GC 的判据都先过了 has_gc，不会对没有 GC 的系统报警"
else
    bad "上列 $GCGUARD 处会对 baseline / nezha-nogc 报 GC 的警——健康系统被报成「值得查」"
fi

echo
info "=== 三十二、派生的闸门阈值不得超过本格条数 ==="
# 跨节点落差阈值取 `max(条数×LAG_PCT%, LAG_FLOOR)`。LAG_FLOOR 是个常数，按"别让极小的
# 格子被百分比压出噪声"定的；可是**大 value 档的总条数会小于这个常数**——
# 10GB ÷ 256KB 只有 40955 条，而下限 50000 比总条数还大。落差不可能达到阈值，
# 这道闸门在整档里恒不触发，却照样在日志里打出阈值、看起来在查。
# 这与第 25、26 节同一类：判据自己失效，而失效本身没有任何迹象。
#
# 这里**不是**比对字符串（那只是给当前写法拍张照，换个等价写法就误报）。
# 做法是把 maintable3.sh 里那段派生原文抠出来直接执行，喂进六档真实条数，
# 断言 LAG_LIMIT 始终小于本格条数。去掉那个上限，256KB 档立刻判出来。
MT3="$PROJECT_DIR/scripts/multinode/maintable3.sh"
DERIV=$(awk '/^    if \[ -n "\$LAG_ENTRIES" \]; then$/{f=1} f{print} f&&/^    fi$/{exit}' "$MT3")
LAGBAD=0
if [ -z "$DERIV" ]; then
    echo "       抠不出 LAG_LIMIT 的派生段——maintable3.sh 的写法变了，这一节没在审任何东西"
    LAGBAD=$((LAGBAD+1))
else
    # 六档 = 64B / 256B / 1KB / 4KB / 16KB / 256KB 在 10GB 下的条数，外加一个极小格。
    for n in 114227853 37543420 10187303 2602379 654162 40955 8000; do
        out=$(
            set +e
            export n
            LAG_ENTRIES="" LAG_PCT="${LAG_PCT:-25}" LAG_FLOOR="${LAG_FLOOR:-50000}" LAG_LIMIT=""
            eval "$DERIV"
            echo "$LAG_LIMIT"
        )
        case "$out" in ''|*[!0-9]*) echo "       n=$n 派生出的阈值不是数字：'$out'"; LAGBAD=$((LAGBAD+1)); continue;; esac
        if [ "$out" -ge "$n" ]; then
            echo "       n=${n} 派生出阈值 ${out} ≥ 条数——这一档的落差闸门恒不触发"
            LAGBAD=$((LAGBAD+1))
        fi
    done
fi
# 第二道：运行期还要有个兜底，因为 LAG_ENTRIES 是显式覆盖，上面那段管不到它。
# 找守卫时先把注释剔掉（第 31 节的教训：成因说明里的字会把判据自己骗过去）。
if ! grep -v '^[[:space:]]*#' "$MT3" | grep -qF 'LAG_LIMIT" -lt "$n"'; then
    echo "       maintable3.sh 里没有「阈值 < 本格条数」的运行期兜底——LAG_ENTRIES 写错了不会有人知道"
    LAGBAD=$((LAGBAD+1))
fi
if [ "$LAGBAD" -eq 0 ]; then
    good "落差阈值在六档 value 下都小于本格条数，运行期另有兜底"
else
    bad "上列 $LAGBAD 处会让落差闸门在某些档位恒不触发——它不报错，只是什么都不查"
fi

echo
info "=== 三十三、点读路径上一个库只许查一次 ==="
# 2026-09-19 的 10GB 实测查出来的缺陷：StartGet 先调 GetInline 问一次"是不是内联"，
# 不是就回落到多路查找里用 Get_opt 再查一次拿偏移——**两次都是完整的 db.Get**。
# 于是开了 -inlinePlacement 而 value 大于内联阈值时，每一次点读都白查一遍：
# 1KB / 4KB 两档的点读吞吐因此低 9.0% / 8.5%，16KB / 256KB 归零（固定开销被摊薄）。
#
# 它不报错、不影响正确性，只是慢——**没有任何判据会发现它**，是靠"两个本该一样的
# 系统数字对不上"才追出来的。所以把修完之后的形状钉住：
#   read.go     一律走 lookupValue（内部是 GetRecord，一次查找同时分流）
#   service.go  不许再有预查
# Get_opt 只回答偏移，拿到内联记录会返回错误，所以用它就必然要先问一次——
# 判据钉的是"读路径不许再出现 Get_opt"，而不是某种写法。
DBLLOOK=0
if [ ! -f "$PROJECT_DIR/internal/kvstore/read.go" ]; then
    echo "       找不到 internal/kvstore/read.go——这一节没在审任何东西"
    DBLLOOK=$((DBLLOOK+1))
else
    # 剔注释再查（第 31 节的教训：成因说明里的字会把判据自己骗过去）。
    if grep -v '^[[:space:]]*//' "$PROJECT_DIR/internal/kvstore/read.go" | grep -qF 'Get_opt('; then
        echo "       internal/kvstore/read.go 又出现 Get_opt——它答不了内联，调用方必然要多查一次"
        DBLLOOK=$((DBLLOOK+1))
    fi
    if grep -v '^[[:space:]]*//' "$PROJECT_DIR/internal/kvstore/service.go" | grep -qF 'GetInline('; then
        echo "       internal/kvstore/service.go 又出现 GetInline 预查——落空的那次是白查的"
        DBLLOOK=$((DBLLOOK+1))
    fi
    # 正向：修法本身还在。只钉不许出现什么、不钉必须出现什么的话，
    # 把 lookupValue 整个删掉也能判过。
    if ! grep -qF 'func (kvs *KVServer) lookupValue(' "$PROJECT_DIR/internal/kvstore/read.go"; then
        echo "       internal/kvstore/read.go 里没有 lookupValue——一次查找完成分流的那一层被拆了"
        DBLLOOK=$((DBLLOOK+1))
    fi
    if ! grep -qF 'func (p *Persister) GetRecord(' "$PROJECT_DIR/internal/raft/persister.go"; then
        echo "       internal/raft/persister.go 里没有 GetRecord——分流所需的那一次查找没了"
        DBLLOOK=$((DBLLOOK+1))
    fi
fi
if [ "$DBLLOOK" -eq 0 ]; then
    good "点读路径每个库只查一次，分流由记录首字节在同一次查找里完成"
else
    bad "上列 $DBLLOOK 处会让点读回到双查找——它不报错，只是每次读都白查一遍"
fi

echo
info "=== 三十四、apply 批量落库之后才能唤醒客户端 ==="
# applyBatch 把一批的行攒成一个 WriteBatch 落库，然后才 close(opCtx.committed)。
# **顺序反了就是一个静默的读己之写破坏**：客户端拿到 OK 之后的读走 leader 本地的库
# （租约读），若唤醒发生在落库之前，它会读不到自己刚写的值——而且不报任何错。
#
# 这条只能按**行序**钉：运行期测先后是不确定的（applyBatch 是同步的，
# 两件事都会发生，测不出谁先），所以判据是最后一次 flushRows() 必须出现在
# close( 之前。剔注释再比，否则这段说明自己就会被当成代码（第 31 节的教训）。
WAKEORD=0
AF="$PROJECT_DIR/internal/kvstore/apply.go"
if [ ! -f "$AF" ]; then
    echo "       找不到 internal/kvstore/apply.go——这一节没在审任何东西"
    WAKEORD=$((WAKEORD+1))
else
    NOCOMMENT=$(mktemp "${TMPDIR:-/tmp}/apply-nocomment.XXXXXX")
    grep -vE '^[[:space:]]*//' "$AF" > "$NOCOMMENT"
    # **只认调用，不认定义。** 第一版 grep 的是裸的 flushApplyRows(，而函数定义
    # 排在 close() 之后，tail -1 抓到的是定义行——于是正确的代码也被判失败。
    # 调用点一律带 kvs. 前缀，定义行是 `func (kvs *KVServer) flushApplyRows(`，没有。
    LASTFLUSH=$(grep -n 'kvs\.flushApplyRows(' "$NOCOMMENT" | tail -1 | cut -d: -f1)
    FIRSTCLOSE=$(grep -n 'close(c\.committed)\|close(opCtx\.committed)' "$NOCOMMENT" | head -1 | cut -d: -f1)
    rm -f "$NOCOMMENT"
    if [ -z "$LASTFLUSH" ]; then
        echo "       apply.go 里没有 flushApplyRows()——攒行落库那一层被拆了"
        WAKEORD=$((WAKEORD+1))
    elif [ -z "$FIRSTCLOSE" ]; then
        echo "       apply.go 里没有 close(...committed)——等在写上的客户端不会被唤醒"
        WAKEORD=$((WAKEORD+1))
    elif [ "$LASTFLUSH" -ge "$FIRSTCLOSE" ]; then
        echo "       唤醒（第 $FIRSTCLOSE 行）排在最后一次落库（第 $LASTFLUSH 行）之前"
        echo "           客户端会在数据落库前拿到 OK，随后的租约读读不到自己刚写的值"
        WAKEORD=$((WAKEORD+1))
    fi
fi
if [ "$WAKEORD" -eq 0 ]; then
    good "apply 的唤醒排在落库之后，读己之写成立"
else
    bad "上列 $WAKEORD 处会让客户端在数据落库前拿到 OK——不报错，只是偶尔读到旧值"
fi

echo
if [ "$FAILED" -eq 0 ]; then
    good "自审通过：注入的每一种故障都被判出来了，良性行一条都没被误判"
else
    echo -e "${RED}[FAIL]${NC} $FAILED 项未通过"
    exit 1
fi
