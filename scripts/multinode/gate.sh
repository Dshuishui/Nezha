#!/bin/bash
# 多节点驱动脚本共用的判定。source 进来用，不单独执行。
#
# 为什么要抽出来：同一条判定此前在 failover.sh、recover.sh、lsmraft.sh 里各写了一遍
#     echo "$rep" | grep -qE 'races=0 err_lines=0' || fail=1
# 三份拷贝各自漂移，而它有两个毛病：
#
#  1. **不看节点还活着没有。** REPORT 行里本来就有 alive=，三份拷贝一个都没用它。
#     于是一个**静默死掉**的节点照样判过——而"静默死掉"正是这次快照改造要防的那种
#     失效（leader 内存被慢 follower 钉住，最后被 OOM 杀掉，日志里一行解释都没有）。
#  2. **子串匹配、且要求两个字段相邻。** `err_lines=0` 会前缀匹配 `err_lines=01`；
#     更现实的是在两个字段之间插一个新字段，整条判定就变成**永久假失败**——
#     而假失败和漏报一样糟，它会训练人去忽略这个闸门。
#
# two-node-rounds.sh 里的写法是对的（按字段取值再精确比较），这里把它变成共用的一份。

# gate_field <REPORT 行> <字段名>
# 按字段名取值。逐 token 比对而不是正则匹配：REPORT 行本来就是空格分隔的
# `名=值`，按 token 取值**在构造上**就不可能前缀匹配（`err_lines=0` 不会命中
# `err_lines=03`），也不依赖 `\<` 这种 GNU 扩展——BSD grep 上它不一定管用，
# 而这几个脚本既在 Linux 服务器上跑，也在 mac 上被本机自审调用。
gate_field() {
    local tok
    for tok in $1; do
        case "$tok" in
            "$2="*) printf '%s' "${tok#*=}"; return 0;;
        esac
    done
    return 1
}

# gate_report_ok <REPORT 行> <期望 alive：yes|no|any> <标签>
# 合格条件：alive 符合预期、races=0、err_lines=0。
# 空行（SSH 超时或节点脚本自己挂了）一律不合格——拿不到判据不等于判据通过。
# 返回 0 = 合格；不合格时把原因打出来。
gate_report_ok() {
    local rep=$1 want_alive=$2 label=$3
    if [ -z "$rep" ]; then
        echo "    [闸门] $label: 拿不到 REPORT（SSH 超时或节点脚本失败）——不算通过"
        return 1
    fi
    local alive races err
    alive=$(gate_field "$rep" alive)
    races=$(gate_field "$rep" races)
    err=$(gate_field "$rep" err_lines)
    local bad=0
    if [ "$want_alive" != any ] && [ "$alive" != "$want_alive" ]; then
        echo "    [闸门] $label: alive=${alive}，期望 ${want_alive}（节点在本轮结束时的存活状态不对）"
        bad=1
    fi
    if [ "$races" != 0 ]; then
        echo "    [闸门] $label: races=$races"
        bad=1
    fi
    if [ "$err" != 0 ]; then
        echo "    [闸门] $label: err_lines=$err"
        bad=1
    fi
    # 字段本身缺失也算不合格：REPORT 的格式变了而判定没跟上，同样是判据失效。
    if [ -z "$alive" ] || [ -z "$races" ] || [ -z "$err" ]; then
        echo "    [闸门] $label: REPORT 行里缺字段（alive=$alive races=$races err_lines=${err}）"
        bad=1
    fi
    return $bad
}

# gate_read_ok <readonly 的输出> <标签>
# "直读某个指定节点"的判定。三种结局必须分开，混成一句"读失败"会让最常见的那种
# 误配变得根本查不出来：
#
#   FAILOVER_VERIFY_OK        通过
#   目标节点未持有 leader     **脚本自己配错了**，与数据无关
#   其它                      真的数据不对
#
# 中间那种的来历：`-leaderCheck` 自 811ac32（2026-09-13）起默认**开**，follower 上的读
# 一律回 ErrWrongLeader 并附带 leader 提示；而客户端的 -servers 只给一个地址时，
# redirect 无处可去（client.go 的 redirect：hint 越界或等于当前目标就退回原节点），
# 于是每 10ms 重试同一个节点直到工具自己的上限。要直读某个节点的**本地**状态，
# 那个节点必须用 `-leaderCheck=false` 起——snapshot-e2e.sh 里有现成写法和理由。
#
# 这条判定是 2026-09-16 补的，起因是一个三天没人发现的失效：811ac32 之后
# recover / failover / lsmraft / two-node-rounds 四个驱动脚本的直读步骤**恒判失败**，
# 而 f4e10d1 那次闸门审计没抓到——那次只往里注故障、查"闸门会不会判失败"
# （**灵敏度**，不漏报），从未查过"健康的一轮会不会判通过"（**特异性**，不假失败）。
# 一个恒判失败的闸门在只查灵敏度的审计下每次都合格。
gate_read_ok() {
    local out=$1 label=$2
    if [ -z "$out" ]; then
        echo "    [闸门] $label: 直读没有任何输出（SSH 超时或工具没起来）——拿不到判据不等于通过"
        return 1
    fi
    case "$out" in
        *FAILOVER_VERIFY_OK*) return 0;;
        *"未持有 leader 身份"*)
            echo "    [闸门] $label: 目标节点把读挡回了（ErrWrongLeader）。"
            echo "           这是**脚本配置问题**、不是数据问题：要直读一个节点的本地状态，"
            echo "           它必须用 -leaderCheck=false 起（见本文件上方注释）。"
            return 1;;
    esac
    echo "    [闸门] $label: 直读的数据不对"
    return 1
}




# with_timeout / require_out —— 让 ssh 的失败**响**起来。
#
# 2026-09-16 实测的第十个静默失效：本机内存压到只剩 0.1GB、压缩 13.7GB 时，本地的 ssh
# 进程被饿住——**连接并没有断**，所以 ConnectTimeout 和 ServerAlive* 一个都不触发，只是
# 慢到几十分钟。于是 `r` 静默返回空串，而脚本照着空串往下走：
#   restart 的回执丢了（节点其实根本没重启，数据目录里连 n1.log 都没有）
#   kill9 的回执丢了、leader_wins 返回空（"40s 内没有新 leader 当选"）
#   最后 node2 判 alive=no，整轮跑了 1 小时 40 分才失败，而原因显示成"数据不对"
# 本该在第一个空回执处就失败。拿不到判据不等于判据通过——这条规则 gate_report_ok 和
# gate_read_ok 都写着，但 `r` 自己不遵守。
#
# 超时要便携：**mac 没有 timeout(1)**（那是 coreutils 的，要装才有 gtimeout），
# 而 perl 在 mac 和这几台 Linux 上都是自带的，`alarm` + `exec` 就是一个干净的超时。
with_timeout() { local s=$1; shift
  if command -v timeout >/dev/null 2>&1; then timeout "$s" "$@"
  elif command -v gtimeout >/dev/null 2>&1; then gtimeout "$s" "$@"
  else perl -e 'alarm shift; exec @ARGV' "$s" "$@"; fi; }

# require_out <输出> <标签> —— 需要回执的调用拿到空串时判失败并说明。
# 返回 0 = 有输出。调用方负责 fail=1（这里不直接改 fail，因为它常在 $() 里被调用，
# 子 shell 里改了外面看不见——这正是 2026-09-16 另一处踩过的坑）。
require_out() {
    [ -n "$1" ] && return 0
    echo "    [闸门] $2: 没有回执（ssh 超时/被饿住，或远端脚本失败）——拿不到判据不等于通过"
    return 1
}

# ============================================================================
# 拓扑与传输
# ============================================================================
#
# 两件事集中在这里，因为它们此前散在每个驱动脚本里写死，改一处就漏一处：
#
# **拓扑**。`TOPO=two`（默认）是历史拓扑：node0 在 tikv240，node1 与 node2 **同在
# tikv241**（241 上跑两个节点）。所有历史三节点实验都是这个形状，所以它是默认值——
# 换拓扑会让新数据与历史数据不可比。`TOPO=three` 是三台各一个，node2 落在 node55，
# 复制/选举/快照因此真正跨三台机器。
# node55 硬件不同（40 核 / 128GB，另两台 96 核 / 251GB），所以 TOPO=three **只用于
# 正确性验证**，性能数字仍然只能取 TOPO=two 那一组。
#
# **传输**。驱动脚本既可能在 Mac 上跑，也可能在实验机上跑，而两种情况的 ssh 目标写法
# 不同：Mac 有 ~/.ssh/config 里的别名，实验机上那些别名**解析不了**（实测
# "Could not resolve hostname tikv241"），要用 用户名@IP。更麻烦的是 tikv240 到
# **自己**也不通（它的公钥不在自己的 authorized_keys 里），所以指向本机的调用必须
# 本地执行、不能绕 ssh。这两点由 ssh_target 与 is_self 处理，调用方一律写 `r <别名> "命令"`。
#
# 为什么要能在实验机上跑：2026-09-16 有两轮验证毁在 Mac 的内存上（空闲 0.5GB、压缩
# 13.7GB），本地 ssh 进程被饿住但**连接没断**，于是回执静默变空。把驱动放到实验机上
# 就与本机状况无关了。

TOPO=${TOPO:-two}
case $TOPO in
  two)   TOPO_HOST=(tikv240 tikv241 tikv241)
         TOPO_IP=(192.168.1.240 192.168.1.241 192.168.1.241)
         TOPO_PORT=(3099 3099 3100); TOPO_IPORT=(30991 30991 30992) ;;
  three) TOPO_HOST=(tikv240 tikv241 node55)
         TOPO_IP=(192.168.1.240 192.168.1.241 192.168.1.55)
         TOPO_PORT=(3099 3099 3099); TOPO_IPORT=(30991 30991 30991) ;;
  *) echo "未知的 TOPO=${TOPO}（只认 two | three）" >&2; exit 1 ;;
esac

host_of()  { echo "${TOPO_HOST[$1]}"; }
ip_of()    { echo "${TOPO_IP[$1]}"; }
addr_of()  { echo "${TOPO_IP[$1]}:${TOPO_PORT[$1]}"; }
port_of()  { echo "${TOPO_PORT[$1]} ${TOPO_IPORT[$1]}"; }   # 供 three-node.sh start 用
# 节点之间的内部地址表，要与 three-node.sh 里的 -peers 完全一致。
peers_str()   { local i o=""; for i in 0 1 2; do o="$o,${TOPO_IP[$i]}:${TOPO_IPORT[$i]}"; done; echo "${o#,}"; }
# 客户端用的对外地址表。
servers_str() { local i o=""; for i in 0 1 2; do o="$o,$(addr_of "$i")"; done; echo "${o#,}"; }

# 别名 -> 用户名@IP。用户名在 240 是 Zg.xin、241/55 是 zx（CLAUDE.md 记着这一点）。
host_user_ip() { case $1 in
  tikv240) echo "Zg.xin@192.168.1.240";; tikv241) echo "zx@192.168.1.241";;
  node55)  echo "zx@192.168.1.55";;      *) echo "$1";; esac; }
host_bare_ip() { case $1 in
  tikv240) echo "192.168.1.240";; tikv241) echo "192.168.1.241";;
  node55)  echo "192.168.1.55";;  *) echo "";; esac; }

# 本机的所有 IP。`hostname -I` 只在 Linux 上有；Mac 上取不到就是空串，于是 is_self
# 恒为假、所有调用都走 ssh——正是在 Mac 上想要的行为。
SELF_IPS=" $(hostname -I 2>/dev/null) "
is_self() { local target; target=$(host_bare_ip "$1"); [ -n "$target" ] || return 1
            case "$SELF_IPS" in *" $target "*) return 0;; esac; return 1; }
ON_SERVER=0; case "$SELF_IPS" in *" 192.168.1."*) ON_SERVER=1;; esac
ssh_target() { if [ "$ON_SERVER" = 1 ]; then host_user_ip "$1"; else echo "$1"; fi; }

# r / rq —— 唯一的远端执行入口。指向本机就本地跑，否则走 ssh；两者都带**总超时**。
# SSH_TIMEOUT 默认 600s（写 2 万条 × 1KB 正常约 100s，压力下见过 12 分钟），
# rq 给控制类命令用，90s 足够，超了就是真出问题了。
_SSHOPT="-o ConnectTimeout=20 -o ServerAliveInterval=15 -o ServerAliveCountMax=3 -o BatchMode=yes -o StrictHostKeyChecking=accept-new"
r()  { local h=$1; shift; if is_self "$h"; then with_timeout "${SSH_TIMEOUT:-600}" bash -c "$*" 2>/dev/null
       else with_timeout "${SSH_TIMEOUT:-600}" ssh $_SSHOPT "$(ssh_target "$h")" "$@" 2>/dev/null; fi; }
rq() { local h=$1; shift; if is_self "$h"; then with_timeout "${SSH_TIMEOUT_Q:-90}" bash -c "$*" 2>/dev/null
       else with_timeout "${SSH_TIMEOUT_Q:-90}" ssh $_SSHOPT "$(ssh_target "$h")" "$@" 2>/dev/null; fi; }

# cleanup_nodes —— 停掉**我们自己**的三个节点。两头都要调（挂 EXIT + 启动前再清一遍）：
# trap 覆盖不了 SIGKILL，2026-09-16 有一轮被系统因内存不足杀掉，残留把下一轮的启动顶掉。
# 只碰 node 0/1/2，靠 three-node.sh 的 pid 文件与 `-data ~/work/three-N` 匹配，
# **碰不到别人的实验**——这三台机器是共用的，这是硬约束。
cleanup_nodes() { local i; for i in 0 1 2; do
  rq "$(host_of "$i")" "~/three-node.sh stop $i" >/dev/null 2>&1 || true; done; }

# leader_wins / wait_new_leader —— 杀掉 leader 之后等新 leader 当选。
#
# **不许用固定 sleep。** minElectionTimeout=10s + jitter=1s 只是"察觉 leader 没了"的
# 时间，还要加投票往返、可能的分票重选，-race 构建又整体更慢：2026-09-16 实测过一次
# **18 秒**才选出来。所以原来的 `sleep 10` 必错，`sleep 20` 也只是勉强够。
# 更根本的是，固定 sleep 把**测试脚本的常量**钉在**源码里的常量**上——
# minElectionTimeout 从 3s 改到 10s（811ac32）时没人改脚本，判据就静默失效了。
#
# 判据是"当选次数**增加**"而不是"日志里出现过 Candidate -> Leader"：初始那次选举的
# 记录一直留在日志里，按存在性判会立刻返回真，等于没等。被杀掉的那个节点日志不再变化，
# 所以把三个都数进来不影响"增加"这个判据，而且不必知道谁被杀了。
leader_wins() { local i n=0 c
  for i in 0 1 2; do
    c=$(rq "$(host_of "$i")" "cat ~/work/three-$i/n*.log 2>/dev/null | grep -c 'Candidate -> Leader'" | tr -d '\r')
    case "$c" in ''|*[!0-9]*) c=0;; esac
    n=$((n + c))
  done; echo "$n"; }
wait_new_leader() { # $1=杀之前记下的当选次数 $2=上限秒数（默认 60）
  local k cur lim=${2:-60}
  for k in $(seq 1 $((lim/3))); do
    sleep 3
    cur=$(leader_wins); cur=${cur:-0}
    if [ "$cur" -gt "$1" ] 2>/dev/null; then say "新 leader 已当选（等了 $((k*3))s，当选次数 $1 -> ${cur}）"; return 0; fi
  done
  say "${lim}s 内没有新 leader 当选（当选次数仍为 $1）"; fail=1; return 1; }
