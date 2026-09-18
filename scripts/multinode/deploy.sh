#!/bin/bash
# Ship the current branch to both experiment machines and prove it landed.
#
# A bundle carries prerequisites: `git bundle create f BASE..branch` can only be applied
# by a repo that already has BASE. Bundling from a local commit the servers never received
# makes `git fetch` fail with "Repository lacks these prerequisite commits", and if that
# error is discarded the following `git checkout FETCH_HEAD` silently re-checks-out the
# stale FETCH_HEAD from an earlier bundle. Three verification runs were spent on a binary
# three commits old that way, including one where a probe read 0 because it was not in the
# build at all. So: base the bundle on each server's own HEAD, and verify afterwards.
set -eu
cd "$(dirname "$0")/../.."
BRANCH=$(git rev-parse --abbrev-ref HEAD)
WANT=$(git rev-parse HEAD)

# **本脚本按 git 同步，所以未提交的改动到不了服务器。** 只有 three-node.sh 与
# rep-node.sh 是从工作树 scp 的，其余一切（gate.sh、各驱动、各 scripts/test/*.sh）
# 都靠 `git fetch` 加 `git checkout FETCH_HEAD` 落地——工作树里改了没提交，服务器上
# 拿到的仍是 HEAD 那一版，而且**部署会报 DEPLOY_OK**。
# 2026-09-16 因此白跑了两轮：一次是 gate.sh 的新函数在服务器上 "command not found"，
# 一次是新加的诊断一行都没打出来，让人以为是别的原因。
DIRTY=$(git status --porcelain -- ':!results' ':!notes' 2>/dev/null | grep -vE '^\?\?' || true)
if [ -n "$DIRTY" ]; then
  echo "DEPLOY_WARN 工作树有未提交的改动，**它们不会被部署**（本脚本按 git 同步）："
  echo "$DIRTY" | sed 's/^/    /'
  echo "    → 要让这些改动上服务器：先 git commit，再重新部署"
  [ "${ALLOW_DIRTY:-0}" = 1 ] || { echo "DEPLOY_REFUSED 先提交，或显式 ALLOW_DIRTY=1"; exit 1; }
  echo "    ALLOW_DIRTY=1，继续部署（服务器拿到的是 HEAD 那一版）"
fi
# 默认只有两台（历史拓扑）。TOPO=three 或 HOSTS 显式给了 node55 时才带上它。
# node55 硬件不同（40 核 / 128GB），只用于正确性验证，不出性能数字。
HOSTS=${HOSTS:-"tikv240 tikv241"}
[ "${TOPO:-two}" = three ] && HOSTS="tikv240 tikv241 node55"
BUILD=${BUILD:-1}   # BUILD=0 to skip rebuilding the node binaries
FORCE=${FORCE:-0}   # FORCE=1 to deploy anyway while something is running

# **先确认每台机器都连得上，再谈别的。**
#
# 下面那个进程守卫把 ssh 的输出捕进变量，于是 ssh 失败（rc=255）时 `set -eu` 立刻退出，
# 而失败命令的输出在变量里——**操作者一个字都看不到，只有一个 255**，看起来像脚本坏了。
# 2026-09-18 实测过一次：三台同时 "Connection timed out during banner exchange"。
#
# 即使不退出，空的 $running 也分不清两件事：**"没有进程在跑"（可以部署）** 与
# **"连不上这台机器"（什么都不知道）**。后者当成前者，就是带着未知状态往下走——
# 与 gate.sh 里那条"拿不到判据不等于判据通过"是同一件事。
#
# rc 要先存下来再用：写在 `if ! ssh ...; then` 的分支里取 $? 拿到的是**被 ! 取反后的 0**，
# 打出来是 "rc=0"，等于这行诊断在说假话（第一版就是这么写的）。
# 临时文件一律写全模板。`mktemp -t 前缀` 只有 BSD（macOS）认；**GNU 的 -t 要求模板
# 自带至少三个 X**，给了裸前缀就直接 `mktemp: too few X's in template`，脚本第 49 行就死。
# 本脚本一直只在 Mac 上跑，所以这个洞藏到 2026-09-18 才暴露——那天因为 Mac 到实验机的
# ssh 撑不过几秒，改成在 WSL（Linux）里发起部署，第一次跑就是这条报错。
# 顺带修掉 `$(mktemp ...).err` 这个写法：它让 mktemp 建的是 X，真正用的却是 X.err
# ——后者没经过 mktemp，既丢了原子性，X 本身也没人删。
CONNERR=$(mktemp "${TMPDIR:-/tmp}/deploy-ssh.XXXXXX")
trap 'rm -f "$CONNERR"' EXIT
for h in $HOSTS; do
  # **必须写成 `|| rc=$?`。** `cmd; rc=$?` 在 set -e 下是两条简单命令，第一条失败就
  # 立刻退出，`rc=$?` 与下面的诊断**一行都不会跑**——于是又变成"裸 255、零输出"，
  # 正是这段代码要消灭的症状。`||` 列表豁免 set -e，所以 rc 才拿得到。
  # 2026-09-18 实测：第一版这么写，部署仍然静默退出 255。
  rc=0
  ssh -o ConnectTimeout=20 -o BatchMode=yes "$h" true 2>"$CONNERR" || rc=$?
  [ "$rc" = 0 ] && continue
  echo "DEPLOY_FAIL $h: ssh 连不上（rc=${rc}）——**不是代码问题**，先查连通性"
  sed 's/^/    /' "$CONNERR" | head -3
  exit 1
done

# 部署到正在跑实验的机器上会污染那次实验：BUILD=1 会重建节点二进制，而覆盖工作树还会
# 换掉正在被 bash 执行的脚本（bash 按字节偏移惰性读取，换了文件等于从中途跳到别处）。
# 所以先看一眼有没有被测进程在跑，有就拒绝，除非显式 FORCE=1。
#
# **这些机器是共用的**：跑着的进程可能是别人的实验。所以这里一并报出**属主**与**已运行
# 时长**，好让人判断是不是自己的——不是自己的就别停，去问机器的其他使用者。
#
# 用 pgrep **不带 -f**：匹配进程名而非完整命令行。带 -f 会匹配到这条 ssh 命令自己的
# 命令行（今天踩了三次），也会匹配到命令行里恰好提到二进制名的 bash 包装进程。
#
# 模式是 'nezha' 而不是 'nezha-'：别人的节点二进制就叫 `nezha`，带横线的模式匹配不上，
# 于是守卫在三台机器都有实验在跑的时候一声不响地放行了（2026-09-13 实测）。另外
# /proc/<pid>/comm 只有 15 个字符，`nezha-three-normal` 在那里是 `nezha-three-nor`，
# 任何比它更长的固定模式同样会漏。
for h in $HOSTS; do
  running=$(ssh "$h" "pgrep 'nezha' 2>/dev/null | head -20 | xargs -r ps -o user=,pid=,etime=,args= -p 2>/dev/null | cut -c1-150" 2>/dev/null)
  [ -z "$running" ] && continue
  echo "DEPLOY_REFUSED $h: 有被测进程在跑，部署会污染它"
  printf '    %-10s %-8s %-10s %s\n' 属主 PID 已运行 命令
  echo "$running" | sed 's/^/    /'
  [ "$FORCE" = 1 ] || {
    echo "    → 是自己的实验：等它跑完，或 ssh $h '~/three-node.sh stop <IDX>' 停掉"
    echo "    → 不是自己的（属主不是你）：**不要停**，先问机器的其他使用者"
    echo "    → 确认无碍：FORCE=1 bash scripts/multinode/deploy.sh"
    exit 1
  }
  echo "    FORCE=1，继续部署"
done


# rread <主机> <说明> <远端命令> —— 取一个**短**回执，空串就重试。
#
# 今晚这台 Mac 到实验机的 ssh 会话活不过 3~10 秒（sleep 3 通、sleep 10 起 rc=255），
# 而且即使 rc=0 也可能什么都不返回。于是本文件里**五处**判据先后把这种抖动报成了
# 数据或代码问题："编译不过"、"cannot read remote HEAD"、"HEAD is ,"、"scp 失败"……
# 每一条都会把人引去查根本没问题的东西。统一成一个会重试、并且说得清"是连不上还是
# 真读不到"的入口。
#
# 只用于**短**命令：它靠"回执非空"判成功，长命令的中途输出会被它当成已经成功。
#
# 从 mac 发起时，链路（mac → winbox-wsl → jump_ncu_via_win → 实验机）空闲 5~8 秒
# 就断，`go build` 那 30~60 秒必然失败，只能 BUILD=0。**从 WSL 里发起就没这个问题**，
# BUILD=1 正常。所以不是"这个仓库只能 BUILD=0"，而是"从 mac 发起时只能 BUILD=0"——
# 正确做法是把部署本身挪到 WSL 上跑（winjob run），而不是绕开编译。
rread() {
    local host=$1 what=$2; shift 2
    local out="" rc=0 attempt
    for attempt in 1 2 3 4 5; do
        rc=0
        out=$(ssh -o ConnectTimeout=20 -o BatchMode=yes "$host" "$@" 2>/dev/null | tr -d '\r') || rc=$?
        [ -n "$out" ] && { printf '%s' "$out"; return 0; }
        sleep 3
    done
    echo "DEPLOY_FAIL ${host}: 「${what}」取了 5 次都是空回执（最后一次 rc=${rc}）——**ssh 抖动**，不是远端的问题" >&2
    return 1
}

rrun() { # rrun <主机> <说明> <远端命令>
  local host=$1 what=$2; shift 2
  local rc=0
  ssh "$host" "$@" || rc=$?   # `|| rc=$?` 而不是 `; rc=$?`，理由见连通性预检那段
  [ "$rc" = 0 ] && return 0
  if [ "$rc" = 255 ]; then
    echo "DEPLOY_FAIL ${host}: ssh 连接层失败（rc=255）于「${what}」——**不是代码问题**"
  else
    echo "DEPLOY_FAIL ${host}: ${what} 失败（rc=${rc}）"
  fi
  return 1
}
for h in $HOSTS; do
  # 读不到远端 HEAD 有两种完全不同的原因，原先混成同一句 "cannot read remote HEAD"：
  #   仓库真有问题（目录不在、不是 git 库）——要去查
  #   ssh 抖了一下（rc=255）——与仓库无关，重试即可
  # 2026-09-18 实测撞上后者，而消息指向前者。
  have=$(rread "$h" "读远端 HEAD" "cd ~/work/Nezha && git rev-parse HEAD") || exit 1
  # **服务器上的工作树脏了，要在开跑之前就说清楚，而不是让 checkout 在中途炸。**
  #
  # 2026-09-18 实测：tikv240 上 `scripts/bench/lost-keys.py` 被手工改过（在服务器上
  # 边查边改的同一处修复），于是 checkout 被 git 拒绝，部署停在
  #     error: Your local changes to the following files would be overwritten
  #     DEPLOY_FAIL tikv240: fetch/checkout
  # 这条消息没错，但它出现在**已经给这台机器传完 bundle 之后**，而且不说是哪些文件、
  # 也不说该怎么办；更要紧的是三台机器只处理到第一台就退出了。
  #
  # 这里不自动丢弃：`~/work/Nezha` 是共用账号下的目录，改动可能是别人留在那儿的，
  # 悄悄 checkout 掉就是删别人的东西。所以只报出来、让人看一眼，
  # 确认无用之后用 ALLOW_REMOTE_DIRTY=1 明确授权丢弃。
  # 末尾那个 __END__ 是必须的：rread 把**空回执当成 ssh 抖动**（重试 5 次再报失败），
  # 而"工作树干净"的正常回执恰恰就是空串。不加哨兵的话，每一次干净的部署都会
  # 在这里重试 15 秒然后 DEPLOY_FAIL——判据自己把好机器判死，正是这个文件里
  # 反复出现的那一类。
  rdirty=$(rread "$h" "查远端工作树" \
    "cd ~/work/Nezha && git status --porcelain -- ':!results' ':!notes' | grep -v '^??'; echo __END__") || exit 1
  rdirty=$(printf '%s' "${rdirty%__END__}" | sed '/^[[:space:]]*$/d')
  if [ -n "$rdirty" ]; then
    echo "DEPLOY_WARN $h 的工作树有未提交的改动，**checkout 会被 git 拒绝**："
    echo "$rdirty" | sed 's/^/    /'
    if [ "${ALLOW_REMOTE_DIRTY:-0}" = 1 ]; then
      echo "    ALLOW_REMOTE_DIRTY=1 —— 丢弃这些改动"
      rrun "$h" "丢弃远端改动" "cd ~/work/Nezha && git checkout -- ." || exit 1
    else
      echo "    → 先看一眼是不是别人的（这是共用账号）：ssh $h 'cd ~/work/Nezha && git diff'"
      echo "    → 确认可以丢弃再重跑：ALLOW_REMOTE_DIRTY=1 $0"
      echo "DEPLOY_REFUSED $h: 远端工作树不干净"
      exit 1
    fi
  fi
  if [ "$have" = "$WANT" ]; then
    echo "$h already at ${WANT:0:7}"
  else
    git cat-file -e "$have^{commit}" 2>/dev/null || { echo "DEPLOY_FAIL $h: remote HEAD $have unknown locally"; exit 1; }
    b=$(mktemp "${TMPDIR:-/tmp}/nezha-bundle.XXXXXX")   # 同上：裸前缀在 GNU mktemp 上不合法
    git bundle create "$b" "$have..$BRANCH" >/dev/null
    scp -q "$b" "$h:~/deploy.bundle"
    rm -f "$b"
    # No 2>/dev/null here: a fetch failure must be visible, and checkout must not run on a
    # stale FETCH_HEAD, hence the &&.
    ssh "$h" "cd ~/work/Nezha && git fetch ~/deploy.bundle $BRANCH && git checkout -B $BRANCH FETCH_HEAD" || {
      echo "DEPLOY_FAIL $h: fetch/checkout"; exit 1; }
  fi
  # 回读也要重试：一次 ssh 打嗝会让 $got 变成空串，而消息 "HEAD is , wanted ..."
  # 读起来像**仓库落错了 commit**——2026-09-18 实测，node55 明明已经在目标提交上。
  # 这是本文件里第四处"把连接层抖动报成数据/代码问题"，判据一律要能重试并说清区别。
  got=$(rread "$h" "回读 HEAD" "cd ~/work/Nezha && git rev-parse HEAD") || exit 1
  [ "$got" = "$WANT" ] || { echo "DEPLOY_FAIL ${h}: HEAD 是 ${got}，要的是 ${WANT}"; exit 1; }
  # **ssh 自己失败与"远端命令失败"必须分开报。**
  #
  # ssh 连接层出问题时退出码是 255，而远端命令的失败退出码是它自己的。原先两者都落进
  # 同一句 "DEPLOY_FAIL $h: build"，于是一次握手超时被报成**编译不过**——
  # 2026-09-18 实测连撞三次，每次失败在不同的随机步骤（节点二进制 / countkeys /
  # scp rep-node.sh），而每一步单独手工跑都是 rc=0。照那条消息去查代码是白查。
  # 255 这个码是 ssh 的约定：远端命令即便返回 255 也极少见，误判的代价远小于混报。
  if [ "$BUILD" = 1 ]; then
    rrun "$h" "编译节点二进制" "source ~/env.sh; cd ~/work/Nezha && go build -o /tmp/nezha-three-normal ./cmd/nezha/" || exit 1
    # **bench 工具也要跟着建。** 以前这里只建节点二进制，于是 tikv240 上的 scanverify /
    # readonly 一直停留在 2026-09-06——早于 eb8b5d0（key 宽到 24B）和 afbcf71（定长编码
    # 移到客户端）。旧工具按旧编码补齐 key，而节点现在原样存 key，**拿它校验会把每一条
    # 都报成丢失**，读起来正像快照弄坏了数据。2026-09-16 差点因此白跑一整轮。
    for t in scanverify readonly randwrite_goroutine countkeys; do
      rrun "$h" "编译 $t" "source ~/env.sh; cd ~/work/Nezha && go build -o /tmp/$t ./cmd/bench/$t/" || exit 1
    done
  fi
  # 两份节点脚本都要送。rep-node.sh 此前不在这里，于是对它的改动（比如把 gc_done 的
  # 计数从 '垃圾回收完成' 改成 '轮垃圾回收完成'）**根本到不了服务器**：本地改完、
  # 提交完、部署完，跑起来还是旧逻辑，而且不报错。
  # **判据是校验和，不是 scp 的退出码。**
  #
  # 2026-09-18 实测：scp 把文件完整送达（verbose 里 "Exit status 0"、字节数对得上），
  # 随后自己被 SIGHUP —— `Killed by signal 1`，于是退出码非零。40 次部署里绝大多数
  # 栽在这个**假失败**上，而文件的 sha256 与本地逐字节相同。
  # 反过来也要防：scp 报成功而文件被截断（本机内存压力下见过 ssh 被饿住），
  # 那时只看退出码同样发现不了。两个方向都由校验和覆盖，所以干脆只认它。
  for f in three-node.sh rep-node.sh; do
    want=$(shasum -a 256 "scripts/multinode/$f" 2>/dev/null | cut -d' ' -f1)
    [ -n "$want" ] || want=$(sha256sum "scripts/multinode/$f" | cut -d' ' -f1)
    ok=0
    for attempt in 1 2 3; do
      scp -q "scripts/multinode/$f" "$h:~/$f" >/dev/null 2>&1 || true
      got=$(rread "$h" "回读 $f 的校验和" "sha256sum ~/$f 2>/dev/null | cut -d' ' -f1" 2>/dev/null) || got=""
      [ "$got" = "$want" ] && { ok=1; break; }
      sleep 3
    done
    [ "$ok" = 1 ] || { echo "DEPLOY_FAIL ${h}: ${f} 传了 3 次校验和仍不符（想要 ${want:0:12} 拿到 ${got:0:12}）"; exit 1; }
  done
  # 驱动脚本现在也可以**在服务器上**执行（2026-09-16 起：从 Mac 上跑时本机内存压力会把
  # ssh 饿住，回执静默变空，两轮验证因此报废）。它们从工作树里跑，所以只要仓库到位就行；
  # 这里额外确认一下工作树里有 gate.sh，免得 checkout 漏了文件而在跑的时候才发现。
  ssh "$h" "[ -f ~/work/Nezha/scripts/multinode/gate.sh ]" || {
    echo "DEPLOY_FAIL $h: 工作树里没有 scripts/multinode/gate.sh"; exit 1; }
  echo "DEPLOY_OK $h ${WANT:0:7}"
done
