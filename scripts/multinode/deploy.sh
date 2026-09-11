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
HOSTS=${HOSTS:-"tikv240 tikv241"}
BUILD=${BUILD:-1}   # BUILD=0 to skip rebuilding the node binaries
FORCE=${FORCE:-0}   # FORCE=1 to deploy anyway while something is running

# 部署到正在跑实验的机器上会污染那次实验：BUILD=1 会重建节点二进制，而覆盖工作树还会
# 换掉正在被 bash 执行的脚本（bash 按字节偏移惰性读取，换了文件等于从中途跳到别处）。
# 所以先看一眼有没有被测进程在跑，有就拒绝，除非显式 FORCE=1。
#
# **这些机器是共用的**：跑着的进程可能是别人的实验。所以这里一并报出**属主**与**已运行
# 时长**，好让人判断是不是自己的——不是自己的就别停，去问机器的其他使用者。
#
# 用 pgrep **不带 -f**：匹配进程名而非完整命令行。带 -f 会匹配到这条 ssh 命令自己的
# 命令行（今天踩了三次），也会匹配到命令行里恰好提到二进制名的 bash 包装进程。
for h in $HOSTS; do
  running=$(ssh "$h" "pgrep 'nezha-' 2>/dev/null | head -20 | xargs -r ps -o user=,pid=,etime=,args= -p 2>/dev/null | cut -c1-150" 2>/dev/null)
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


for h in $HOSTS; do
  have=$(ssh "$h" "cd ~/work/Nezha && git rev-parse HEAD" 2>/dev/null | tr -d '\r')
  [ -n "$have" ] || { echo "DEPLOY_FAIL $h: cannot read remote HEAD"; exit 1; }
  if [ "$have" = "$WANT" ]; then
    echo "$h already at ${WANT:0:7}"
  else
    git cat-file -e "$have^{commit}" 2>/dev/null || { echo "DEPLOY_FAIL $h: remote HEAD $have unknown locally"; exit 1; }
    b=$(mktemp -t nezha-bundle).bundle
    git bundle create "$b" "$have..$BRANCH" >/dev/null
    scp -q "$b" "$h:~/deploy.bundle"
    rm -f "$b"
    # No 2>/dev/null here: a fetch failure must be visible, and checkout must not run on a
    # stale FETCH_HEAD, hence the &&.
    ssh "$h" "cd ~/work/Nezha && git fetch ~/deploy.bundle $BRANCH && git checkout -B $BRANCH FETCH_HEAD" || {
      echo "DEPLOY_FAIL $h: fetch/checkout"; exit 1; }
  fi
  got=$(ssh "$h" "cd ~/work/Nezha && git rev-parse HEAD" 2>/dev/null | tr -d '\r')
  [ "$got" = "$WANT" ] || { echo "DEPLOY_FAIL $h: HEAD is $got, wanted $WANT"; exit 1; }
  if [ "$BUILD" = 1 ]; then
    ssh "$h" "source ~/env.sh; cd ~/work/Nezha && go build -o /tmp/nezha-three-normal ./cmd/nezha/" || {
      echo "DEPLOY_FAIL $h: build"; exit 1; }
  fi
  scp -q scripts/multinode/three-node.sh "$h:~/three-node.sh"
  echo "DEPLOY_OK $h ${WANT:0:7}"
done
