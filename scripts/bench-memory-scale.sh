#!/bin/bash
# 大规模内存对比：原版 Nezha（稠密 map）vs 本分支（三项优化齐全）。
#
# 为什么要单独一个脚本：小规模下稠密 map 只占几十 MB，淹没在 RSS 噪声里，
# 必须把 key 数提上去差异才显形。内存按 key 计费而数据量按 value 计费，
# 64B 小值正是两者脱节最严重的场景。
#
# 对照组的构造：从 multiGC 分支取代码，只打两个补丁——
#   1. 删掉 rf.Offsets 的哨兵（那是本分支修掉的一个真实 bug，
#      不删的话对照组会在 GC 后崩溃，测不出内存曲线）
#   2. 把写死的 GC 阈值改成与实验组一致，保证两边 GC 行为可比
# 除此之外不动，确保差异只来自三项优化本身。
#
# 用法: bash scripts/bench-memory-scale.sh [写入条数] [value大小]
set -u

GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
warn(){ echo -e "${YEL}[WARN]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"
# shellcheck source=scripts/lib/bench-common.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/bench-common.sh"

export PATH=$PATH:/usr/local/go/bin
for d in /usr/lib/x86_64-linux-gnu /usr/local/lib /usr/lib; do
    [ -f "$d/librocksdb.so" ] && L=$d && break
done
[ -z "${L:-}" ] && fail "librocksdb.so 未找到，先跑 scripts/setup-env.sh"
export CGO_CFLAGS="-I/usr/include"
export CGO_LDFLAGS="-L$L -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH:-}:$L

N=${1:-5000000}; VSIZE=${2:-64}
BASE_BRANCH="${BASE_BRANCH:-multiGC}"
THIS_BRANCH=$(git rev-parse --abbrev-ref HEAD)
CSV="/tmp/memscale_${N}_${VSIZE}.csv"

BPE=$((20 + 10 + VSIZE))
GC_GB=$(awk -v n=$N -v b=$BPE 'BEGIN{printf "%.4f", n*b/1073741824/3}')

info "写入 $N 条 × ${VSIZE}B，GC 阈值 ${GC_GB} GB"
awk -v n=$N -v b=$BPE 'BEGIN{
  printf "理论内存：对照组 rf.log %.0fMB + 稠密map %.0fMB ≈ %.2fGB；实验组 稀疏索引 %.0fMB + 缓存预算 64MB\n",
    n*280/1048576, n*55/1048576, n*(280+55)/1073741824, n*b/4096*40/1048576}'
info "机器内存：$(free -m | awk '/^Mem:/{print $2}') MB"

# 工作区不干净的话，下面的 git checkout 会毁掉未提交的改动
git diff --quiet || fail "工作区有未提交改动，先 commit 或 stash"

echo "label,commit,writes,vsize,peak_rss_mb,final_rss_mb,gc_rounds,node_alive,goodput,put_latency_ms,put_elapse" > "$CSV"

restore_branch(){ git checkout -q raft/raft.go kvstore/FlexSync/FlexSync.go 2>/dev/null; git checkout -q "$THIS_BRANCH"; }
trap restore_branch EXIT

run() {
  local label=$1 mode=$2
  local BIN=/tmp/nezha-mem-$label

  if [ "$mode" = dense ]; then
    git checkout -q "$BASE_BRANCH" || fail "切不到对照分支 $BASE_BRANCH"
    sed -i '/rf.Offsets = append(rf.Offsets, 0)/d' raft/raft.go
    sed -i "s/if fileSizeGB <= 4000 {/if fileSizeGB <= $GC_GB {/" kvstore/FlexSync/FlexSync.go
    grep -q "fileSizeGB <= $GC_GB" kvstore/FlexSync/FlexSync.go \
        || warn "对照组 GC 阈值未替换成功，两组 GC 行为可能不可比"
    go build -o $BIN "$(server_pkg)" || { restore_branch; fail "[$label] 编译失败"; }
    restore_branch
    EXTRA=""
  else
    git checkout -q "$THIS_BRANCH"
    go build -o $BIN "$(server_pkg)" || fail "[$label] 编译失败"
    EXTRA="-inlineCacheMB 64 -indexBlockKB 4 -gcThresholdGB $GC_GB"
  fi

  local D; D=$(mktemp -d)
  # shellcheck disable=SC2086
  $BIN -address 127.0.0.1:3088 -internalAddress 127.0.0.1:30881 \
       -peers 127.0.0.1:30881 -data "$D" -gap 5000000 $EXTRA > "$D/n.log" 2>&1 &
  local PID=$!
  sleep 10
  kill -0 $PID 2>/dev/null || { tail -15 "$D/n.log"; rm -rf "$D" $BIN; fail "[$label] 节点未启动"; }

  local S; S=$(start_rss_sampler "$PID" "$D/rss.txt" 5)

  go run "$(bench_pkg randwrite_goroutine)" \
      -cnums 50 -dnums "$N" -vsize $VSIZE -servers 127.0.0.1:3088 > "$D/put.out" 2>&1
  local W; W=$(grep "elapse:" "$D/put.out" | tail -1)
  echo "  写入: ${W:-（无输出，benchmark 未产出结果行）}"
  sleep 60
  kill $S 2>/dev/null

  # 节点被 OOM 杀死是本实验的有效结论之一（对照组撑不住这个规模），
  # 因此这里必须照常出结果，不能因为读不到 RSS 就整轮报错退出。
  local alive=DEAD; kill -0 $PID 2>/dev/null && alive=ALIVE
  local peak final gc gp lat elapse
  peak=$(peak_mb "$D/rss.txt") || peak=0
  final=$(rss_mb "$PID")
  gc=$(count_matches "垃圾回收完成" "$D/n.log")
  gp=$(sed -n 's/.*goodPut \([0-9]*\).*/\1/p' <<<"$W")
  lat=$(sed -n 's/.*avg latency:\([0-9.]*\)ms.*/\1/p' <<<"$W")
  elapse=$(sed -n 's/.*elapse:\([^,]*\),.*/\1/p' <<<"$W")

  info "[$label] 峰值RSS=${peak}MB 结束RSS=${final}MB GC轮次=$gc 节点=$alive goodPut=${gp:-NA}/$N"
  [ "$alive" = DEAD ] && warn "[$label] 节点已退出——写入未完成，性能数字是内存耗尽状态下的读数，不可当作干净基线"
  echo "$label,$(git rev-parse --short HEAD),$N,$VSIZE,$peak,$final,$gc,$alive,${gp:-NA},${lat:-NA},${elapse:-NA}" >> "$CSV"

  kill $PID 2>/dev/null; wait $PID 2>/dev/null
  rm -rf "$D" $BIN
  sleep 10
}

run dense_map dense
run sparse4kb sparse

echo ""; info "结果 -> $CSV"; column -s, -t "$CSV"
