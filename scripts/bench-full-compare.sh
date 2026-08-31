#!/bin/bash
# 原版 Nezha vs 本分支的完整对照：内存 + PUT/GET/SCAN 三项。
#
# 与 bench-memory-scale.sh 的分工：那个只测写入内存，能把规模推到对照组 OOM 为止；
# 这个要求两组都完整跑完读负载，因此规模必须选在对照组撑得住的范围内。
#
# 对照组的构造（三处改动，缺一不可）：
#   1. 删掉 rf.Offsets 哨兵——本分支修掉的真实 bug，不删对照组会在 GC 后崩溃
#   2. 把写死的 GC 阈值 4000GB 改成与实验组相同的值
#      注意：对照组没有 -gcThresholdGB 这个 flag，只能改源码。不改的话对照组
#      根本不触发 GC，读路径不走 sortedFile，两组测的就不是同一件事了。
#   3. 其余一律不动
#
# 用法: bash scripts/bench-full-compare.sh [写入量] [value大小] [SCAN轮数]
set -u

GREEN='\033[0;32m'; RED='\033[0;31m'; YEL='\033[1;33m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $1"; }
warn(){ echo -e "${YEL}[WARN]${NC} $1"; }
fail(){ echo -e "${RED}[FAIL]${NC} $1"; exit 1; }

PROJECT_DIR="${PROJECT_DIR:-$HOME/Github/Nezha}"; cd "$PROJECT_DIR" || fail "无项目目录"

N="${1:-3000000}"; VSIZE="${2:-64}"; SCAN_TESTS="${3:-20}"
BASE_BRANCH="${BASE_BRANCH:-multiGC}"
THIS_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BPE=$((20 + 10 + VSIZE))
GC_GB=$(awk -v n="$N" -v b="$BPE" 'BEGIN{printf "%.4f", n*b/1073741824/3}')

git diff --quiet || fail "工作区有未提交改动，先 commit 或 stash（本脚本会切分支）"

info "规模 $N 条 × ${VSIZE}B，GC 阈值 ${GC_GB} GB，SCAN ${SCAN_TESTS} 轮"
awk -v n="$N" 'BEGIN{printf "[INFO] 对照组理论内存 %.2f GB（rf.log 280B/条 + 稠密 map 55B/条）\n", n*335/1073741824}'
info "机器内存 $(free -m | awk '/^Mem:/{print $2}') MB —— 对照组必须完整跑完读负载，规模不能贪大"

restore(){ git checkout -q raft/raft.go kvstore/FlexSync/FlexSync.go 2>/dev/null; git checkout -q "$THIS_BRANCH"; }
trap restore EXIT

# 对照组：打补丁 -> 跑 -> 立刻还原，避免脏工作区留到下一步
info "===== 对照组 ($BASE_BRANCH) ====="
git checkout -q "$BASE_BRANCH" || fail "切不到 $BASE_BRANCH"
sed -i '/rf.Offsets = append(rf.Offsets, 0)/d' raft/raft.go
sed -i "s/if fileSizeGB <= 4000 {/if fileSizeGB <= $GC_GB {/" kvstore/FlexSync/FlexSync.go
grep -q "fileSizeGB <= $GC_GB" kvstore/FlexSync/FlexSync.go \
    || fail "对照组 GC 阈值替换失败——不修正的话对照组不会触发 GC，两组不可比"
grep -q "rf.Offsets = append(rf.Offsets, 0)" raft/raft.go \
    && fail "对照组哨兵未删除——会在 GC 后崩溃"
# scan_pro 的 -tests flag 只有本分支有，对照组要用同样的轮数，把这个文件也带过去
git checkout -q "$THIS_BRANCH" -- benchmark/scan_pro/scan_pro.go
SCAN_TESTS="$SCAN_TESTS" bash scripts/bench-avp-compare.sh before "$N" "$VSIZE" 64 4
RC=$?
restore
[ $RC -eq 0 ] || fail "对照组未跑完（rc=$RC）"

info "===== 实验组 ($THIS_BRANCH) ====="
SCAN_TESTS="$SCAN_TESTS" bash scripts/bench-avp-compare.sh after "$N" "$VSIZE" 64 4 || fail "实验组未跑完"

echo ""
info "===== 汇总 ====="
head -1 /tmp/avpcmp_before.csv > /tmp/avpcmp_full.csv
tail -n +2 /tmp/avpcmp_before.csv >> /tmp/avpcmp_full.csv
tail -n +2 /tmp/avpcmp_after.csv  >> /tmp/avpcmp_full.csv
column -s, -t /tmp/avpcmp_full.csv
info "结果 -> /tmp/avpcmp_full.csv"
