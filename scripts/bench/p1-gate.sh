#!/bin/bash
# P1 验收：分区化布局是否伤到了读。
#
# 门槛是 GET/SCAN 的 p50/p99 相对改进前退化不超过 5%。改造把 GC 的产物从一个有序文件变成
# N 个按 key 区间分区的文件，GET 因此只查一个更小的索引，SCAN 则可能横跨若干文件——后者是
# 真正的风险点，也是这条门槛要拦的东西。
#
# 两条容易把结果搞脏的地方，这里都堵住了：
#
#  1. **基线必须用今天的脚本跑。** 检出旧 commit 会把 scripts/ 一并退回旧版，于是"改进前 vs
#     改进后"的差异里混进了脚本行为的变化。这里把 scripts/ 拷到工作树之外，两边都从这份副本
#     运行，用 REPO_DIR 指向各自的代码。
#  2. **分区大小必须显式调小。** 100MiB 数据配默认 128MB 只会切出一个分区，路由代码根本不
#     执行，跑出来的"无退化"是假的。
#
# 只跑 nezha 与 nezha-avp：baseline 和 nezha-nogc 不产出有序文件，读路径完全不碰分区，
# 代码路径与改造前逐字相同，跑它们只是浪费机时。
#
# 用法: bash scripts/bench/p1-gate.sh <改进前的 commit> [标签]
# 环境变量:
#   SYNC_WAL=0        先跑关 fsync 的一组；开 fsync 再跑一次即可（PUT 阶段慢很多）
#   PARTITION_MB=16   分区目标大小
#   ROUNDS=3 TOTAL_MB=100 VSIZES="64 256 1024"
#   OUTDIR=/tmp/p1-gate-<标签>
set -u
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info(){ echo -e "${GREEN}[GATE]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

BASE_COMMIT="${1:-}"
[ -n "$BASE_COMMIT" ] || die "用法: p1-gate.sh <改进前的 commit> [标签]"
LABEL="${2:-$(date +%m%d-%H%M)}"

cd "$(dirname "$0")/../.." || die "无项目目录"
REPO=$(pwd)
NEW_COMMIT=$(git rev-parse --short HEAD)
git cat-file -e "${BASE_COMMIT}^{commit}" 2>/dev/null || die "本地没有 commit $BASE_COMMIT"

export TMPDIR=${TMPDIR:-$HOME/work/tmp}; mkdir -p "$TMPDIR"
OUTDIR="${OUTDIR:-/tmp/p1-gate-$LABEL}"; mkdir -p "$OUTDIR"
SYNC_WAL="${SYNC_WAL:-0}"
PARTITION_MB="${PARTITION_MB:-16}"
ROUNDS="${ROUNDS:-3}"
# 扫描规模两边必须相同。它按分区大小派生，但对照组不能收到 -partitionTargetMB
# （旧二进制不认识这个 flag 会直接退出），所以用一个独立变量喂给两边。
SCAN_PART_MB="${SCAN_PART_MB:-$PARTITION_MB}"
TOTAL_MB="${TOTAL_MB:-100}"
VSIZES="${VSIZES:-64 256 1024}"

# 脚本副本放在工作树之外，两边共用同一份
SCRIPTS="$OUTDIR/scripts"
rm -rf "$SCRIPTS"; mkdir -p "$SCRIPTS"
cp -r "$REPO/scripts/." "$SCRIPTS/"
MT="$SCRIPTS/bench/maintable.sh"

# 改进前的代码放在独立 worktree，不动当前分支
BASE_TREE="$TMPDIR/p1-gate-base-$BASE_COMMIT"
if [ ! -d "$BASE_TREE" ]; then
  git worktree add --detach "$BASE_TREE" "$BASE_COMMIT" >/dev/null 2>&1 || die "建立 worktree 失败"
fi
[ "$(cd "$BASE_TREE" && git rev-parse --short HEAD)" = "$(git rev-parse --short "$BASE_COMMIT")" ] \
  || die "worktree 的 HEAD 与 $BASE_COMMIT 不符"

info "改进前 $BASE_COMMIT ($BASE_TREE) | 改进后 $NEW_COMMIT ($REPO)"
info "syncWAL=$SYNC_WAL partitionMB=$PARTITION_MB rounds=$ROUNDS total=${TOTAL_MB}MB vsizes=[$VSIZES]"
info "输出 $OUTDIR"

# run <标签> <代码目录> <分区大小，空则不传>
run(){
  local tag="$1" tree="$2" pmb="$3"
  info "跑 $tag"
  REPO_DIR="$tree" TOTAL_MB="$TOTAL_MB" ROUNDS="$ROUNDS" VSIZES="$VSIZES" \
    SYSTEMS="nezha nezha-avp" SYNC_WAL="$SYNC_WAL" PARTITION_MB="$pmb" \
    SCAN_PART_MB="$SCAN_PART_MB" \
    OUT="$OUTDIR/$tag.csv" bash "$MT" "$LABEL-$tag" > "$OUTDIR/$tag.log" 2>&1 \
    || die "$tag 失败，见 $OUTDIR/$tag.log"
}

# 改进前不认识 -partitionTargetMB，必须传空
run before "$BASE_TREE" ""
run after  "$REPO"      "$PARTITION_MB"

# GC 搬丢的记录不会报错，只在某次 GET 上变成一个 NOKEY，命中率看不出来（见 lost-keys.py）。
# 每格都数一遍，任何一格非零都该在判门槛之前先解决。
info "统计每格丢失的 key"
{
  for tag in before after; do
    for sys in nezha nezha-avp; do for vs in $VSIZES; do for r in $(seq 1 "$ROUNDS"); do
      d="$TMPDIR/mt-$LABEL-$tag-$sys-$vs-$r"
      [ -d "$d" ] || continue
      n=$(awk -v mb="$TOTAL_MB" -v v="$vs" 'BEGIN{printf "%d", mb*1048576/(20+10+v)}')
      th=0; [ "$sys" = nezha-avp ] && th=512   # 内联的小值不在 valuelog 里，本工具数不准
      echo "$tag $sys v=$vs r=$r: $(python3 "$SCRIPTS/bench/lost-keys.py" "$d" "$n" "$vs" "$th" 2>&1 | grep -oE '丢失 [0-9]*|不适用')"
    done; done; done
  done
} | tee "$OUTDIR/lost-keys.txt"

info "判门槛"
python3 "$SCRIPTS/bench/p1-gate-report.py" "$OUTDIR/before.csv" "$OUTDIR/after.csv" | tee "$OUTDIR/verdict.txt"
