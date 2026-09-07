#!/bin/bash
# 把一次实验的数据从服务器收回 results/ 下的标准目录，并填好 meta.txt。
#
# 数据与产生它的条件必须绑在一起。一个只有 CSV 的目录，几周后没人说得清它是哪个
# commit、什么参数、跑在哪台机器上——2026-09-07 有三轮验证跑在了三个提交之前的
# 二进制上，当时若没记下服务器实际的 commit，这件事根本发现不了。所以 commit 一栏
# 取的是**服务器上那份仓库的 HEAD**，不是本地 HEAD。
#
# 用法:
#   bash scripts/bench/collect-results.sh <类别> <标签> <主机> <远端CSV> [远端日志]
# 例:
#   bash scripts/bench/collect-results.sh maintable nofsync tikv241 \
#        /tmp/maintable-nofsync.csv /tmp/maintable-nofsync.log
# 环境变量:
#   DATE=YYYY-MM-DD   目录日期，默认今天
#   CMD="..."         产生这份数据的完整命令行，写进 meta
#   PARAMS="..."      关键参数展开
#   VERDICT="..."     OK / 部分失败(...) / 作废(...)；留空则写 TODO 提醒补
#   NOTES="..."       影响解读的事项
set -eu
GREEN='\033[0;32m'; RED='\033[0;31m'; NC='\033[0m'
info(){ echo -e "${GREEN}[INFO]${NC} $*"; }
die(){  echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cd "$(dirname "$0")/../.."
CAT="${1:?用法: $0 <类别> <标签> <主机> <远端CSV> [远端日志]}"
LABEL="${2:?缺标签}"
HOST="${3:?缺主机}"
RCSV="${4:?缺远端 CSV 路径}"
RLOG="${5:-}"
DATE="${DATE:-$(date +%F)}"

DIR="results/$CAT/$DATE-$LABEL"
[ -d "$DIR" ] && die "$DIR 已存在——换个标签，不要覆盖既有数据"
mkdir -p "$DIR"

info "从 $HOST 取 $RCSV"
scp -q "$HOST:$RCSV" "$DIR/data.csv" || die "取 CSV 失败"
[ -s "$DIR/data.csv" ] || die "CSV 是空的——先确认实验真的跑出了数据"
if [ -n "$RLOG" ]; then
  scp -q "$HOST:$RLOG" "$DIR/driver.log" || die "取日志失败"
fi

# 服务器上仓库的 HEAD 才是被测版本
RCOMMIT=$(ssh "$HOST" "cd ~/work/Nezha && git rev-parse HEAD" 2>/dev/null | tr -d '\r')
RSHORT=${RCOMMIT:0:7}
LOCAL=$(git rev-parse HEAD)
SAME=$([ "$RCOMMIT" = "$LOCAL" ] && echo "与本地 HEAD 一致" || echo "⚠ 与本地 HEAD ($( echo "$LOCAL" | cut -c1-7)) 不同")

ROWS=$(( $(wc -l < "$DIR/data.csv") - 1 ))
cat > "$DIR/meta.txt" <<EOF
experiment  $CAT
date        $DATE
label       $LABEL
commit      $RSHORT   （服务器 $HOST 上仓库的 HEAD；$SAME）
host        $HOST
command     ${CMD:-（未记录——请补上完整命令行）}
params      ${PARAMS:-（未记录）}
rows        $ROWS
verdict     ${VERDICT:-TODO 补：OK / 部分失败(哪几格) / 作废(原因)}
notes       ${NOTES:-}

收集时间    $(date '+%F %T')
远端路径    $HOST:$RCSV${RLOG:+ , $HOST:$RLOG}
EOF

info "已存 $DIR （$ROWS 行）"
cat "$DIR/meta.txt"
