#!/bin/bash
# 重复验证用的节点启动脚本（两台机器各放一份，内容相同）。
#   rep-node.sh start ROLE VS N BIN      ROLE=leader|follower  BIN=normal|race
#   env SYSTEM=nezha|original|lsm-raft|... (default nezha), EXTRA="..." extra node flags
#   rep-node.sh stop  ROLE
#   rep-node.sh report ROLE               GC 轮数 + 错误行
# 与 two-node.sh 的区别：不每轮重编译（二进制比源码旧才重编），且支持 -race 版本。
set -u

# key_width —— 盘上定长 key 的宽度（internal/raft/persister.go 的 KeyLength）。
# 本脚本被 deploy.sh 单独 scp 到服务器的 ~ 下执行，source 不到 scripts/lib/bench-common.sh，
# 所以这里内联一份。写死过一次（10），KeyLength 改成 24 之后 gcThresholdGB 偏小约 10%，
# 不报错、只是 GC 比预期早触发。解析不到就退出，不给默认值。
key_width() {
    local w; w=$(sed -n 's/^const KeyLength = \([0-9]*\).*/\1/p' \
        "$HOME/work/Nezha/internal/raft/persister.go" 2>/dev/null | head -1)
    [ -n "$w" ] || { echo "读不到 ~/work/Nezha/internal/raft/persister.go 里的 KeyLength" >&2; exit 1; }
    echo "$w"
}

source ~/env.sh
cd ~/work/Nezha
export TMPDIR=$HOME/work/tmp; mkdir -p "$TMPDIR"
CMD=${1:?start|stop|report}; ROLE=${2:?leader|follower}
D=$HOME/work/rep-$ROLE
L=192.168.1.240:30991; F=192.168.1.241:30991
case $CMD in
start)
  VS=${3:?vsize}; N=${4:?n}; BIN=${5:-normal}; SYSTEM=${SYSTEM:-nezha}; EXTRA=${EXTRA:-}
  SELF=$L; [ "$ROLE" = follower ] && SELF=$F
  EXE=/tmp/nezha-rep; [ "$BIN" = race ] && EXE=/tmp/nezha-rep-race
  NEWEST=$(ls -t internal/raft/*.go cmd/nezha/*.go | head -1)
  if [ ! -x "$EXE" ] || [ "$EXE" -ot "$NEWEST" ]; then
    if [ "$BIN" = race ]; then go build -race -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }
    else go build -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }; fi
  fi
  rm -rf "$D"; mkdir -p "$D"
  GB=$(awk -v n="$N" -v v="$VS" -v k="$(key_width)" 'BEGIN{printf "%.6f", n*(20+k+v)/1073741824/3}')
  nohup "$EXE" -address "${SELF%:*}:3099" -internalAddress "$SELF" -peers "$L,$F" \
      -data "$D" -gap 1000000 -system "$SYSTEM" -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 $EXTRA \
      > "$D/n.log" 2>&1 &
  echo $! > "$D/pid"
  sleep 1; kill -0 "$(cat "$D/pid")" 2>/dev/null && echo "STARTED $ROLE pid=$(cat "$D/pid") bin=$BIN gc=${GB}GB" || { echo "START_FAIL $ROLE"; tail -5 "$D/n.log"; exit 1; }
  ;;
stop)
  [ -f "$D/pid" ] && kill "$(cat "$D/pid")" 2>/dev/null; sleep 1
  [ -f "$D/pid" ] && kill -9 "$(cat "$D/pid")" 2>/dev/null
  echo "STOPPED $ROLE"
  ;;
report)
  alive=no; [ -f "$D/pid" ] && kill -0 "$(cat "$D/pid")" 2>/dev/null && alive=yes
  idx=$(grep -c '建立了索引' "$D/n.log") || idx=0
  done_=$(grep -c '垃圾回收完成' "$D/n.log") || done_=0
  err=$(grep -c -E 'panic|DATA RACE|垃圾回收出现了错误|读取旧库记录失败|合并中止|failed to read entry|EOF' "$D/n.log") || err=0
  echo "REPORT $ROLE alive=$alive gc_index=$idx gc_done=$done_ err_lines=$err"
  grep -E -m 5 'panic|DATA RACE|垃圾回收出现了错误|读取旧库记录失败|合并中止|failed to read entry|EOF' "$D/n.log" | cut -c1-200
  ;;
esac
