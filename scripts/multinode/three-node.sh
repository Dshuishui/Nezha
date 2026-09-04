#!/bin/bash
# 三节点故障切换验证的节点脚本（-race 构建）。两台机器各放一份。
#   three-race-node.sh start IDX PORT IPORT VS N     以 -race 二进制拉起 node IDX（数据目录清空）
#   three-race-node.sh kill9 IDX                     模拟宕机
#   three-race-node.sh stop  IDX
#   three-race-node.sh report IDX                    GC 轮数 / DATA RACE / 错误行
set -u
source ~/env.sh
cd ~/work/Nezha
export TMPDIR=$HOME/work/tmp; mkdir -p "$TMPDIR"
CMD=${1:?}; IDX=${2:?}
D=$HOME/work/three-$IDX
PEERS="192.168.1.240:30991,192.168.1.241:30991,192.168.1.241:30992"
BIN=${BIN:-race}; EXE=/tmp/nezha-three-$BIN
ERRPAT='panic|DATA RACE|垃圾回收出现了错误|读取旧库记录失败|合并中止|failed to read entry|EOF|fatal|RECOVER\] .*失败'
case $CMD in
start)
  PORT=${3:?}; IPORT=${4:?}; VS=${5:-1024}; N=${6:-20000}
  SELF_IP=$(hostname -I | awk '{for(i=1;i<=NF;i++) if($i ~ /^192\.168\.1\./){print $i;exit}}')
  NEWEST=$(ls -t internal/raft/*.go cmd/nezha/*.go | head -1)
  if [ ! -x "$EXE" ] || [ "$EXE" -ot "$NEWEST" ]; then
    if [ "$BIN" = race ]; then go build -race -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }
    else go build -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }; fi
  fi
  rm -rf "$D"; mkdir -p "$D"
  GB=$(awk -v n="$N" -v v="$VS" 'BEGIN{printf "%.6f", n*(20+10+v)/1073741824/3}')
  echo "$PORT $IPORT $GB" > "$D/args"   # restart 时原样复用
  nohup env ${GC_PAUSE_MS:+NEZHA_GC_PAUSE_MS=$GC_PAUSE_MS} "$EXE" -address "$SELF_IP:$PORT" -internalAddress "$SELF_IP:$IPORT" -peers "$PEERS" \
      -data "$D" -gap 1000000 -system nezha -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 \
      > "$D/n.log" 2>&1 &
  echo $! > "$D/pid"; sleep 1
  kill -0 "$(cat "$D/pid")" 2>/dev/null && echo "STARTED node$IDX $SELF_IP:$PORT pid=$(cat "$D/pid") bin=$BIN" || { echo "START_FAIL node$IDX"; tail -3 "$D/n.log"; exit 1; }
  ;;
restart)
  # 不清目录、不重建：同一份数据目录原地重启，走崩溃恢复路径。日志另起一个文件便于区分。
  [ -f "$D/args" ] || { echo "RESTART_FAIL node$IDX: no args"; exit 1; }
  read -r PORT IPORT GB < "$D/args"
  SELF_IP=$(hostname -I | awk '{for(i=1;i<=NF;i++) if($i ~ /^192\.168\.1\./){print $i;exit}}')
  n=$(ls "$D"/n*.log 2>/dev/null | wc -l)
  LOGF="$D/n$n.log"
  nohup "$EXE" -address "$SELF_IP:$PORT" -internalAddress "$SELF_IP:$IPORT" -peers "$PEERS" \
      -data "$D" -gap 1000000 -system nezha -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 \
      > "$LOGF" 2>&1 &
  echo $! > "$D/pid"; sleep 2
  kill -0 "$(cat "$D/pid")" 2>/dev/null && echo "RESTARTED node$IDX pid=$(cat "$D/pid") log=$LOGF" || { echo "RESTART_FAIL node$IDX"; tail -5 "$LOGF"; exit 1; }
  ;;
recoverlog)
  # 打印最近一次启动日志里的恢复相关行
  f=$(ls -t "$D"/n*.log | head -1); grep -hE "RECOVER|恢复|GC-PAUSE|Candidate|Leader|election|panic|DATA RACE|fatal" "$f" | head -30
  ;;
kill9) kill -9 "$(cat "$D/pid")" 2>/dev/null; sleep 1; kill -0 "$(cat "$D/pid")" 2>/dev/null && echo "STILL_ALIVE node$IDX" || echo "KILLED node$IDX" ;;
stop)  kill "$(cat "$D/pid")" 2>/dev/null; sleep 1; kill -9 "$(cat "$D/pid")" 2>/dev/null; echo "STOPPED node$IDX" ;;
report)
  alive=no; kill -0 "$(cat "$D/pid")" 2>/dev/null && alive=yes
  gc=$(cat "$D"/n*.log | grep -c '垃圾回收完成') || gc=0
  races=$(cat "$D"/n*.log | grep -c 'WARNING: DATA RACE') || races=0
  err=$(cat "$D"/n*.log | grep -c -E "$ERRPAT") || err=0
  cand=$(cat "$D"/n*.log | grep -c 'Candidate\|3秒没有收到') || cand=0
  echo "REPORT node$IDX alive=$alive gc_done=$gc races=$races err_lines=$err silent_leader_msgs=$cand"
  cat "$D"/n*.log | grep -E -m 3 "$ERRPAT" | grep -v "DATA RACE" | cut -c1-160
  ;;
esac
