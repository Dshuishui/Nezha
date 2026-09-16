#!/bin/bash
# 三节点故障切换验证的节点脚本（-race 构建）。两台机器各放一份。
#   three-race-node.sh start IDX PORT IPORT VS N     以 -race 二进制拉起 node IDX（数据目录清空）
#   three-race-node.sh kill9 IDX                     模拟宕机
#   three-race-node.sh stop  IDX
#   three-race-node.sh pause  IDX                    SIGSTOP：进程还在、只是不干活
#   three-race-node.sh resume IDX                    SIGCONT
#   three-race-node.sh mem    IDX                    RSS 与最近一次压缩后的内存日志条数
#   three-race-node.sh snaplog IDX [N]               最近 N 行快照/截断相关日志
#   three-race-node.sh report IDX                    GC 轮数 / DATA RACE / 错误行
# 环境变量：BIN=race|normal；SYSTEM=nezha|original|lsm-raft|...（默认 nezha）；
#           EXTRA="-sstSpanMB 4" 之类追加给节点的参数；GC_PAUSE_MS 见 GC.go。
set -u
source ~/env.sh
cd ~/work/Nezha
export TMPDIR=$HOME/work/tmp; mkdir -p "$TMPDIR"
CMD=${1:?}; IDX=${2:?}
# key_width —— 盘上定长 key 的宽度。存储层原样存 key，宽度由客户端决定：
# internal/client 的 DefaultKeyPadWidth。
# 本脚本被 deploy.sh 单独 scp 到服务器的 ~ 下执行，source 不到 scripts/lib/bench-common.sh，
# 所以这里内联一份。写死过一次（10），改宽度之后 gcThresholdGB 就偏小，
# 不报错、只是 GC 比预期早触发。解析不到就退出，不给默认值。
key_width() {
    local w; w=$(sed -n 's/^[[:space:]]*DefaultKeyPadWidth[[:space:]]*=[[:space:]]*\([0-9][0-9]*\).*/\1/p' \
        "$HOME/work/Nezha/internal/client/client.go" 2>/dev/null | head -1)
    [ -n "$w" ] || { echo "读不到 ~/work/Nezha/internal/client/client.go 里的 DefaultKeyPadWidth" >&2; exit 1; }
    echo "$w"
}


D=$HOME/work/three-$IDX
# 拓扑由调用方给（驱动脚本从 gate.sh 的 peers_str 取，两种拓扑见那里）。默认值是历史
# 拓扑——node1/node2 同在 241——这样单独手工调用时行为不变。
#
# **start 时会把它落盘到 $D/peers，restart 一律从那里读。** 不这么做的话：restart 若没带
# PEERS 环境变量就会回落到默认值，于是一个本该在三台拓扑里的节点带着两台拓扑的 peers 表
# 重启，它会加入一个**不同的集群**——而且不报错，只表现为"追不上"。
PEERS=${PEERS:-"192.168.1.240:30991,192.168.1.241:30991,192.168.1.241:30992"}
BIN=${BIN:-race}; EXE=/tmp/nezha-three-$BIN
SYSTEM=${SYSTEM:-nezha}; EXTRA=${EXTRA:-}
# 什么算"错误行"。这里的取舍很要紧：漏了真错误是漏报，收进良性行是**假失败**，
# 而假失败会训练人去忽略这个闸门，所以两个方向都不能松。
#
# `LOG-STUCK` 曾经在这份名单里，现在按措辞区分：
#   "交给快照补齐"        —— 良性。快照路径接手了，这只是一条值得知道的事件。
#   "它再也追不上了"      —— 真失效。本节点没启用快照，那个副本从此是个摆设。
# 不区分的后果是：任何真的跑到快照的运行都会被判成失败。
#
# 新增的三条是快照路径**静默**的失效：安装失败会退回原状态并等 leader 重发，
# 反复失败的话那个副本永远追不上，而它不 panic、不崩、日志之外没有任何迹象。
# `发快照失败` 刻意**不**收进来：对端宕机、任期陈旧都会让它发生，和
# "[LSM-Raft] ship" 一样是设计上的瞬时失败。
ERRPAT='panic|DATA RACE|LOG-STUCK.*再也追不上|垃圾回收出现了错误|读取旧库记录失败|合并中止|failed to read entry|EOF|fatal|RECOVER\] .*失败|\[Error\].*LSM-Raft|\[SNAPSHOT\] 安装失败|制作快照失败|没装上快照'
# "[LSM-Raft] ship" errors are transient by design (peer down, stale term) and not counted.
errlines() { cat "$D"/n*.log | grep -E "$ERRPAT" | grep -v 'LSM-Raft\] ship'; }
# pid 文件可能是陈旧的（见 start 里的说明），所以两个收尾命令都再按 `-data $D` 兜一遍。
#
# 匹配的是**本节点自己的数据目录**加一个尾随空格（命令行里 -data 后面一定还有参数），
# 所以既碰不到别人的实验，也不会把 three-1 前缀匹配到 three-10。
# 这里用 `pgrep -f` 是安全的，与 deploy.sh 里那条禁令不冲突：那条禁令针对的是**经 ssh
# 发起**的 pgrep 会匹配到 ssh 自己的命令行，而本脚本是在服务器上执行的，它自己的命令行
# 是 `~/three-node.sh stop 1`，不含 -data。仍然把自己的 pid 排掉，以防将来被内联调用。
own_pids() { pgrep -f -- "-data $D " 2>/dev/null | grep -v "^$$\$"; }
case $CMD in
start)
  PORT=${3:?}; IPORT=${4:?}; VS=${5:-1024}; N=${6:-20000}
  SELF_IP=$(hostname -I | awk '{for(i=1;i<=NF;i++) if($i ~ /^192\.168\.1\./){print $i;exit}}')
  # 新旧判据必须覆盖**全部**被编译进节点的源码。原先只看 internal/raft 与 cmd/nezha，
  # 而 GC 改造整个落在 internal/kvstore——改完直接跑，会静默复用上一次的二进制。
  NEWEST=$(find internal cmd -name '*.go' -newer /dev/null -print0 2>/dev/null | xargs -0 ls -t 2>/dev/null | head -1)
  if [ ! -x "$EXE" ] || [ "$EXE" -ot "$NEWEST" ]; then
    if [ "$BIN" = race ]; then go build -race -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }
    else go build -o "$EXE" ./cmd/nezha/ || { echo BUILD_FAIL; exit 1; }; fi
  fi
  rm -rf "$D"; mkdir -p "$D"
  GB=$(awk -v n="$N" -v v="$VS" -v k="$(key_width)" 'BEGIN{printf "%.6f", n*(20+k+v)/1073741824/3}')
  echo "$PORT $IPORT $GB $SYSTEM $EXTRA" > "$D/args"   # restart 时原样复用
  echo "$PEERS" > "$D/peers"                          # 同上，理由见文件开头 PEERS 那段
  # shellcheck disable=SC2086
  if [ "${TS:-0}" = 1 ]; then
    # A FIFO, not process substitution: with >(...) the timestamper inherits the caller's
    # stdout, so an ssh session that starts a node never returns (it printed STARTED and
    # then hung for the life of the node). Here every fd of both processes is a file or the
    # FIFO, so the SSH channel closes as soon as start does.
    rm -f "$D/pipe"; mkfifo "$D/pipe"
    nohup perl -MTime::HiRes=time -ne 'BEGIN{$|=1} my $n=time; my @t=localtime($n); printf "%02d:%02d:%02d.%03d %s",$t[2],$t[1],$t[0],($n-int($n))*1000,$_' \
        < "$D/pipe" > "$D/n.log" 2>/dev/null &
    nohup env ${GC_PAUSE_MS:+NEZHA_GC_PAUSE_MS=$GC_PAUSE_MS} "$EXE" -address "$SELF_IP:$PORT" -internalAddress "$SELF_IP:$IPORT" -peers "$PEERS" \
        -data "$D" -gap 1000000 -system "$SYSTEM" -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 $EXTRA \
        < /dev/null > "$D/pipe" 2>&1 &
  else
    nohup env ${GC_PAUSE_MS:+NEZHA_GC_PAUSE_MS=$GC_PAUSE_MS} "$EXE" -address "$SELF_IP:$PORT" -internalAddress "$SELF_IP:$IPORT" -peers "$PEERS" \
        -data "$D" -gap 1000000 -system "$SYSTEM" -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 $EXTRA \
        > "$D/n.log" 2>&1 &
  fi
  NEWPID=$!; sleep 1
  # **启动失败时不要写 pid 文件。** 曾经是无条件 `echo $! > pid`：端口被上一轮的残留占着
  # 时新进程立刻退出，pid 文件却已被覆盖成这个死 pid，于是后面的 `stop` 去杀一个不存在的
  # 进程、把上一轮的真进程**孤立**掉，而且两边都不报错。2026-09-16 实测：run 3 因故中途
  # 死掉留下两个节点，run 4 启动失败覆盖了 pid，`stop` 全部返回 STOPPED 而进程还在跑。
  if kill -0 "$NEWPID" 2>/dev/null; then
    echo "$NEWPID" > "$D/pid"
    echo "STARTED node$IDX $SELF_IP:$PORT pid=$NEWPID bin=$BIN"
  else
    echo "START_FAIL node${IDX}（pid 文件保持原样，不覆盖）"; tail -3 "$D/n.log"; exit 1
  fi
  ;;
restart)
  # 不清目录、不重建：同一份数据目录原地重启，走崩溃恢复路径。日志另起一个文件便于区分。
  [ -f "$D/args" ] || { echo "RESTART_FAIL node$IDX: no args"; exit 1; }
  read -r PORT IPORT GB SYSTEM EXTRA < "$D/args"; SYSTEM=${SYSTEM:-nezha}; EXTRA=${EXTRA:-}
  # peers 必须从盘上读，读不到就拒绝重启：静默回落到默认拓扑会让节点加入另一个集群。
  if [ -s "$D/peers" ]; then PEERS=$(cat "$D/peers")
  else echo "RESTART_FAIL node$IDX: 没有 $D/peers，拒绝用默认拓扑重启"; exit 1; fi
  SELF_IP=$(hostname -I | awk '{for(i=1;i<=NF;i++) if($i ~ /^192\.168\.1\./){print $i;exit}}')
  n=$(ls "$D"/n*.log 2>/dev/null | wc -l)
  LOGF="$D/n$n.log"
  # shellcheck disable=SC2086
  nohup "$EXE" -address "$SELF_IP:$PORT" -internalAddress "$SELF_IP:$IPORT" -peers "$PEERS" \
      -data "$D" -gap 1000000 -system "$SYSTEM" -syncWAL -gcThresholdGB "$GB" -commitTimeoutS 60 $EXTRA \
      > "$LOGF" 2>&1 &
  NEWPID=$!; sleep 2
  # 与 start 同理：失败时不覆盖 pid 文件，理由见那里。
  if kill -0 "$NEWPID" 2>/dev/null; then
    echo "$NEWPID" > "$D/pid"
    echo "RESTARTED node$IDX pid=$NEWPID log=$LOGF"
  else
    echo "RESTART_FAIL node${IDX}（pid 文件保持原样，不覆盖）"; tail -5 "$LOGF"; exit 1
  fi
  ;;
recoverlog)
  # 打印最近一次启动日志里的恢复相关行
  f=$(ls -t "$D"/n*.log | head -1); grep -hE "RECOVER|恢复|GC-PAUSE|LEASE|LOG-PINNED|LOG-STUCK|Candidate|Leader|election|panic|DATA RACE|fatal" "$f" | head -30
  ;;
kill9) kill -9 "$(cat "$D/pid" 2>/dev/null)" 2>/dev/null
       for q in $(own_pids); do kill -9 "$q" 2>/dev/null; done
       sleep 1; [ -n "$(own_pids)" ] && echo "STILL_ALIVE node$IDX" || echo "KILLED node$IDX" ;;
stop)  kill "$(cat "$D/pid" 2>/dev/null)" 2>/dev/null
       for q in $(own_pids); do kill "$q" 2>/dev/null; done
       sleep 1
       for q in $(own_pids); do kill -9 "$q" 2>/dev/null; done
       sleep 1; [ -n "$(own_pids)" ] && echo "STOP_FAIL node$IDX" || echo "STOPPED node$IDX" ;;
pause)
  # SIGSTOP 而不是 kill：进程还在、端口还占着、TCP 连接不断，只是不干活。这正是
  # CockroachDB 所说的 "not recently active" follower，也最贴近"某个节点明显更慢"。
  kill -STOP "$(cat "$D/pid")" 2>/dev/null && echo "PAUSED node$IDX" || echo "PAUSE_FAIL node$IDX"
  ;;
resume) kill -CONT "$(cat "$D/pid")" 2>/dev/null && echo "RESUMED node$IDX" || echo "RESUME_FAIL node$IDX" ;;
mem)
  # RSS 与"最近一次成功压缩之后的内存日志条数"。两者都只能在节点所在的机器上取，
  # 因为 pid 文件和日志都在那里。
  #
  # retained 取不到时输出 none 而**不是** 0：调用方的判据是"被按住期间的驻留条数
  # ≤ 基线的若干倍"，用 0 顶替等于让判据无条件成立——一次都没压缩过反而会被判成
  # "内存有界"，正好把要抓的失效放过去。
  rssv=$(ps -o rss= -p "$(cat "$D/pid" 2>/dev/null)" 2>/dev/null | tr -d ' ')
  ret=$(cat "$D"/n*.log 2>/dev/null | grep -o 'compactLog: [0-9]* -> [0-9]* 条' | tail -1 | grep -oE '[0-9]+ 条' | grep -oE '[0-9]+')
  trunc=$(cat "$D"/n*.log 2>/dev/null | grep -c 'LOG-TRUNCATE') || trunc=0
  pin=$(cat "$D"/n*.log 2>/dev/null | grep -c 'LOG-PINNED') || pin=0
  sent=$(cat "$D"/n*.log 2>/dev/null | grep -c '开始给它发快照') || sent=0
  made=$(cat "$D"/n*.log 2>/dev/null | grep -c 'SNAPSHOT\] 做好一份') || made=0
  got=$(cat "$D"/n*.log 2>/dev/null | grep -c 'SNAPSHOT\] 装好一份') || got=0
  echo "MEM node$IDX rss_kb=${rssv:-0} retained=${ret:-none} truncates=$trunc pinned=$pin snap_sent=$sent snap_made=$made snap_installed=$got"
  ;;
snaplog)
  cat "$D"/n*.log 2>/dev/null | grep -E '\[SNAPSHOT\]|\[LOG-TRUNCATE\]|\[LOG-PINNED\]|\[LOG-STUCK\]' | tail -"${3:-8}" | cut -c1-200
  ;;
report)
  alive=no; kill -0 "$(cat "$D/pid")" 2>/dev/null && alive=yes
  # "轮垃圾回收完成", not "垃圾回收完成": the latter also matches the banner line that
  # precedes it, so a single round used to be reported as two.
  gc=$(cat "$D"/n*.log | grep -c '轮垃圾回收完成') || gc=0
  races=$(cat "$D"/n*.log | grep -c 'WARNING: DATA RACE') || races=0
  err=$(errlines | wc -l | tr -d ' ')
  cand=$(cat "$D"/n*.log | grep -c 'Candidate\|没有收到来自leader') || cand=0
  silent=$(cat "$D"/n*.log | grep -c '没有收到来自leader') || silent=0
  elect=$(cat "$D"/n*.log | grep -c 'Follower -> Candidate') || elect=0
  won=$(cat "$D"/n*.log | grep -c 'Candidate -> Leader') || won=0
  stalls=$(cat "$D"/n*.log | grep -c 'LOCK-STALL') || stalls=0
  slow=$(cat "$D"/n*.log | grep -c 'SLOW-APPEND') || slow=0
  term=$(cat "$D"/n*.log | grep -o 'currentTerm\[[0-9]*\]' | tail -1 | grep -o '[0-9]*'); term=${term:-0}
  # LSM-Raft: spans cut as leader / ingested as follower, with the last index of each
  cut=$(cat "$D"/n*.log | grep -c 'LSM-Raft\] span \[.*\] cut') || cut=0
  lastcut=$(cat "$D"/n*.log | grep -o 'LSM-Raft\] span \[[0-9]*,[0-9]*\] cut' | tail -1 | grep -o ',[0-9]*' | tr -d ,); lastcut=${lastcut:-0}
  ing=$(cat "$D"/n*.log | grep -c 'LSM-Raft\] ingested span') || ing=0
  lasting=$(cat "$D"/n*.log | grep -o 'LSM-Raft\] ingested span \[[0-9]*,[0-9]*\]' | tail -1 | grep -o ',[0-9]*' | tr -d ,); lasting=${lasting:-0}
  replay=$(cat "$D"/n*.log | grep -c 'LSM-Raft\].*replaying') || replay=0
  echo "REPORT node$IDX alive=$alive gc_done=$gc races=$races err_lines=$err silent_leader_msgs=$cand silent=$silent elections=$elect won=$won lock_stalls=$stalls slow_appends=$slow term=$term lsm_cut=$cut lsm_lastcut=$lastcut lsm_ingested=$ing lsm_lastingested=$lasting lsm_replays=$replay"
  errlines | grep -v "DATA RACE" | head -3 | cut -c1-160
  ;;
timeline)
  # GC start/end, heartbeat silence, and every role change, in log order. Needs TS=1 at
  # start for the fmt.Printf lines (GC, "没有收到来自leader") to carry a time of their own.
  cat "$D"/n*.log | grep -E 'Starting garbage collection|垃圾回收完成|垃圾回收出现了错误|GC-PHASE|GC-ABSORB|LEASE|LOG-PINNED|LOG-STUCK|LOCK-STALL|SLOW-APPEND|忽略.*拉票|没有收到来自leader|Follower -> Candidate|Candidate -> Leader' \
    | sed "s/^/node$IDX /"
  ;;
esac
