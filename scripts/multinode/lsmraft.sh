#!/bin/bash
# LSM-Raft baseline, three nodes (-system lsm-raft). Checks that followers whose RocksDB
# is fed only by ingested spans hold the same data as the leader, across a leader kill
# and a restart of the killed node:
#   1. start node0..2, write N values, wait until both followers ingested the last span
#   2. read node1 and node2 directly (they never replayed an entry)
#   3. kill node0; the new leader is a former follower and must replay its held entries
#   4. write again through the new leader, wait for the surviving follower, read both
#   5. restart node0 as a follower, wait until it ingested up to the new leader's last span,
#      read it directly
#   6. per-node report: 0 races, 0 error lines, span counters
# Env: BIN=race|normal (default race), SPAN_MB (default 4 so a 20 MB run cuts several
# spans), N, VS_A, VS_B.
set -u
cd "$(dirname "$0")"
BIN=${BIN:-race}; SPAN_MB=${SPAN_MB:-4}; N=${N:-20000}; VS_A=${VS_A:-1024}; VS_B=${VS_B:-512}
LOG=lsmraft-$BIN.log; : > "$LOG"
# r / rq 在 gate.sh（带总超时、本机直跑、别名->用户名@IP），理由见那里。
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
ALL="192.168.1.240:3099,192.168.1.241:3099,192.168.1.241:3100"
NODE_ENV="BIN=$BIN SYSTEM=lsm-raft EXTRA='-sstSpanMB $SPAN_MB -leaderCheck=false'"
fail=0
# shellcheck source=scripts/multinode/gate.sh
. "$(dirname "$0")/gate.sh"
# 这个脚本里的地址还是写死的两台拓扑，所以 TOPO=three 下**拒绝运行**而不是静默跑错。
# 必须放在 source gate.sh 之后、做任何事之前：第一版放在第 1 步之前，于是第 0 步
# （编译客户端工具）已经跑掉了才撞上守卫——拒绝得太晚等于白做一遍活。
# recover.sh 与 slow-follower.sh 已接入 gate.sh 的集中拓扑，它们可以用 TOPO=three。
[ "$TOPO" = two ] || { echo "$0 暂不支持 TOPO=${TOPO}：脚本内地址仍写死两台拓扑" >&2; exit 1; }

# 直读某个指定节点的**本地**状态，所以这些节点必须用 -leaderCheck=false 起。
# `-leaderCheck` 自 811ac32 起默认开，follower 上的读一律回 ErrWrongLeader，而客户端
# 的 -servers 只有一个地址时 redirect 无处可去——本脚本的直读步骤因此从 2026-09-13
# 起恒判失败，直到 2026-09-16 才发现。理由与判定见 gate.sh 的 gate_read_ok。

field() { grep -o "$2=[0-9a-z]*" <<<"$1" | head -1 | cut -d= -f2; }
# report HOST IDX: the node's REPORT line; an empty answer means the SSH hop timed out,
# not that the node is gone, so retry a few times before giving up.
report() {
  local out k
  for k in 1 2 3; do out=$(r "$1" "~/three-node.sh report $2"); [ -n "$out" ] && { echo "$out"; return 0; }; sleep 5; done
  say "no report from node$2 on $1 after 3 attempts"; fail=1
}

# wait_ingested LEADER_SPEC FOLLOWER_SPEC... : until every follower's last ingested index
# equals the leader's last cut index (and both are > 0), stable over two polls.
wait_ingested() {
  local leader=$1; shift; local prev="" cur k
  for k in $(seq 1 40); do
    sleep 3; cur=""
    lrep=$(report "${leader% *}" "${leader#* }"); lc=$(field "$lrep" lsm_lastcut)
    if [ "${lc:-0}" = 0 ] && [ "$k" -ge 5 ]; then say "leader cut no span after $k polls; nothing to wait for"; fail=1; return 1; fi
    ok=1; [ "${lc:-0}" -gt 0 ] || ok=0
    for spec in "$@"; do
      frep=$(report "${spec% *}" "${spec#* }"); li=$(field "$frep" lsm_lastingested)
      cur="$cur node${spec#* }=$li"; [ "${li:-0}" = "$lc" ] || ok=0
    done
    if [ $ok = 1 ] && [ "$cur" = "$prev" ]; then say "ingested through $lc:$cur"; return 0; fi
    prev=$cur
  done
  say "followers did not catch up: leader lastcut=$lc followers=$cur"; fail=1
}
# write_verify LEADER_IDX N VS: scanverify through the given node. An empty answer is an
# SSH timeout, not a verdict; rerunning is safe (same keys, same values).
write_verify() {
  local W k
  for k in 1 2 3; do
    W=$(r tikv240 "source ~/env.sh; /tmp/scanverify -servers $ALL -leader $1 -dnums $2 -vsize $3 -span 50 -sample 20" | grep -vE 'new pool success')
    [ -n "$W" ] && break; say "write attempt $k returned nothing (SSH), retrying"; sleep 5
  done
  echo "$W" | tail -3 | sed 's/^/    /' | tee -a "$LOG"; echo "$W" | grep -q VERIFY_OK || fail=1
}
read_direct() { # read_direct ADDR N VS
  local R; R=$(r tikv240 "source ~/env.sh; /tmp/readonly -servers $1 -dnums $2 -vsize $3 -check 300 -sample 30" | grep -vE 'new pool success')
  echo "$R" | tail -3 | sed "s/^/    [$1] /" | tee -a "$LOG"
  local why; why=$(gate_read_ok "$R" "直读 $1") || fail=1; [ -n "$why" ] && echo "$why" | tee -a "$LOG"
}

say "===== 0. build client tools ====="
r tikv240 'source ~/env.sh; cd ~/work/Nezha; export TMPDIR=~/work/tmp; go build -o /tmp/scanverify ./cmd/bench/scanverify/ && go build -o /tmp/readonly ./cmd/bench/readonly/ && echo TOOLS_OK' | tail -1 | tee -a "$LOG"

trap cleanup_nodes EXIT   # 定义在 gate.sh，理由见那里

# 启动前先清掉**我们自己**上一轮的残留。
#
# trap 只能覆盖"脚本自己出错退出"，覆盖不了被 SIGKILL：2026-09-16 有一轮因本机内存不足
# 被系统杀掉，EXIT trap 根本没机会跑，两个节点留在 tikv241 上，下一轮启动就端口冲突。
# 所以清理必须**两头都有**：挂 EXIT，并且启动前再清一遍。
# 只清 node 0/1/2（靠 pid 文件与 `-data ~/work/three-N` 匹配），碰不到别人的实验。
cleanup_nodes


say "===== 1. start three nodes (-system lsm-raft, span ${SPAN_MB} MB, $BIN) ====="
say "$(r tikv240 "$NODE_ENV ~/three-node.sh start 0 3099 30991 $VS_A $N" | tail -1)"; sleep 3
say "$(r tikv241 "$NODE_ENV ~/three-node.sh start 1 3099 30991 $VS_A $N" | tail -1)"
say "$(r tikv241 "$NODE_ENV ~/three-node.sh start 2 3100 30992 $VS_A $N" | tail -1)"
grep -c "STARTED node" "$LOG" | grep -q '^3$' || { say "a node did not start"; exit 1; }
sleep 12
say "===== 2. write $N x ${VS_A}B through node0, verify on the leader ====="
write_verify 0 "$N" "$VS_A"
wait_ingested "tikv240 0" "tikv241 1" "tikv241 2"
say "===== 3. read node1 and node2 directly (data arrived only by ingestion) ====="
for a in 192.168.1.241:3099 192.168.1.241:3100; do read_direct "$a" "$N" "$VS_A"; done
# `report` sets fail=1 when it gives up, but it is called in a command substitution --
# a subshell -- so that assignment never reaches this shell. Combined with the old
# `[ -z "$rep" ] ||` skip, an SSH timeout silently passed this gate: no report, no check,
# no failure. Check the emptiness here instead of skipping on it.
for i in 1 2; do
  rep=$(report tikv241 $i)
  if [ -z "$rep" ]; then say "no report from node$i: cannot judge lsm_replays"; fail=1; continue; fi
  [ "$(field "$rep" lsm_replays)" = 0 ] || { say "node$i replayed entries locally: $rep"; fail=1; }
done

say "===== 4. kill -9 node0 (leader) ====="
W0=$(leader_wins); W0=${W0:-0}
say "$(r tikv240 '~/three-node.sh kill9 0')"
wait_new_leader "$W0" 40
r tikv241 "grep -hE 'Candidate -> Leader|LSM-Raft\] leader: replaying' ~/work/three-1/n.log ~/work/three-2/n.log | tail -4" | sed 's/^/    /' | tee -a "$LOG"
say "===== 5. write $N x ${VS_B}B through the new leader, verify, wait for the follower ====="
write_verify 1 "$N" "$VS_B"
L=1; F=2
if r tikv241 "grep -q 'Candidate -> Leader' ~/work/three-2/n.log"; then L=2; F=1; fi
say "new leader is node$L"
wait_ingested "tikv241 $L" "tikv241 $F"
for a in 192.168.1.241:3099 192.168.1.241:3100; do read_direct "$a" "$N" "$VS_B"; done

say "===== 6. restart node0 as a follower and let it catch up by ingestion ====="
say "$(r tikv240 '~/three-node.sh restart 0' | tail -1)"; sleep 5
wait_ingested "tikv241 $L" "tikv240 0"
read_direct 192.168.1.240:3099 "$N" "$VS_B"

say "===== 7. reports ====="
# node0 was restarted in step 6, so all three must be alive here.
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do
  rep=$(report "${spec% *}" "${spec#* }"); echo "$rep" | sed 's/^/    /' | tee -a "$LOG"
  why=$(gate_report_ok "$rep" yes "node${spec#* }") || fail=1
  [ -n "$why" ] && echo "$why" | tee -a "$LOG"
done
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do r "${spec% *}" "~/three-node.sh stop ${spec#* }" >/dev/null; done
[ $fail = 0 ] && say "LSMRAFT_OK" || say "LSMRAFT_FAIL"
