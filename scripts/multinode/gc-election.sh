#!/bin/bash
# Does GC starve the heartbeat path? Three nodes, 4 KB values, ~10 GB written, GC at 2 GB.
#
# The node logs the pieces this needs but not on one clock: role changes come from DPrintf
# (dated), while the GC banners and "3秒没有收到" come from fmt.Printf (undated). TS=1 in
# three-node.sh stamps every line as it leaves the process, so the three can be lined up.
#
# Reads as reproduced when, per node: silent>0 or elections>0, and the timeline shows the
# quiet window sitting inside a GC round rather than scattered across the run.
#
# Env: N (records, default 2500000), VS (4096), GCGB (2), CNUMS (100), BIN (normal).
set -u
cd "$(dirname "$0")"
N=${N:-2500000}; VS=${VS:-4096}; GCGB=${GCGB:-2}; CNUMS=${CNUMS:-100}; BIN=${BIN:-normal}
ROUNDS=${ROUNDS:-2}; SETTLE=${SETTLE:-60}
ALL="192.168.1.240:3099,192.168.1.241:3099,192.168.1.241:3100"
LOG=gc-election.log; : > "$LOG"
r() { ssh -o ConnectTimeout=40 -o ServerAliveInterval=15 "$@" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
NODE_ENV="BIN=$BIN TS=1 SYSTEM=nezha EXTRA='-gcThresholdGB $GCGB'"
# report HOST IDX: an empty answer is an SSH timeout through the four-hop jump, not a
# verdict about the node, so retry before believing it.
report() {
  local out k
  for k in 1 2 3; do out=$(r "$1" "~/three-node.sh report $2"); [ -n "$out" ] && { echo "$out"; return 0; }; sleep 5; done
  say "no report from node$2 on $1 after 3 attempts"
}

say "=== 1. start three nodes (BIN=$BIN, gcThresholdGB=$GCGB, TS=1) ==="
r tikv240 "$NODE_ENV ~/three-node.sh start 0 3099 30991 $VS $N" | tee -a "$LOG"
r tikv241 "$NODE_ENV ~/three-node.sh start 1 3099 30991 $VS $N" | tee -a "$LOG"
r tikv241 "$NODE_ENV ~/three-node.sh start 2 3100 30992 $VS $N" | tee -a "$LOG"
sleep 12

say "=== 2. write $N x ${VS}B (~$((N*VS/1073741824)) GB) through $CNUMS clients ==="
say "    GC fires each time the value log passes $GCGB GB; rounds are capped at two, so"
say "    the run stops ${SETTLE}s after round $ROUNDS rather than writing all $N records."
r tikv240 "source ~/env.sh; nohup /tmp/randwrite -servers $ALL -cnums $CNUMS -dnums $N -vsize $VS \
    < /dev/null > /tmp/randwrite-gc.log 2>&1 & echo WRITER \$!"  | tee -a "$LOG"

# Wait for the last GC round on the leader's host, then let the cluster settle: the
# elections this is looking for happen at the tail of a round, and a few seconds after it
# are what show whether the cluster recovers on its own.
for k in $(seq 1 240); do
  sleep 15
  done0=$(r tikv240 "grep -c '轮垃圾回收完成' ~/work/three-0/n.log" 2>/dev/null)
  done1=$(r tikv241 "grep -c '轮垃圾回收完成' ~/work/three-1/n.log" 2>/dev/null)
  done2=$(r tikv241 "grep -c '轮垃圾回收完成' ~/work/three-2/n.log" 2>/dev/null)
  [ $((k % 8)) = 0 ] && say "    gc rounds so far: node0=${done0:-?} node1=${done1:-?} node2=${done2:-?}"
  if [ "${done0:-0}" -ge "$ROUNDS" ] && [ "${done1:-0}" -ge "$ROUNDS" ] && [ "${done2:-0}" -ge "$ROUNDS" ] 2>/dev/null; then
    say "    all nodes finished round $ROUNDS; settling ${SETTLE}s"
    sleep "$SETTLE"; break
  fi
done
r tikv240 "pkill -f randwrite; tail -2 /tmp/randwrite-gc.log" 2>/dev/null | sed 's/^/    /' | tee -a "$LOG"

say "=== 3. per-node report ==="
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do
  report "${spec% *}" "${spec#* }" | tee -a "$LOG"
done

say "=== 4. timeline (GC rounds vs heartbeat silence vs role changes) ==="
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do
  r "${spec% *}" "~/three-node.sh timeline ${spec#* }" | tee -a "$LOG"
done

say "=== 5. nodes left running for inspection; stop with three-node.sh stop IDX ==="
