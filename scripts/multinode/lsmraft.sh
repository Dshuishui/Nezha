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
r() { ssh -o ConnectTimeout=40 -o ServerAliveInterval=15 "$@" 2>/dev/null; }
say() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }
ALL="192.168.1.240:3099,192.168.1.241:3099,192.168.1.241:3100"
NODE_ENV="BIN=$BIN SYSTEM=lsm-raft EXTRA='-sstSpanMB $SPAN_MB'"
fail=0

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
  echo "$R" | tail -3 | sed "s/^/    [$1] /" | tee -a "$LOG"; echo "$R" | grep -q FAILOVER_VERIFY_OK || fail=1
}

say "===== 0. build client tools ====="
r tikv240 'source ~/env.sh; cd ~/work/Nezha; export TMPDIR=~/work/tmp; go build -o /tmp/scanverify ./cmd/bench/scanverify/ && go build -o /tmp/readonly ./cmd/bench/readonly/ && echo TOOLS_OK' | tail -1 | tee -a "$LOG"

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
for i in 1 2; do rep=$(report tikv241 $i); [ -z "$rep" ] || [ "$(field "$rep" lsm_replays)" = 0 ] || { say "node$i replayed entries locally: $rep"; fail=1; }; done

say "===== 4. kill -9 node0 (leader) ====="
say "$(r tikv240 '~/three-node.sh kill9 0')"; sleep 20
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
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do
  rep=$(report "${spec% *}" "${spec#* }"); echo "$rep" | sed 's/^/    /' | tee -a "$LOG"
  echo "$rep" | grep -qE 'races=0 err_lines=0' || fail=1
done
for spec in "tikv240 0" "tikv241 1" "tikv241 2"; do r "${spec% *}" "~/three-node.sh stop ${spec#* }" >/dev/null; done
[ $fail = 0 ] && say "LSMRAFT_OK" || say "LSMRAFT_FAIL"
