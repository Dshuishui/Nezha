#!/bin/bash
# 连跑 N 次 -race 故障切换，汇总每次的结果与选举耗时。
set -u
cd "$(dirname "$0")"
RUNS=${RUNS:-3}
for i in $(seq 1 "$RUNS"); do
  echo "################ RUN $i / $RUNS ################"
  BIN=race KEEP=0 ./failover.sh
  cp failover.log "failover-run$i.log"
  k=$(grep -oE '\[[0-9:]+\] KILLED' failover-run$i.log | grep -oE '[0-9:]+' | head -1)
  l=$(grep -oE '[0-9]{2}:[0-9]{2}:[0-9]{2} RaftNode\[[0-9]\] Candidate -> Leader' failover-run$i.log | tail -1)
  echo "RUN $i: $(grep -oE 'FAILOVER_RACE_(OK|FAIL)' failover-run$i.log | tail -1) killed=$k newleader=[$l] races=$(grep -oE 'races=[0-9]+' failover-run$i.log | tr '\n' ' ')"
  sleep 5
done
echo LOOP_DONE
