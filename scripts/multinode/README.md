# Multi-node verification scripts

Correctness drivers for the two lab machines (`tikv240` hosts node0, `tikv241` hosts node1
and node2). They assume the repository is checked out at `~/work/Nezha` on each server with
the toolchain environment in `~/env.sh`, and that `ssh tikv240` / `ssh tikv241` work without
a password from the driving machine. The two servers cannot reach each other over SSH, so all
orchestration runs from the driving machine.

| Script | Runs on | Purpose |
|---|---|---|
| `rep-node.sh` | both servers, `~/` | node control for the two-node steady-state runs: `start ROLE VS N [normal\|race]`, `stop`, `report` |
| `two-node-rounds.sh` | driver | repeated two-node rounds: write and verify, wait for GC to settle, read the leader and the follower directly, collect logs |
| `three-node.sh` | both servers, `~/` | three-node control: `start IDX PORT IPORT VS N`, `kill9`, `restart` (same data directory, exercises recovery), `stop`, `report`, `recoverlog`; `BIN=race\|normal`; `GC_PAUSE_MS=` pauses GC after the file switch |
| `failover.sh` | driver | kill the leader, wait for the election, read both survivors, write through the new leader, read again |
| `failover-loop.sh` | driver | run `failover.sh` N times |
| `recover.sh` | driver | crash recovery: `MODE=restart` restarts a killed follower and a killed leader; `MODE=midgc` kills a follower inside the GC window and restarts it |

Deploy the server-side scripts with `scp three-node.sh tikv240:~/ && ssh tikv240 chmod +x
~/three-node.sh` (and the same for `tikv241` and `rep-node.sh`). The drivers call them as
`~/three-node.sh` and `~/rep-node.sh`.

Reading a follower directly works because the read path currently has no leader check (see
`TODO-avp.md`). That gap is what lets these scripts verify a follower's local state.
