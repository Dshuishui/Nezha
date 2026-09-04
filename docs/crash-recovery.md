# Crash Recovery

Status: implemented and verified (2026-09-04).
Commits: `5aa8fdf` (Raft: rebuildable log, durable term/vote/base), `e8ebaab`
(applied index stored with the data), `4a3d9ef` (KV state file, startup recovery,
interrupted-GC resume). Verification driver: `scripts/multinode/recover.sh`.

## Problem

Before this work a restarted node came back blank. Raft's term, vote, log and offset
queue lived only in memory; nothing on startup read the RocksDB index or the value-log
files back. Because the leader compacts its in-memory log, a blank node could never catch
up either.

## Goals

1. After `kill -9`, a node restarted on the same data directory recovers to its
   pre-crash state, catches up as a follower, and can take part in elections.
2. Recovery uses local disk only (no data transfer from peers in this version).
3. The write path is not slowed down: new persistence happens only on elections and on
   GC file switches.

Out of scope: a node that has fallen behind the leader's in-memory log (that needs
InstallSnapshot, i.e. shipping RocksDB plus sorted files). Leader compaction is bounded by
`min(matchIndex)`, so a node that is merely down does not lose the entries it is missing.

## What is persisted, and where

| State | Before | Recovered from | Written when |
|---|---|---|---|
| `currentTerm`, `votedFor` | memory | `raft_state.json` (atomic write + fsync) | every change, before the RPC is answered or sent |
| index/term just before the oldest retained log file | memory | `raft_state.json` | when GC deletes an old log |
| Raft log tail | memory | replayed from the retained log files (each record holds index, term, key, value) | no extra write |
| pending `Offsets` / `offsetVersions` | memory | rebuilt during the same replay for `index > applied` | no extra write |
| applied index | memory | RocksDB key `\x00applied_index`, written in the same WriteBatch as the data row | every apply, no extra fsync |
| `commitIndex` | memory | starts at the applied index; the leader's heartbeats advance it | — |
| GC round, current log/index paths, latest sorted file, in-flight old log/index | memory | `kv_state.json` (atomic write + fsync) | GC switch and GC completion |
| sparse index and inline cache of the sorted file | memory | rebuilt from the sorted file at startup (cache starts cold) | no extra write |
| RocksDB index | disk | opened at the path named in `kv_state.json` | already durable |

The applied index lives inside RocksDB rather than in a state file because it must agree
exactly with the rows that made it into the store: a crash between two files would replay
or skip an entry. One WriteBatch gives atomicity for free, and under `-syncWAL` the marker
has the same durability as the data.

## Changes to the on-disk log

- Leader no-op entries (`TermLog`) are written to the log file as records with an empty
  key (`keySize == 0`; real keys are always `KeyLength` bytes after padding), on both leader
  and follower. Without them the file had index gaps and could not be replayed. Every entry
  now owns an offset slot, which also makes the follower's conflict-overwrite offset
  arithmetic exact.
- A conflict overwrite truncates the file at the end of the new content. The old code moved
  the write position back to the previous end of file, leaving stale bytes that a sequential
  replay would read as records.

## Startup sequence

```
1. read kv_state.json      -> GC round, current log/index paths, sorted file, in-flight GC
   (absent = fresh node: open the initial index, write an initial state file)
2. open the current RocksDB -> read \x00applied_index
   if a GC was in flight: open the old index too, take the larger applied index
3. rebuild the sorted-file index (if a round has completed) and set the read-path flags
4. raft.Make(stateFile)    -> loads term, vote, log base; starts nothing
5. RecoverLog(files, applied): scan the retained log files oldest first
     every record  -> rf.log entry (Term, Key, Value, Index; empty key = TermLog)
     index > applied -> Offsets / offsetVersions (offset within its file, file version)
   indices must be contiguous; first index - 1 must equal the persisted base
   a half-written final record in the last file is truncated away
6. SetCurrentLogVersioned(current log, round) -> appends continue at the file end
7. StartLoops: election, replication, apply, compaction, gRPC
8. client-facing server starts; an interrupted GC round is redone in the background
```

## GC ordering

Recovery needs `kv_state.json` written at the file switch, not only at completion: once the
switch takes effect new writes go to the new file, so a crash during migration leaves the
log split across two files and the state must say so.

```
switch files -> write kv_state (in flight) -> drain pending applies for the old version
-> migrate -> fsync sorted file -> PersistLogBase + write kv_state (done) -> delete old log
```

A crash before the second state write restarts the node with `gc_in_progress = true`: both
log files are replayed, the partial sorted file is discarded, and the round is redone from
the migration step (both rounds are re-entrant). A crash after it leaves only an unreferenced
old file behind.

## Verification

Three-node cluster built with `-race`, 20000 keys of 1 KB, GC threshold set so both rounds run.

| Scenario | Result |
|---|---|
| follower `kill -9`, 20000 keys rewritten while it is down, restart | recovered from disk, caught up, served the new values within 10 s |
| leader `kill -9`, new leader elected in 2 s, 20000 keys rewritten, old leader restarted | rejoined as follower, caught up, served the new values within 10 s |
| follower `kill -9` between GC switch and migration (`NEZHA_GC_PAUSE_MS`) | detected the interrupted round, redid it in 138 ms, later ran round 2 normally, all keys correct |

No data-race reports on any node in any scenario. Unit tests cover the log rebuild, recovery
across GC files, truncated tails, gap and base-mismatch rejection, overwrite truncation, the
hard-state round trip and the applied-index batch.

## Known gaps

- Conflict truncation on a follower is exercised only by a unit test; the cluster scripts
  kill the leader after the writes finish, so no uncommitted tail is left behind.
- No InstallSnapshot: a node that falls behind the leader's compaction point cannot catch up.
- After the second GC round the first round's sorted file (`RaftState_sorted_1`) is left on
  disk. Pre-existing behaviour; recovery does not depend on it.
