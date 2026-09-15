# Snapshot Replication

Status: implemented and verified (2026-09-15).
Commits: `af89f8a` (payload kind on the wire), `a6d9558` (partition refcounts),
`25cb3da` (making a snapshot), `6357fe1` (installing one), `17186df` (empty
AppendEntries consistency check), `8f44efb` (per-peer progress), `2b4f708`
(bounded truncation), `7ba0a7b` (crash scenarios E and F).
Verification drivers: `scripts/test/snapshot-e2e.sh`,
`scripts/test/snapshot-crash.sh`, `scripts/test/slow-follower.sh`.

## Problem

Two failures, both measured rather than assumed.

**The leader's memory was pinned by its slowest follower.** `compactLog` clamped its
compaction point to `min(matchIndex)` across peers. The reason was sound — discarding an
entry a follower has not replicated leaves it permanently unable to catch up, because
there was no way to ship state — but the cost was that `rf.log`, which is resident in
memory, grew with however far behind the slowest replica was. Measured on three nodes:
stopping one follower for 40 s while writing 150000 entries of 256 B took the leader's
in-memory log from 5000 entries to 150000, and RSS from 114 MB to 254 MB. At 100 GB of
64 B values with a replica 10% behind, that is roughly 30 GB — the process is killed, and
nothing in the log says why.

The three-node case makes it worse rather than better: commit waits for
`sortedMatchIndex[len(peers)/2]`, the median, which for three nodes is the leader plus
whichever follower is faster. The slower one is never waited for and the write path
applies no backpressure to it, so a small persistent difference in hardware accumulates
without bound. On the lab machines two nodes held 4.6 GB and the third 463 MB, and the
third was *active* the whole time.

**A replica that lost its log was recorded as fully caught up.** Separately, the
AppendEntries handler answered success to any request carrying no entries without checking
`PrevLogIndex`/`PrevLogTerm`. The leader calls the same path when it has nothing new to
send, so a replica whose disk was wiped answered "yes" to every probe and the leader booked
it at the leader's own index. It never received its data back, and it was inside the commit
quorum while missing entries the leader believed replicated — a safety violation, not a
performance bug.

## What a snapshot is

Four things that together are "the complete state as of some index":

| Part | Property |
|---|---|
| `snapshot.json` | the manifest: the point, the partition list, how much of the log to take, whether a store export is present |
| partition files + sidecar sparse indexes | already-collected data; immutable once sealed, streamed from the original files |
| `store.sst` | the store (key → offset in the current log), written with `SstFileWriter` so it can be ingested |
| a prefix of the current value log | the increment since the last GC round; append-only, so a prefix is a consistent cut |

This is where partitioned GC pays off a second time. A general state machine has to stop
writes or copy-on-write to snapshot itself; ours does not, because after GC the bulk of the
state is a set of sealed, immutable partition files. The Raft dissertation names the same
property for LSM-style log cleaning in chapter 5: "runs are immutable, so there is no
concern of the runs being modified during the transfer". Worth a paragraph in the paper's
discussion, as a consequence of the GC design rather than a contribution of its own.

The snapshot point is the current log file's base (`rf.fileBaseIndex`): everything earlier
has been migrated into partition files, everything later is in the stretch of log shipped
alongside. The receiver therefore ends up with the log through the cut, and the leader
resumes ordinary replication at the next index with no separate catch-up mechanism.

Only the store export is copied locally, and RocksDB leaves no choice — it refuses to
ingest its own flush output. That copy is bounded by the GC threshold rather than by the
dataset: each GC round switches to a new store and deletes the superseded one, so the
current store only holds rows written since the last round. Partition files and the log
prefix are streamed from where they already are.

Routine snapshots cost nothing and are not materialised at all. The applied index is
written in the same WriteBatch as the data row, so "what is already on disk constitutes an
index point" is true without doing any work — dragonboat's `saveDummy` for on-disk state
machines has the same semantics. State is only materialised when it is about to be sent.

## Transport

`InstallSSTable` already was a general chunked file stream, built for the LSM-Raft
baseline: client-streaming, reassembly by name and offset, Raft's term rules on the header,
a deadline that scales with the payload. It gained one field, `Kind` (`SPAN` / `SNAPSHOT`),
rather than a second RPC that would duplicate all of that.

`Kind` is an explicit field, not a convention over the existing index fields. The two
payloads differ in exactly one requirement on the receiver — a span needs the follower
applied through `Start-1`, a snapshot needs nothing, which is the entire point of a
snapshot — and encoding that as a sentinel in a field that means something else is the kind
of implicit agreement that eventually gets read the wrong way round.

Everything else a snapshot implies (its point, its applied index, its partition list)
travels in the manifest file, so there is one source of truth rather than a wire field the
installer would have to choose between.

A per-file byte limit was added for the log prefix: the file is being appended to, and its
length must be the length at the moment the rest of the snapshot was read.

## Sending: per-peer progress

The leader previously held only `nextIndex` and `matchIndex` per peer, so its only response
to a peer it could not serve was to skip it every round and retry every round. It now has
the minimal state machine etcd/raft has:

```
replicating   send log entries normally
snapshotting  a snapshot is in flight; send no entries until it reports back
```

The trigger is the paper's, and deliberately not a threshold: the leader resorts to a
snapshot when it has already discarded the next entry the follower needs. Any separate
"how many entries behind" knob would be a second, independently tunable definition of
too-far-behind, free to disagree with the one the truncation policy implies.

Three requirements, each load-bearing:

1. **Every path reports back.** `finishSnapshot` is reached on a failed source, a failed
   transfer, a stale term and a refused install alike. Without it the peer stops in
   `snapshotting` forever, receiving neither entries nor snapshots — etcd's
   `ReportSnapshot` documentation calls that limbo, and it is exactly the symptom this
   work removes, reintroduced in another form.
2. **Peers that are not recently active are skipped**, as etcd checks `!pr.RecentActive`.
   Sending gigabytes to an unreachable node only occupies a link that is also carrying
   entries. Any reply marks a peer active, heartbeats included — which matters, because a
   peer far enough behind to need a snapshot receives no entries, so heartbeats are its
   only evidence of life.
3. **Compaction is skipped while a snapshot is in flight**, and for 30 s after (etcd's
   `releaseDelayAfterSnapshot`). Truncating inside that window leaves the catching-up node
   landing before the new first index again; CockroachDB's note on it reads "likely
   entering a never ending loop of snapshots" (cockroachdb#8629).

One transfer at a time, rate-limited to 100 MiB/s by default (`-snapshotRateMB`), matching
TiKV's `snap-io-max-bytes-per-sec` default and the 110 MB/s our GC migration achieves, so
it is not a new bottleneck. The limit is not "lower is safer": the sender blocks log
truncation while it runs, which is why CockroachDB puts a *floor* under its snapshot rate.

Partition files are refcounted for the duration. The sender opens them one at a time as it
walks the file list, so a transfer that takes minutes is exposed for minutes: if GC retired
a partition the sender had not reached, the next `os.Open` would fail with the snapshot half
sent. A pin defers the unlink only — the descriptor pools are still left to their
finalizers, because closing them would look like fixing the known pool leak, but that leak
was measured not to be one (fd count oscillates between 36 and 68 across 8 GC rounds), while
a reader that had just taken the pointer would hit a closed pool.

## Installing

All-or-nothing, with `kv_state.json` as the switch: it is written last, the same order GC
commits in. A crash before it restarts the node as what it was, the snapshot simply not
installed, and the leader re-sends; a crash after it leaves the new state complete.

For that to hold, nothing received may overwrite a file the old state file still points at,
so everything lands under snapshot-specific names — `RaftState_snap<idx>.log`,
`RaftState_sorted_snap<idx>.pN`, `dbfile/keyIndex_snap<idx>`. Using the leader's own names
would collide whenever the two nodes are on the same GC round, and would destroy the old log
while the old state file still referenced it, which is not recoverable. Files land by rename
from an incoming directory inside the data directory, so it is one filesystem and the rename
is atomic. The price of the separate names is orphaned files after an interrupted install,
collected at startup by comparing against what the loaded state actually references — by
reference and not by name, because after a *successful* install the referenced files are
precisely the ones with `snap` in their names.

The store is always ingested into a freshly created store, and the target path is removed
first. Ingest means "add as the newest data", so merging into the node's existing store
would leave keys from its old history behind with offsets into a log file that is no longer
current — another record's value, no error. The same applies to a retry of an interrupted
install, which lands on the same path.

Replacing the state machine needs readers held off, and `kvs.mu` is not enough: one read
uses the persister, the current log and the partition set together, and an install swaps all
three. Guarding each assignment individually still lets a read pair a new persister with an
old log path. `KVServer.stateMu` closes that: reads and apply take it for read, an install
takes it for write. GC is excluded differently — it also swaps those three things, but a
round can run for minutes, and an install waiting on the write lock would block reads for
that whole time (Go's `RWMutex` stops admitting readers once a writer waits), so GC and
install refuse rather than wait, with the leader's resend backoff supplying the retry.

Transfers are idempotent retries, never resumable, which is what all three of etcd, TiKV and
CockroachDB do (TiKV replays from the start after a receiver crash; CockroachDB does one
atomic ingest-and-excise). So half-received state is worth nothing: the incoming directory
is cleared at startup and the leader's export directory with it.

## Bounded truncation

Three tiers. The first two are CockroachDB's `computeTruncateDecision`:

| Follower | Protection |
|---|---|
| recently active | protected to its `Match`, however large the log grows |
| not recently active | protected to its `Match` only while the log is within budget |

The distinction matters in both directions: a merely slow active replica should not be
pushed into a snapshot, which costs far more than shipping entries, and a replica that is
already gone should not be able to exhaust the leader's memory.

The third tier is ours: the compaction point may never fall more than the budget behind
`lastApplied`, whoever is behind it. CockroachDB does not need one because it has a proposal
quota (`RaftProposalQuota = threshold/2`) that backpressures the slowest *active* replica,
and because its Raft log lives on disk with a separately bounded entry cache. We have
neither — the log is resident in memory, the write path applies no backpressure, and commit
waits only for the median — so "active means protected unconditionally" is unbounded memory
here, which is the failure being fixed.

The budget is in bytes (`-raftLogBudgetMB`, default 256 MiB, the same order as TiKV's
`raft-log-gc-size-limit` of 192 MiB). Entry counts will not do: one entry is about 280 B at
64 B values and about 16.2 KB at 16 KB values, so a one-million-entry budget means 268 MB in
one column of our own sweep and 16 GB in another. Bytes in turn force an exact accounting,
so `rf.logBytes` is maintained and four functions in `compact.go` are the only places
`rf.log` is mutated — scattered increments would eventually miss one, and missing one is
silent: the count reads low, the budget stops binding, the memory is unbounded again. A unit
test recomputes the total after every kind of mutation.

`budgetFloorLocked` walks back from the end of the log accumulating bytes until the budget is
met, so its work is proportional to the budget rather than to the log, which is what lets it
run inside the compaction loop under `rf.mu`. When the whole log fits the budget it returns
`lastIncludedIndex`, i.e. no constraint, so the third tier needs no separate over-budget
test.

**Order of implementation was not free.** Bounded truncation had to land last. Opening it up
before snapshots worked would have traded an eventual OOM for an immediate permanent stall,
and the stall is the worse of the two: an OOM crashes and is visible, while a replica that
can never catch up is silent and the numbers keep looking fine.

## Verification

| Driver | What it shows |
|---|---|
| `slow-follower.sh` | same script, same scale as the original reproduction: retained entries 5000 → 5000 (was 5000 → 150000), RSS 114 → 126 MB (was 114 → 254 MB), one `[LOG-TRUNCATE]` at 19 MB against a 16 MB test budget, and on `SIGCONT` the stalled replica repaired by a 23 MB snapshot made in 90 ms |
| `snapshot-e2e.sh` | a replica's data directory wiped and restarted: 6 partitions and 17 MB shipped in 229 ms, installed in 31 ms with all six sidecar indexes read rather than rebuilt (210 µs), 400+ records verified, `lost-keys.py` counting 60000 distinct keys on disk with 0 lost, and ordinary replication resumed afterwards |
| `snapshot-crash.sh` | scenarios E and F (see `docs/crash-recovery.md`) |
| unit tests | the progress machine (7 cases), the truncation tiers and byte accounting (5 cases), the store export round trip and the log cut (5 cases), the empty-AppendEntries consistency check |

`crash-recovery.sh`, `gc-rounds.sh`, `leaseread-e2e.sh` and `gate-audit.sh` are unchanged
and still pass, which is what says the stricter consistency check and the new locking did
not disturb ordinary operation.

Two verification notes worth keeping, because both cost time:

- The write tool and the verification tools must agree on how a value is derived from its
  key. `randwrite_goroutine` writes a fixed generated string; `scanverify` and `readonly`
  expect a key-derived value. Mixing them reports every single record as wrong, which reads
  exactly like a snapshot that corrupted the data.
- Per-record verification is sampling. `lost-keys.py` counting keys on disk is the only
  full-coverage evidence that nothing was lost, and for a mechanism that moves state in
  bulk it is the judgement that matters.

## Known gaps

- The transfer window is entered in tests by lowering `-snapshotRateMB`; there is no
  fault-injection hook inside the receive loop. That is deliberate (a production knob beats
  a test switch) but it does mean the window cannot be hit at full rate.
- Reads of `kvs.lastPartitions`, `kvs.currentLog` and `kvs.persister` are now serialised
  against an install by `stateMu`, but the GC loop is excluded by a pair of refusal flags
  rather than by the lock. A GC round and an install therefore cannot overlap at all, which
  is stricter than necessary.
- `snapshotNeeded` is evaluated per replication round, so the decision to snapshot can lag
  the condition by one round of the replication loop's idle tick.
