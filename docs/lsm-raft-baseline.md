# LSM-Raft Baseline (`-system lsm-raft`)

Status: implemented and verified on three nodes (2026-09-05).
Code: `cmd/nezha/lsmraft.go` (policy), `internal/raft/sstable.go` (transport),
`internal/raft/persister_sst.go` (SSTable export/import), `api/raftrpc/raft.proto`
(`InstallSSTable`). Verification driver: `scripts/multinode/lsmraft.sh`.

## What LSM-Raft is

LSM-Raft (Zhang, Tan, Song, Huang, Wang, *Proc. ACM Manag. Data* 3(6), 2025, DOI
10.1145/3769805) generalises Raft's replicated log into a sequence of *elements*: raw
entries or compacted SSTables that each cover a span of log indexes. The leader picks, per
follower, whether to send entries or an SSTable, using a cost model of transmission versus
the follower's local compaction work. A follower that receives an SSTable ingests it into
its LSM-tree and skips the write path (WAL, memtable, flush) for that span. The leader's
own writes are unchanged. The paper reports 3–10 % more throughput and about 30 % less
CPU, compaction and network on TiDB and Apache IoTDB.

No implementation is public. The paper describes patches to TiKV's RaftStore and IoTDB's
PipeConsensus and links to no code; a search of GitHub finds only unrelated projects with
the same name. The baseline here is therefore the smallest faithful equivalent that can be
built on Nezha's stack (Go, RocksDB via grocksdb).

## Design

The Raft log is untouched. Every entry is still replicated, acknowledged and persisted in
the value log on every node, so elections, commit and crash recovery work exactly as in
`original`. What changes is how a **follower's state machine** is fed.

**Leader.** Applies entries to RocksDB exactly like `original`. In addition it keeps the
rows of the *open span* in memory (store key → value, last version per key). When the span
holds `-sstSpanMB` of values (default 32) or no write has arrived for `-sstIdleMs`
(default 1000), it writes the rows with RocksDB's `SstFileWriter` as one file, including
the applied-index marker `"\x00applied_index" = SpanEnd`, labels the file with
`[SpanStart, SpanEnd]` and keeps it in a catalog of the last 16 spans. One goroutine per
follower streams spans in order (`InstallSSTable`, 1 MB chunks) and follows the follower's
answer.

RocksDB refuses to ingest its own flush output ("External file version not found"; only
`SstFileWriter` files carry the required table property), so the span file is *built*
rather than reused from a flush. This costs the leader one extra sequential write of the
span's data. It is the only deviation on the leader side from the paper's design; the
paper's leader reuses compaction output at no extra cost.

**Follower.** Never replays an entry into RocksDB while a leader is shipping. Committed
entries arriving from Raft are held in memory; when a span `[a, b]` arrives the follower
ingests it (`IngestExternalFile`, files moved into the store, global sequence number) and
drops the held entries up to `b`. Its RocksDB therefore sees neither WAL, memtable nor
flush for replicated data — the follower-side saving LSM-Raft describes. Compaction of the
ingested files happens as usual.

This is the "always ship an SSTable" end of the paper's cost-aware policy. Under a
sustained write load the followers are the ones the policy would switch to SSTables, so
the baseline represents the mechanism at its full effect on followers.

## Correctness

A follower's store must equal what replaying the log would produce. Three rules give that:

1. **Ordered ingestion.** A span `[a, b]` is ingested only when
   `a-1 <= lastApplied < b`. Ingestion is therefore strictly in log order and every
   ingested file receives a higher sequence number than all data before it, so the last
   version of a key is always the one from the latest span — the same result as replaying.
   A span the follower has already passed (`lastApplied >= b`) is skipped; a span too far
   ahead (`lastApplied < a-1`) is refused with `GAP` and the follower's position, and the
   leader continues from an older span.
2. **Marker travels with the data.** The span file carries `applied_index = b`, so after
   ingestion `GetApplied()` returns `b` and the existing recovery path (`recoverOrInit`,
   `RecoverLog`) needs no change. A restarted follower resumes from its last ingested span.
3. **Fallback to replay.** When the leader has nothing older than the span it offers
   (`OldestAvailable == a`), the follower replays its held entries up to `a-1` itself and
   then ingests. Held entries are also replayed when the node becomes leader (its store
   must be complete before it serves reads and cuts spans) and when they exceed
   8 × span size without a span arriving, as a liveness guard.

Partially replayed spans are safe: if a follower replayed entries `a..k` (k < b) and then
ingests `[a, b]`, the file's versions are the same or newer than the replayed ones and win
by sequence number (`TestSpanOverReplayedRows`).

Role changes: a new leader replays its held entries first, then applies normally; the
former leader, back as a follower, drops its open span and catalog (they describe its
store at its own term and would send older versions if re-offered later), skips spans it
has already applied and ingests from there on. An entry Raft delivers after the span that
covers it was ingested is dropped rather than held, so a later replay cannot resurrect an
older value. Term rules apply to `InstallSSTable` like to any Raft RPC (stale leaders are
refused; a newer term demotes the receiver).

## Known trade-offs

- A follower's visible state lags the leader by at most one span. Reads are served by the
  leader; the verification scripts read followers only after the last span has landed.
- The leader keeps up to one span of values in memory and writes each span once more as
  an SSTable. Followers keep up to one span of held entries in memory.
- The catalog holds the last 16 spans. A follower further behind than that replays the
  missing prefix itself (rule 3) and then continues by ingestion.
- No cost model: all followers always receive SSTables. The paper's adaptive choice
  between entries and SSTables is not reproduced.

## Verification

`scripts/multinode/lsmraft.sh` runs three nodes with `-system lsm-raft` (race build,
4 MB spans so a 20 MB run cuts several spans), writes and verifies through the leader,
waits until both followers ingested the leader's last span, reads both followers directly
(their data arrived only by ingestion; `lsm_replays=0` is asserted), kills the leader,
writes through the new leader (a former follower that must replay its held entries),
reads the survivor, restarts the killed node as a follower and reads it after it caught
up by ingestion. Every node must report 0 data races and 0 error lines.
