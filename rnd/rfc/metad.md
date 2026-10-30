# metad: durable, highly available metadata service in ≤ 2 GiB per replica

Status: draft, 2026-10-30. P0 done. Evidence labels as in `LEDGER.md`.

## Constraints

- Each replica ≤ 2 GiB RAM (hard cap, page cache included) and ~2 vCPU; no larger machines.
- Durable: an acknowledged change survives the loss of any one replica (RPO 0).
- Highly available: automatic failover without operator action.
- 39 M files now, 100 M+ later; RAM is a bounded cache and must not grow with file count.
- About 1 000 mounted clients; bursts of thousands of small write transactions per second.
- Cost per operation not worse than the Redis-protocol engine in use today; no metadata reads from object storage.

## What the measurements rule out

| Option | Why not | Label |
|---|---|---|
| Valkey / Redis | 236 B/file in RAM; 2 GiB ends at ≈ 9 M files | RUN (`results/vm/valkey-growth*`), CALC |
| Kvrocks 2.17 | write ceiling ≈ 550 create/unlink cycles/s, flat in writers, p99 1.8 s; EXEC holds a server-wide lock through its fsync; Lua writes are not atomic | RUN (`results/vm/write-ceiling-spread*`), SOURCE |
| PostgreSQL 18 | CPU-bound at ≈ 1 200 cycles/s on 2 cores, 1–2 ms CPU per cycle, one backend per connection (OOM at 256 connections in 512 MiB) | RUN |
| Badger in-process | RAM grows with files (1.96 GB peak at 4 M); one engine per process, not shareable between mounts | RUN |
| etcd | in-memory index of all keys (≈ 120 B + key per key), 8 GiB suggested DB maximum | REPORTED (etcd v3.6 docs) |
| dqlite | database held in an in-memory VFS; RAM proportional to data | REPORTED (canonical/dqlite #368, #602) |
| rqlite | no interactive transactions | REPORTED (rqlite v10 docs) |
| FoundationDB | 4 GiB per process minimum in the docs, ~12 processes in the default topology | REPORTED (FDB 8.0 docs) |
| TiKV, CockroachDB, YugabyteDB | documented RAM floors far above 2 GiB | REPORTED |
| Garnet, Pika, KeyDB FLASH | asynchronous persistence or replication; no consensus | REPORTED |
| All-in-RAM compact encoding (JuiceFS EE style) | 2 GiB / 39 M = 55 B/file; raw attr alone is 72 B; EE reports ≈ 100 B/file planned, 50 B best case with compression, no headroom for growth | CALC, REPORTED (juicefs.com blog 2024-02-22) |

## Levers that were not used so far

1. **Group commit.** The Kvrocks failure is one fsync per transaction under a global lock. Pebble's WAL writer releases every pending synced commit with one `Sync()`, so concurrent commits share an fsync (REPORTED, `record/log_writer.go`).
2. **Batching at the consensus layer.** One Raft entry per batch of client commits: one fsync on each replica and one network round trip per batch, not per transaction.
3. **Blind atomic adds.** JuiceFS counters (`usedSpace`, `totalInodes`, `nextInode`, `nextChunk`, dir stats, parent link counts) are read-modify-write in TKV (`tkv.go` `incrBy`), so every client that flushes stats conflicts with every other client on the same keys. A server-side add without a read removes those conflicts for call sites that ignore the result.
4. **Optimistic concurrency checked at one place.** The leader validates read versions in the apply order; no locks are held across network round trips and no global lock exists.
5. **Fewer background transactions per client.** Scoped quota refresh (`feat/quota-scoped-refresh`) turns the C × Q refresh into a per-client constant; it is required for any engine.
6. **Subtree write-back lease (later).** When one client writes a subtree alone, it can hold a lease, apply creates locally and flush them in batches (Lustre WBC reported 10–20× on createmany; CephFS async dirops with delegated inode numbers). This removes most server round trips for the dominant single-writer pattern. Needs a recall protocol; not in the first version.
7. **Inode-embedded directories (later).** CephFS stores single-link inodes inside their directory fragment, so `ls -l` and lookups share cache blocks. Only worth it if cache efficiency, not size, becomes the limit.

## Design

### Shape

- `metad`: a Go server, three replicas (StatefulSet), each with a local SSD volume.
- State machine: Pebble, JuiceFS TKV key layout unchanged (`tkv.go`), so `pkg/meta` logic, dump/load and the TKV test suites are reused.
- Consensus: `etcd-io/raft` (active, used by etcd, CockroachDB, TiKV); our own log storage and transport. Dragonboat has the right state-machine API but no release since 2023.
- Client: a new `tkvClient` (`tkv.go:71`, ten methods) speaking to the leader; `metad://host1,host2,host3/volume`.

### Write path

1. The client runs the JuiceFS transaction function; reads go to the leader (`gets` batched in one RPC) and record `(key, version)`; writes are buffered.
2. Commit RPC: read set, write set, blind adds.
3. Leader checks every read version against the applied state plus the in-flight write set; conflict → the client retries (existing `txn` loop).
4. Accepted commits wait in a batcher (bounded by size or ~100–200 µs) and become one Raft entry.
5. The entry is fsynced on the leader and replicated; on quorum the batch is applied to Pebble without sync (the Raft log is the durable record; the applied index is written in the same Pebble batch), then all commits in it are acknowledged.

Each value carries the Raft index of its last write (8 B) as its version.

### Read path

Leader-lease reads from Pebble; `Get`, `Gets`, `Scan` RPCs. Follower reads are not needed at first.

### Memory budget per replica (2 GiB)

| Part | Budget |
|---|---|
| Pebble block cache (data, index, filter blocks) | 768 MiB |
| Memtables | 2 × 64 MiB |
| Raft unapplied entries in memory | ≤ 64 MiB |
| Go heap (`GOMEMLIMIT`) | ≤ 256 MiB |
| 1 000 client connections | ≈ 50 MiB |
| Page cache for SST reads | the rest, reclaimable |

### Failure handling

- Leader loss: election timeout 1–2 s, clients follow redirects.
- Follower catch-up: Pebble checkpoint (hard links) streamed as a Raft snapshot.
- Full disk: refuse writes before Pebble does, keep serving reads.
- Backups: periodic Pebble checkpoint to object storage (rare, batched) plus `juicefs dump`.

## Results so far

P0, Pebble alone with the JuiceFS TKV layout, 39 M files, 2 GiB cgroup including page cache, 2 cores (RUN, `LEDGER.md` "Pebble with the JuiceFS TKV layout"):

- 3.0 GB of SST (≈ 77 B/file), no OOM kill at the cap.
- Lookup over all 39 M uniformly: p50 83 µs, p99 2.0 ms, 49 µs CPU; hot 10 k: p50 12 µs, 13 µs CPU.
- Synced creates: 1.8 k txn/s with one writer, 22 k txn/s with 64 writers (p99 8 ms), 64–129 µs CPU per transaction.

Not yet measured: network, replication, conflict checks, the JuiceFS client on top (P1, P2).

## Plan

| Step | Content | Gate |
|---|---|---|
| P0 | Pebble alone, 39 M files, JuiceFS TKV layout, 2 GiB cgroup, 2 cores | fits; cold lookup p99 < 10 ms; synced write ceiling far above 1 200 txn/s |
| P1 | single-node `metad` + `tkvClient`, no Raft | TKV test suites pass; M2 probe and write ceiling vs Valkey on the same VM |
| P2 | Raft, three replicas, batching | kill -9 of leader and followers: no acknowledged write lost (linearizability check with porcupine), failover time measured |
| P3 | 39 M files + 1 000 simulated clients, scoped quota refresh | engine CPU per op flat in client count; RAM at cap |
| P4 | migration from Redis-protocol engine via dump/load, shadow run | owner decision |
| Later | blind adds in `kvTxn`, server-side lookup, subtree leases | each measured separately |

## Risks

- A replicated database is new code to own: OCC, Raft log, snapshots, membership. Mitigation: reuse `etcd-io/raft`, linearizability checking, the existing TKV suites, fault injection from M3.
- Network round trips per operation: a create is at least two RPCs (reads, commit). In-cluster RTT 100–300 µs keeps it near today's 0.8 ms p50; a server-side lookup can cut stat to one RPC.
- Hot counters until blind adds land: conflicts and retries under many writers.
