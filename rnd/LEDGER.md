# Evidence ledger

Labels: USER = measured by the owner; SOURCE = read in code at the pinned SHA; RUN = measured on this stand (command and raw output in `rnd/results`); REPORTED = upstream report not reproduced; INFERENCE; PROPOSAL; UNKNOWN.

Pins: `rnd/PINS.md`. Stand: 4 vCPU, 15 GiB, Valkey 9.1.1 with `appendfsync always`, no replicas. Replica memory is not measured; each replica holds a full copy.

## Memory per file (R0)

| Item | Label | Value |
|---|---|---|
| Valkey per file, production corpus | USER | ~234 B, ~2 keys |
| Valkey per small file, 1 chunk, dirs of 100 | RUN | 226 B, 2.02 keys (`i` 97 B, `c` 72 B, dentry 21 B, main dict ~35 B) |
| dirs of 10 / 1000 | RUN | 254 B / 244 B |
| empty file | RUN | 137 B |
| 16-chunk file | RUN | 1513 B |
| 5 slices per chunk | RUN | 339 B |
| 5% symlinks, 2% hardlinks, 10% xattr | RUN | 241 B |
| 11 000 dir quotas on 1 M files | RUN | 227.5 B |

Cost is per-key overhead, so packing in RAM is a constant factor, not a fix (INFERENCE).

## Quotas

| Item | Label | Value |
|---|---|---|
| Every client loads all dir/user/group quotas at start and every heartbeat, no subdir scoping | SOURCE | `base.go` NewSession/refresh, `quota.go` loadQuotas |
| v1.4.2 loads with per-row HGET; main uses HSCAN (#7405 per brief, attribution not verified in shallow history) | SOURCE | `redis.go` doLoadQuotas |
| Foreign rename of a dir or its ancestor leaves a stale quota chain forever; quota limit bypass | RUN on main 1dd8856 | `pkg/meta/rnd_chain_test.go` (rnd), `TestRedisDirQuotaRefresh` fails on main |
| Foreign hardlink is accounted correctly (parents read inside the txn) | RUN | `TestRndForeignHardlink` |
| Cross-quota rename moves usage from non-strict dir stats; another client's unflushed stats are missed | RUN | first run of `TestRndForeignRenameDir` (/a=1 /b=1 instead of 0/2) |
| Upstream race: `refresh` reads `m.sid` before `NewSession` sets it | RUN (`-race`, heartbeat 100 ms) | `base.go:805/824/976` |
| Scoped refresh sees quotas created, changed and deleted by raw HSET/HDEL within one refresh | RUN | `TestRedisDirQuotaExternalWriter` |

Background quota cost, one client, heartbeat 1 s, 10 active quotas (RUN, `rnd/results/qbench`):

| Q | Valkey CPU ms/s v1.4.2 | main | scoped refresh | startup v1.4.2 / main / scoped | bytes/s to client v1.4.2 / main / scoped |
|---|---|---|---|---|---|
| 100 | 33 | 30 | 31 | 9.1 / 1.5 / 0.8 ms | 37 / 40 / 35 KB |
| 1 000 | 58 | 32 | 31 | 79 / 2.6 / 1.5 ms | 70 / 97 / 35 KB |
| 10 000 | 235 | 33 | 31 | 789 / 14 / 1.5 ms | 315 / 701 / 35 KB |
| 100 000 | 441 | 58 | 31 | 7724 / 154 / 1.4 ms | 616 / 5607 / 36 KB |

~31 ms/s and ~35 KB/s are the foreground creates of the benchmark itself. Production heartbeat is 12 s: background numbers scale by 1/12 per client and by the number of clients (INFERENCE).

## Metadata capsules (PoC, Redis, offline eviction)

| Item | Label | Value |
|---|---|---|
| Evict → hydrate restores every Valkey value byte for byte; `juicefs dump --subdir` identical | RUN | `TestRedisCapsuleEvictHydrate`, `rnd/results/capsule` |
| 90 units × 10 000 files evicted in 13.8 s; `used_memory` −198 MB of ~203 MB expected | RUN | |
| Capsule size | RUN | 1.6 MB per 10 000 files (~160 B/file, uncompressed) |
| Evict / hydrate time per 10 000-file unit | RUN | ~120 ms / ~87 ms |
| Hydrate peak memory | RUN | equal to the unit's own hot size, no extra burst |
| Residual per cold unit in Valkey | RUN | ~1.7 KB, 2 keys (descriptor + root records that stay hot) |
| Cold namespace 0.9 M → 9.9 M files (×11), hot 100 000 files fixed | RUN | `used_memory` 29.6 → 30.6 MB (~1 KB per cold unit), keys 202 → 204 thousand; hot stat p50 95 µs, create p50 ~590 µs unchanged; capsules 1.6 GB (`rnd/results/growth`) |
| 200 000-file unit | RUN | evict 2.5 s, hydrate 2.1 s, peak = hot size (47 MB), capsule 32 MB |
| Transparent hydration on lookup / readdir / inode access; summary of a cold root without hydration; rmdir of a cold root → ENOTEMPTY | RUN | `TestRedisCapsuleTransparent` |
| GC sees capsule slices; without object storage GC fails closed (EIO) | RUN | same test |
| Crash before commit, during deletion, during hydration, stale hydrator after takeover | RUN | `TestRedisCapsuleCrashRecovery` |
| Running old clients exit at next `reloadFormat` when MetaVersion > their MaxVersion; MinClientVersion is not re-checked there | SOURCE | `base.go` reloadFormat |

## Kvrocks 2.17.0 (control)

| Gate | Label | Result |
|---|---|---|
| Default `redis-databases 0`: SELECT is a no-op, DB1 writes visible in DB0 | RUN | confirmed |
| `CLIENT TRACKING` | RUN | unsupported |
| `DBSIZE` reports 0 while keys exist (async scan) | RUN | confirmed; LoadMeta empty-target guard unreliable |
| #3555 partial write of a failed command inside EXEC | RUN | reproduced on 2.17.0 with `write_batch_max_bytes 180` |
| Full JuiceFS `testMeta` on Kvrocks (`redis-databases 16`, `write_options.sync yes`) | RUN | pass, 33 s |
| Fixed hot probe while namespace grows 1 → 4 M files, block cache 512 MB, `sync yes` | RUN | RSS 193 / 447 / 766 / 794 MB (plateau at cache size + ~250 MB), disk 0.8 / 1.9 / 2.8 / 3.7 GB, stat p50 154–175 µs (Valkey 95), create p50 506–612 µs (Valkey ~550), bulk create 3.4× slower than Valkey (`rnd/results/kvrocks`) |

## Review of the capsule PoC (independent adversarial pass)

Fixed in `fbe4590`, each with a test that fails on `6391999`:

- GC: capsule slices are scanned before hot keys and the scan repeats if `capsuleVer` changed; a hydration during `ScanSlices` no longer hides restored slices.
- Offline fence: an `capsuleEvicting` marker makes `NewSession` refuse while an eviction runs; the commit txn WATCHes `allSessions` and re-checks that only the evicting session is alive.
- Session-less tools and read-only mounts load the cold set lazily; they get EIO on cold contents instead of an empty directory, and `quota check/set` can no longer rewrite usage or create entries inside a cold root.
- `juicefs metadata prefetch` resolves paths with hydrating lookups; `Resolve(force)` hydrates too.
- A partial drop is resumable (`evict` again finishes it) and `cleanupLeakedInodes/Chunks` skip inodes inside capsule runs.
- MetaVersion is bumped at the cold commit only; an aborted eviction deletes its uploaded object.
- Restore batches are bounded by values, not records; per-root hydration locks; ACL get/set hydrate.
- Gateway and the Java SDK get the capsule storage.

Still open (PoC limits): read-only mounts are not registered as sessions and cannot be detected by the offline fence; walking commands on `/` (compact, tree summary with depth, user/group usage scans) hydrate every unit they touch; periodic metadata backup is refused while units are cold; MetaVersion is never lowered back; a lost ACK on the final hydrate txn can leak the capsule object; chunk keys at or beyond file length are not captured.

## Online eviction and end-to-end

| Item | Label | Value |
|---|---|---|
| Online eviction with a second client creating files and listing the tree | RUN | ~920 files created during eviction, none lost, listings never incomplete (`TestRedisCapsuleOnline`, 3 runs + `-race`) |
| Busy tree | RUN | eviction returns EAGAIN and the tree stays hot when it is used before cleanup |
| Two FUSE mounts alive, tree idle | RUN | keys 466 → 24 in 3.7 s; reads through the other mount (kernel-cached inodes) load it back; sha256 of 200 files, inode numbers, modes, links, sizes identical |
| Real `gc --delete` with objects older than 1 h while the tree is cold | RUN | 202 valid, 0 leaked, nothing deleted, contents intact |
| Upstream main binary on the volume | RUN | `mount` and `gc` refuse: incompatible metadata version 2 |
| Image on `juicedata/mount:ce-v1.4.2` with the integration binary (static, musl) | RUN | mount in container, online evict, transparent load, contents identical |
| `quota set` without `--strict` on a directory being written records usage from unflushed dir stats | RUN on official v1.4.2 and on the fork | 286 / 261 instead of 301 inodes; `quota check --strict` reports inconsistent. Upstream behavior |

## PostgreSQL 16 (control, PG-only)

| Item | Label | Value |
|---|---|---|
| Namespace 1 → 4 M files (100 users × 100 dirs × 100 files per round, per-user dir quota) | RUN | DB 599 / 1188 / 1769 / 2337 MiB (`pg_size_pretty` uses binary units; ≈ 613 B/file on disk), PSS of all postgres processes 0.48 / 0.58 / 0.58 / 0.59 GB (plateau at shared_buffers) (`rnd/results/pg`) |
| Bulk create, 32 threads | RUN | ≈ 2 600 files/s (383–436 s per million) |
| Hot probe at 4 M files (path walk by Lookup, 4 round trips) | RUN | stat p50 598–633 µs, p99 2.0–2.3 ms; create p50 1.08 ms, p99 2.4–2.6 ms |
| Not comparable | NOTE | Valkey/Kvrocks probes used `Resolve` (one Lua call); `Resolve` is ENOTSUP on SQL, so the PG probe walks with `Lookup` |

## Automatic eviction (Track A)

| Item | Label | Value |
|---|---|---|
| Access recording: one HSET+HINCRBY per unit per client per minute; units resolved through cached parents without hooks | RUN | `TestRedisCapsuleAutoEvict` |
| Scan guard: > 100 units in one minute are not recorded | RUN | mutation removing the guard fails the test |
| Idle eviction, `--max-units` limit, min residency after load, pressure order by decayed hits | RUN | each mutation fails the test |
| Two FUSE mounts, 10 users, 3 used, `auto-evict --idle 100s --once` | RUN | 7 idle units evicted, used ones kept, keys 459 → 169, all 200 sha256 equal after transparent load through the other mount |
| Eviction rate | RUN | 26.8 s for 7 units (≈ 3.8 s/unit, dominated by two ACK rounds and the drop grace); 10 k idle units/day needs batched eviction |

## Literature (bounded metadata caches)

- Every surveyed system bounded by memory caches inode/dentry/dirfrag granularity (CephFS dirfrag ≤ 10 k entries, InfiniFS, IndexFS, λFS, Alluxio 2); none loads whole subtrees. Whole-unit capsules fit only when the unit is the access unit (an account).
- Scan resistance: ARC/2Q-style probation; Ceph throttles cap acquisition on readdir and recalls caps from idle sessions.
- Directory rename/setattr is ≈ 0.008 % of operations in Pangu traces (InfiniFS), which makes lazily validated caches viable.
- Alluxio 2.0 (heap cache over RocksDB): ≈ 1 KB/inode on heap vs ≈ 500 B on disk, ~50 % CPU in (de)serialization when missing.
- No source publishes cold-miss latency; measure it ourselves.

## User/group quota refresh (C × Q)

| Item | Label | Value |
|---|---|---|
| Before: each client on every refresh HSCANs all user and group quotas under `quotasFlushLock` | SOURCE | `v1.4.2`, `upstream/main` `loadQuotas` |
| Q = 100 000 uid quotas, one client, 10 uids in use (`zz_ugbench_test.go`, base vs change, same Redis 7.0.15) | RUN | refresh 316 ms → 0.16 ms, 7.3 MB → 562 B per refresh, a flush during refresh waited 305 ms → 0.18 ms |
| C = 1 000 clients, 12 s heartbeat, extrapolated | CALC | ≈ 610 MB/s → ≈ 47 KB/s out of the engine |
| Miss on first write of a uid/gid: one HMGET for uid and gid together, fail-closed EIO after 3 failed loads | RUN | `testUserGroupQuotaRefresh` on Redis, SQLite, MemKV |
| Refresh outside the flush lock; a flush during the read keeps local usage and the stale ids are reread | RUN | mutations removing the generation check or the reread fail the test |
| Adversarial review | RUN | found an SQLite deadlock (fetch under the flush lock inside a txn) in an intermediate version; fixed, regression test hangs on the old code |

Corrections (2026-10-30): the PostgreSQL control ran on PostgreSQL 16, not 17; unit tests, the auto-evict FUSE run and the user quota bench ran on Redis 7.0.15 at port 6379, not Valkey.

## Metadata on SSD with a bounded RAM cap (scale model of 39 M files / 2 GiB)

WITHDRAWN (2026-10-30): the data size during this run was not measured. The 3.7 GB figure came from the earlier load and included WAL and uncompressed L0 files; after the database was reopened and compacted the data directory held 351 MB for 4 M synthetic files (≈ 88 B/file, highly compressible sequential names). The cap-to-data ratio during the run is unknown (roughly 25–70 %, not 5.5 %), so the conclusion "latency does not depend on the working set at ≈ 5 % of data" is not supported. The table is kept as a raw record; the replacement is the `kvrocks-growth` run below (realistic names and sizes from `rnd/corpus`, SST bytes measured after compactions settle, data generated under the cap).

Their case: 39 M files ≈ 8.6 GiB in Valkey (≈ 237 B/file) against a 2 GiB ceiling. Model as run: Kvrocks 2.17.0 with 4 M synthetic files, container memory capped by cgroup including the page cache, host caches dropped before each run, `sync yes`, probe of 5 000 random stats and creates per working set (`rnd/results/kvrocks-cap`).

| Cap (block cache) | Working set | stat p50 / p99 | create p50 / p99 | cgroup memory | OOM |
|---|---|---|---|---|---|
| 256 MB (96 MB) | 10 k files | 258 µs / 33.9 ms (warm-up) | 722 µs / 2.7 ms | 255 MB | 0 |
| 256 MB (96 MB) | 100 k | 226 µs / 0.87 ms | 669 µs / 1.9 ms | 244 MB | 0 |
| 256 MB (96 MB) | 1 M | 234 µs / 0.87 ms | 805 µs / 1.9 ms | 255 MB | 0 |
| 256 MB (96 MB) | 4 M (all) | 291 µs / 0.99 ms | 761 µs / 1.7 ms | 255 MB | 0 |
| 4 GB (512 MB) | 10 k – 4 M | 181–310 µs / 0.6–1.2 ms | 692–800 µs | up to 1.2 GB | 0 |

Valkey on the same probe: stat p50 95 µs, create ≈ 550 µs. Memory stays at the cap and latency barely depends on the working set; the cost is ≈ 2.5× on stat and ≈ 1.3× on create against RAM-only Valkey.

## JuiceFS Enterprise numbers (vendor blog, juicefs.com/en/blog/engineering/reduce-metadata-memory-usage)

All-in-memory engine with Raft and subtree partitions: ≈ 600 → 300 → 150 → 50 B/file via arenas, serialized idle directories and in-memory compression; planned 100 B/file, production 30 GiB for 300 M files. Comparison in the post: CE Redis 430 B/file, HDFS 370, CephFS 2 700, Alluxio heap 2 100. At 100 B/file 39 M files need ≈ 3.9 GB, still above a 2 GiB ceiling [VENDOR CLAIM].

## Big-O audit of the S3 capsule track (C = 1 000, U = 1e5, R = 50, heartbeat 12 s)

| Cost | Value | Source |
|---|---|---|
| Capsule poll HKEYS every second on every mount, paid while idle | ≈ 1.1e8 elements/s, ≈ 4 Redis cores, ≈ 1.5 GB/s | `capsule.go` watchCapsules, `redis_capsule.go` doLoadCapsuleSet |
| Full reload of all descriptors on every version bump | 1e8 HGETALL and ≈ 120 GB egress per bump fleet-wide | same |
| ACK barrier per eviction | ≈ 250 s at U = 1e5 against a 30 s timeout | `redis_capsule.go` waitAcks |
| Client RAM for routing runs | ≈ 250 MB per mount | `capsule.go` newCapsuleSet |
| Automatic metadata backup above 1 M inodes with interval ≤ 1 h | skipped by upstream; with any evicted unit it fails after a full SCAN | `pkg/vfs/backup.go`, `redis_capsule.go` doListCapsules |
| Directory parent verification with quotas | up to 1e4 GET per heartbeat per writing mount; now every ten heartbeats, batch 1 000 | `quota.go` refreshDirParents, `50336ef` |

Decision (owner, 2026-10-30): no on-demand metadata reads from S3 in the main design; metadata lives on SSD with a bounded RAM cache, S3 keeps file contents and rare batch backups.

## Kvrocks qualification on the fork

| Item | Label | Value |
|---|---|---|
| Full `testMeta` on Kvrocks 2.17.0 (`redis-databases 16`, `sync yes`) with `fix/kvrocks-guards` | RUN | pass, 33.4 s (`TestKvrocksClient`, gated by `KVROCKS_ISOLATED_DB`) |
| `redis-databases 0` (Kvrocks default): `SELECT 5` returns OK, a key set in DB 5 is visible in DB 0, `CLIENT INFO` has `namespace=` instead of `db=` | RUN | the fork now refuses database N ≠ 0 there (`50bb930`) |
| `DBSIZE` on a 4 M-file database | RUN | 0 until a DBSIZE scan runs; load/restore emptiness now checked with SCAN (`50bb930`) |
| Hash field read in Kvrocks = metadata CF + subkey CF point reads | SOURCE | kvrocks.apache.org/community/data-structure-on-rocksdb; explains ≈ 2.5× stat vs Valkey |

## Community record (juicedata/juicefs issues, PRs, discussions; anonymous reads)

- Declined as engines by maintainers: Pika/Codis (no transactions, #4355), RocksDB (use Badger, #1518), DragonflyDB (#3363), Tendis (#6268), HBase and Aerospike; SlateDB engine PR #7174 kept pending as "not very stable". Kvrocks is not officially supported; users asked to skip the `noeviction` check for disk engines (#3195, #3888).
- Redis Cluster pins a volume to one hash slot, so it does not shard memory (docs `redis_best_practices.md`). For per-tenant isolation the community answer is one volume per tenant (DISC #5529).
- Maintainers recommend TiKV above ≈ 100 M files; the largest reported deployments exceed 20 B files (DISC #3200).
- Automatic backup is skipped above 1 M inodes at `--backup-meta` ≤ 1 h; there is no incremental backup; binary dump ≈ 1/3 of JSON (docs `metadata_dump_load.md`, #4974, #7290, #5276).
- Quota loading: upstream replaced 2 HGET per quota with 3 HSCAN (#7405) and batched TiKV gets (#7171); nobody proposed scoped refresh or loading outside the flush lock.
- Memory blow-ups in the field come from fragmentation, not only file count: 30 M files with 400 GB in Redis from huge chunk lists after random writes (DISC #6338).
- No proposal to tier, evict or lazy-load metadata out of Redis exists (searched EN and ZH).

## Literature numbers used in the SSD design

- Workload mix (Spotify HDFS trace via HopsFS, FAST'17): read 68.7 %, stat 17 %, list 9 %, create 1.2 %, rename 1.3 % (0.03 % of renames are directories). Alibaba Pangu (InfiniFS, FAST'22): directory rename + set_permission ≈ 0.0083 % of operations. Partitioning by tenant keeps the hot path single-partition.
- Compression of JuiceFS-like records (local run, zstd-3 on 4–64 KiB blocks): 3.5–4× for FUSE-created metadata, 6–7× for bulk-imported, 2.2× worst case; lz4 2.5–4×. RocksDB in Kvrocks defaults to snappy from level 2 only.
- Valkey bytes per small key: 7.2 102.5 B, 9.1 64 B (Percona). Valkey listpack buckets would need `hash-max-listpack-value` ≥ 128 for JuiceFS attributes.
- S3 Standard small GET: p50 ≈ 30–40 ms, p99 ≈ 170 ms; ≈ 30 ms + 20 ms/MiB (VLDB'23). A cold stat served from S3 would cost one paid GET and tens of milliseconds, which is why on-demand S3 metadata is out of the main design.

Evidence fixes (2026-10-30, after external review of `cd780bb`): raw results of the growth, Kvrocks, big-unit and capsule runs are now committed under `rnd/results`; `capsule-exp.sh` and `auto-e2e.sh` exit non-zero on a failed prefetch, dump or manifest comparison; the generator used for the PostgreSQL run is `rnd/nsgen` (the `nsgen2` binary was an uncommitted build of it); `rnd/corpus` gives realistic, collision-free names and sizes shared by `nsgen -random` and `probe -random`.
