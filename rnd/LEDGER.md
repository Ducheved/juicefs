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

## Redis/Lua dependency gate (before any backend choice)

Script: `rnd/kvrocks-gate.sh`; raw output in `rnd/results/gate`. Kvrocks 2.17.0 against Redis 7.0.15.

| Item | Label | Value |
|---|---|---|
| Commands JuiceFS sends (MONITOR during `TestRedisClient` and the Redis lock/clone tests) | RUN | 49 distinct, `commands.txt`; plus CONFIG, DBSIZE, FLUSHALL, CLIENT, MSET, ZRANGE from source |
| Of those, missing on Kvrocks | RUN | 0 (`coverage.txt`) |
| JuiceFS Lua: `scriptLookup`, `scriptResolve` (read-only; `struct.unpack`, `string.format`, `error()`) | RUN | identical replies on Kvrocks; error texts differ (`ERR running script … ENOENT`) but contain the substrings `handleLuaResult` matches (`probe.diff`) |
| Other error texts | RUN | Kvrocks prefixes `ERR Invalid argument:` to WRONGTYPE and overflow; JuiceFS does not match these strings (SOURCE `shouldRetry`) |
| Unexpected Lua error in JuiceFS | SOURCE | disables the script and falls back to HGET+GET with a warning; correctness kept, one extra round trip |
| EVAL/EVALSHA on Kvrocks with `lua-strict-key-accessing no` (default) | SOURCE | flagged `exclusive`: a global lock over all workers (`cmd_script.cc` GenerateEvalFlags, `kvrocks.conf`); with `yes` scripts run concurrently and any script that touches a key outside KEYS is refused |
| EVAL/EVALSHA marks its KEYS as modified for WATCH on Kvrocks, even read-only | RUN | `WATCH d1`; another client `EVALSHA lookup d1 …`; `EXEC` aborted on Kvrocks, committed on Redis (`probe.*`) |
| Effect on JuiceFS: 32 lookup + 8 create/unlink goroutines, 2×4 dirs × 500 files, 20 s | RUN | before: 3 of 3 runs end in EIO after 50 WATCH retries, transactions retried up to 39 times over 11 s; Redis 0 failures (`mixload-kvrocks-before.jsonl`) |
| Fix `fix/kvrocks-guards` 9fdbc1e: EVALSHA_RO when `COMMAND INFO evalsha_ro` knows it | RUN | Kvrocks: 0 transactions with ≥ 10 retries, lookup 35 k/s, create 288/s; Redis 7: 71 k/s and 965/s before and after (`mixload-ab.jsonl`); regression test red before, green after |
| SET throughput while 32 clients run each script form (Kvrocks, sync no) | RUN | EVALSHA 21 k/s, EVALSHA_RO 33 k/s, HGET 37 k/s (`interference-nosync.txt`); with sync yes fsync dominates (~3.4 k/s for all) |
| EXEC when RocksDB cannot append WAL (tmpfs full) | RUN | every queued command returns the IO error; nothing is applied (`enospc.txt`) |
| After the disk gets space again | RUN | writes keep failing for ≥ 215 s (`Failed to resume DB after retryable IO error: Recovery in progress`) until the process restarts; data before the error intact |
| JuiceFS on a full Kvrocks disk (`nsgen`, 32 threads, dir quotas) | RUN | EIO to the caller; after restart `usedInodes` and `usedSpace` equal the dumped tree exactly (11 empty files from creates whose write failed) (`enospc-juicefs.txt`) |
| kill -9 of Kvrocks (`sync yes`) during 16 lookup + 16 create/unlink goroutines, 3 times | RUN | clients get EIO; after restart `usedInodes` and `usedSpace` equal tree + trash exactly each time (`rnd/kvrocks-kill.sh`, `results/kill/summary.txt`). Lost ACK (committed, client saw EIO) not measured |
| Dir quota usage after `quota set` on a dir a running client writes into | RUN | not counted until that client's next heartbeat reload (`loadQuotas`, every `Heartbeat`): on Redis and Kvrocks alike, 500 files written right after `quota set` leave usage 0. Not an engine issue; irrelevant when the quota is set on a new empty account dir |
| #3555 (partial command effects inside EXEC on a runtime error) | REPORTED + SOURCE | open, PR #3556 open; triggers are write-batch memory limit (default 0, off), RocksDB read or decode errors. A full disk fails at commit and is atomic (RUN above). Redis itself does not roll back other commands in EXEC either |
| Kvrocks archived WAL | SOURCE + RUN | kept 3 h for replication (`rocksdb.wal_ttl_seconds 10800`); in the growth run WAL = 2.7 GB vs SST 1.0 GB at 4 M files: disk = SST + 3 h of writes |

## Kvrocks growth under a 192 MiB cgroup (page cache included)

Script: `rnd/kvrocks-growth.sh`; block cache 64 MB, write buffer 8 MB × 2, `sync yes`, 4 workers. Shared 4 vCPU stand, other experiments ran in parallel.

| Files | SST B/file | WAL GB | cgroup max MB | OOM | hot 10 k stat p50/p99 µs | all stat p50/p99 µs | create p50/p99 µs | server CPU µs/op |
|---|---|---|---|---|---|---|---|---|
| 1 M | 248 | 0.66 | 192 | 0 | 334 / 824 | 375 / 1123 | 620 / 3060 | 571 |
| 2 M | 252 | 1.33 | 192 | 0 | 356 / 857 | 504 / 1326 | 602 / 1307 | 615 |
| 3 M | 252 | 2.01 | 192 | 0 | 344 / 809 | 599 / 69144 | 592 / 5844 | 693 |
| 4 M | 251 | 2.68 | 192 | 0 | 279 / 720 | 533 / 1714 | 571 / 1816 | 653 |
| 5 M | 252 | 3.36 | 192 | 0 | 354 / 785 | 702 / 1866 | 658 / 1539 | 705 |
| 6 M | 225 | 4.04 | 192 | 0 | 370 / 916 | 572 / 1386 | 660 / 1649 | 685 |
| 7 M | 186 | 4.71 | 192 | 0 | 370 / 1023 | 583 / 1395 | 606 / 1110 | 688 |
| 8 M | 173 | 5.39 | 192 | 0 | 387 / 944 | 597 / 1126 | 622 / 1378 | 656 |

Raw: `results/kvrocks-growth.txt`, `results/kvrocks-growth/`. RAM stays at the cap while files grow 8×; the hot set stays flat; uniform reads over all files pay disk reads (one p99 spike to 69 ms at 3 M). SST shrinks to 173 B/file as compaction reaches lower levels; archived WAL grows linearly with writes (5.4 GB at 8 M).

## PostgreSQL growth under a 192 MiB cgroup (page cache included)

Script: `rnd/pg-growth.sh`; PostgreSQL 16.15, `shared_buffers 64MB`, `fsync on`, `synchronous_commit on`; all postgres processes in one memory + cpuacct cgroup; same generator (`nsgen -random`, 100 users × 100 dirs × 100 files per round, 32 threads) and probe as the Kvrocks growth run. Raw: `results/pg-growth.txt`, `results/pg-growth/`.

| Files | DB B/file | pg_wal GB | cgroup max MB | OOM kills | gen s per 1 M | hot 10 k stat p50/p99 µs | all stat p50/p99 µs | create p50/p99 µs | server CPU µs/op |
|---|---|---|---|---|---|---|---|---|---|
| 1 M | 656 | 1.07 | 192 | 0 | 373 | 358 / 1216 | 691 / 2693 | 1086 / 2790 | 814 |
| 2 M | 644 | 1.07 | 192 | 0 | 367 | 371 / 1094 | 770 / 2906 | 1155 / 3111 | 868 |
| 3 M | 646 | 1.07 | 192 | 0 | 370 | 363 / 1224 | 751 / 2754 | 1132 / 2498 | 866 |
| 4 M | 642 | 1.07 | 192 | 0 | 375 | 343 / 1277 | 814 / 2883 | 1068 / 2780 | 889 |

Against Kvrocks at 4 M under the same cap (RUN, both tables): bulk generation 375 s vs ≈ 800–930 s per million; hot stat p50 equal (≈ 350 µs); uniform stat p50 814 vs 533 µs; create p50 ≈ 1.0–1.1 ms vs ≈ 0.6 ms; disk 642 vs 251 B/file; WAL bounded by `max_wal_size` (1 GB) vs Kvrocks' 3 h archive. The probe walks paths with `Lookup` on SQL (Resolve is ENOTSUP) and with one Lua `Resolve` on Kvrocks, so stat includes more round trips on PostgreSQL.

## Badger growth (in-process engine)

Script: `rnd/badger-growth.sh`; `badger://…?sync=true`; JuiceFS sets only `SyncWrites`, the rest are Badger v4 defaults (block cache 256 MB, memtable 64 MB × 5, `IndexCacheSize 0` = all table indexes in RAM) (SOURCE `pkg/meta/tkv_badger.go`). The engine lives inside the client, so the cgroup holds `nsgen`/`probe` itself: engine + JuiceFS client + 32 generator threads, unlike the server-only caps above.

| Item | Label | Value |
|---|---|---|
| 10 000 files, 192 MiB cap | RUN | peak 98 MB, no OOM (smoke) |
| 1 M-file round, 192 MiB cap | RUN | OOM-killed during the first round (`results/badger-growth-192m`) |
| 1 M-file round, 2 GiB cap | RUN, partial | peak 649 MB after ~4 min of the first round; run stopped before the round ended (moved to a larger machine) |

## M2 on the bench VM (4 vCPU EPYC 9754, 15 GiB, one 128 GB disk; engine on cores 0-1, loader on 2-3; `results/vm`)

Corpus `nsgen3 -random`, 1 M files per round, 4 rounds. Probe: 3 000 path walks by `Lookup` + `GetAttr` and 3 000 create/close/unlink cycles, one client; "server CPU per op" is engine cgroup CPU / 6 000 probe operations, not per command. Builds from `rnd` use plain `EVALSHA` (`fix/kvrocks-guards` not merged).

| Engine, cap | Memory at 4 M | Disk per file | stat p50 hot / all | create p50 | Server CPU per op | Gen per 1 M | OOM |
|---|---|---|---|---|---|---|---|
| Kvrocks 2.17.0, 192 MiB, cache 64 MB, sync yes | RSS 128–139 MB flat in N | SST 252 B + WAL 2.7 GB (3 h TTL) | 711 / 1 416 µs | 939 / 978 µs | 713–717 µs | 1 313–1 339 s | 0 |
| PostgreSQL 18.4 (CNPG image), 192 MiB, shared_buffers 64 MB | at cap (page cache included) | 637 B + WAL 1 GB | 410 / 1 908 µs | 862 / 1 206 µs | 645–908 µs | 450–456 s (engine CPU-bound, 1.87 cores) | 0 |
| Valkey 9.1.1, maxmemory 2 GB, AOF always | used_memory 947 MB = 236 B/file | AOF 189 B | 208 / 221 µs | 815 / 823 µs | 347–357 µs | 476–480 s | 0 |
| Badger v4 in-process, 2 GiB | peak 826 → 1 472 → 1 576 → 1 963 MB during generation | 687 B (vlog preallocated 2 GB), SST 116 B | 48 / 73 µs (no network) | 493 / 536 µs | n/a | 424–430 s | 0 |

Kvrocks memory is flat in file count; its all-files stat grows with SST size over a 64 MB cache (717 → 1 416 µs from 1 M to 4 M). Valkey reaches 2 GiB at ≈ 9 M files (CALC from 236 B/file).

## Write ceiling (`rnd/write-ceiling.sh`, `results/vm/write-ceiling-*`)

Cycle = Create + Close + Unlink (two write transactions). ≈ 10 k files in 512 dirs, 4 loader processes, writers split evenly, 30 s per step. Engine CPU from its cgroup.

| Writers | Valkey cycles/s (p99) | Kvrocks cycles/s (p99) | PostgreSQL cycles/s (p99) |
|---|---|---|---|
| 4 | 660 (4.7 ms) | 549 (7.4 ms) | 1 126 (3.9 ms) |
| 16 | 1 257 (12.8 ms) | 550 (47 ms) | 1 238 (17 ms) |
| 64 | 2 833 (32 ms) | 539 (266 ms) | 936 (431 ms) |
| 256 | 3 723 (169 ms) | 444 (1.8 s) | OOM-killed at 512 MiB (one backend per connection) |
| Engine cores at the top | 0.84 | 0.44–0.48 | 2.0 (saturated; loader also ≈ 1.7 of 2) |
| Engine CPU per cycle | 215–405 µs | 838–998 µs | 1 070–2 200 µs |
| Retried transactions at 256 / 64 | 16 846 / 3 739 | 2 031 / 431 | 2 / 0 |

Kvrocks is flat in writers at about half its CPU: `exec` is `exclusive` (`src/commands/cmd_txn.cc:133`), the transaction is one global `txn_write_batch_` on `Storage` committed with `sync` inside the exclusive section (`src/storage/storage.cc` BeginTxn/CommitTxn), so every JuiceFS write transaction holds the whole server through its fsync. Not configurable. Lua scripts are not wrapped in that batch: each writing `redis.call` commits separately (`src/storage/scripting.cc`), so moving writes into Lua would lose crash atomicity. One process writing into 8 directories (`results/vm/write-ceiling-hotdir*`) caps both Valkey (876) and Kvrocks (560) because the client serialises transactions per inode (`txLock`, 1 024 stripes).

## Pebble with the JuiceFS TKV layout, 39 M files, 2 GiB cgroup (`rnd/pebblebench`, `results/vm/pebble-bench*`)

Pebble v2.1.7 in one process, no network, no replication, no conflict checks. Cgroup `memory.max` 2 GiB including page cache, cores 0-1, `GOMAXPROCS=2`, `GOMEMLIMIT=1GiB`, block cache 768 MB, memtable 64 MB, bloom 10 bits on all levels, default compression. Keys exactly as `tkv.go`: per file `A<ino>I` (72 B attr), `A<parent>D<name>`, `A<ino>C<0>` (one 24 B slice); per dir attr and `U<ino>` stats; names and sizes from `rnd/corpus`. Lookup = dentry get + inode get. Create = parent attr get + dentry get + three sets, committed with `Sync`.

| Item | Label | Value |
|---|---|---|
| Load 39 M files, 2 threads, unsynced batches | RUN | 278 s (140 k files/s), 395 s CPU |
| Size after compactions stop | RUN | SST 3.0 GB in 362 tables (L6 2.5 GB) ≈ 77 B/file; disk incl. WAL 3.21 GB ≈ 82 B/file |
| Memory | RUN | cgroup at the 2 GiB cap (page cache included), 0 OOM kills in load and in reads/writes; anonymous vs page cache split during reads not recorded (during load: anon 76 MB, file 1.95 GB) |
| Lookup, 10 k hot files, 8 readers | RUN | 148.9 k/s, p50 12 µs, p99 31 µs, 13 µs CPU per lookup |
| Lookup, uniform over all 39 M, 8 readers | RUN | 27.5 k/s, p50 83 µs, p99 2.0 ms, 49 µs CPU per lookup, block cache hit 97 % |
| Synced create, writers 1 / 4 / 16 / 64 / 256 | RUN | 1 802 / 4 231 / 13 597 / 22 041 / 20 996 txn/s; p99 0.86 / 1.2 / 2.0 / 8.0 / 46 ms; 129 / 102 / 67 / 64 / 70 µs CPU per txn |
| Group commit | SOURCE | `record/log_writer.go` v2.1.7: `flushLoop` takes `snapshotForPop` of all waiting syncs, one `syncWithLatency`, then `pop(snap)` releases them |
| Not comparable to the engine tables above | NOTE | those count JuiceFS create+unlink cycles through the client and network; this is the storage layer alone |
| First attempt | NOTE | settle loop waited for `EstimatedDebt == 0`, which Pebble never reached with compactions idle; the process sat idle ~35 min and was stopped; settle now waits for three idle 5 s checks with no new compactions |

## External facts used in `rfc/metad.md` (REPORTED, collected 2026-10-30, not re-run here)

| Fact | Source |
|---|---|
| etcd keeps every key in an in-memory B-tree, ≈ 120 B + key per key; default quota 2 GiB, 8 GiB suggested maximum | etcd.io/docs/v3.6/benchmarks/etcd-storage-memory-benchmark, etcd.io/docs/v3.6/dev-guide/limit |
| dqlite keeps the database in an in-memory VFS; snapshots copy it | github.com/canonical/dqlite issues 368, discussion 602 |
| rqlite: explicit BEGIN/COMMIT undefined, only one-shot batch transactions | rqlite.io/docs/api/api (v10) |
| FoundationDB: 4 GiB per server process required; `memory` default 8 GiB; operator default topology ≈ 12 processes | apple.github.io/foundationdb administration.html, configuration.html (8.0), FoundationDB/fdb-kubernetes-operator |
| TiKV dev/test 8 cores / 32 GB, PD 4 cores / 8 GB | docs.pingcap.com/tidb/stable/hardware-and-software-requirements (v8.5) |
| CockroachDB ≥ 4 vCPU and 4 GiB per vCPU; YugabyteDB on Kubernetes ≥ 15 GB per node | docs.cockroachlabs.com recommended-production-settings, docs.yugabyte.com/stable/deploy/checklist |
| dragonboat last release v3.3.8 (2023-09-25); etcd-io/raft v3.7.0 (2026-06-23); hashicorp/raft v1.8.0 (2026-09-16) | GitHub releases of each project |
| JuiceFS EE: all metadata in memory on 3 Raft nodes; ≈ 600 → 50 B/file with compression, 100 B/file planned, 300 M files in 30 GiB | juicefs.com/en/blog/engineering/reduce-metadata-memory-usage (2024-02-22) |
| CephFS embeds single-link inodes in directory fragments; backtraces replace the anchor table; async create/unlink with delegated inode numbers since kernel 5.7 | Weil OSDI'06; docs.ceph.com Firefly release notes; lore.kernel.org 20200212172729 |
| Lustre metadata write-back cache: 10–20× on createmany in VMs, a proposal (LU-10983), not shipped | wiki.lustre.org LUG2023-Metadata_WBC-Drokin.pdf |

## metad P1: single node, JuiceFS TKV over the network (`feat/metad` 15823bd5, `results/vm/metad-*`, `results/vm/write-ceiling-spread*metad*`)

Server: `juicefs metad`, Pebble v2.1.7, snapshot reads per transaction, optimistic validation against an in-memory recent-writes index (point keys and scanned ranges), one committer batching transactions into one synced Pebble batch. Client: `metad://` `tkvClient`, 64 connections per process. No replication yet. Same VM, cores and loaders as the engine tables above; server in a 2 GiB cgroup on cores 0-1, `GOMAXPROCS=2`, `GOMEMLIMIT=1GiB`, block cache 768 MB.

| Item | Label | Value |
|---|---|---|
| JuiceFS shared meta test suite (`testMeta`) on `metad://` | RUN | pass, 33–38 s; `testTKV` pass; conflict, phantom, snapshot and expired-snapshot cases pass (`TestMetadTransactions`) |
| Growth to 4 M files, two runs | RUN | gen 103–126 s per 1 M (Valkey 476–480, PostgreSQL 450–456, Kvrocks 1 313–1 339); stat p50 hot / all 371–377 / 383–404 µs, p99 ≤ 502 µs; create p50 367–380 µs, p99 ≤ 498 µs; server CPU per probe op 578–634 µs; 0 OOM |
| Memory at 4 M (second run) | RUN | cgroup 1 334 MB (peak 1 355), anon 728 MB, file 586 MB; anon grows with the block cache filling (447 → 728 MB from 1 M to 4 M, data 357 → 615 MB) and is bounded by the 768 MB cache plus memtables and Go heap |
| Disk at 4 M | RUN | 615 MB incl. WAL ≈ 153 B/file (falls as fixed WAL and memtable overhead amortises; Pebble alone at 39 M: 77 B/file SST) |
| Write ceiling, writers 4 / 16 / 64 / 256 | RUN | 1 726 / 3 963 / 6 133 / 5 539 cycles/s; p99 1.9 / 5.5 / 19 / 106 ms; engine 0.77 / 1.16 / 1.50 / 1.41 cores; 245–444 µs engine CPU per cycle; logged retries 150 / 1 305 / 7 109 / 17 134 (doMknod on the parent inode) |
| Against Valkey on the same test | RUN | write ceiling 1.6–3.2× Valkey at every writer count; create p50 2.2× faster (370 vs 815 µs); stat p50 1.8× slower (≈ 380 vs 208–221 µs) and server CPU per probe op 1.7× higher (≈ 600 vs 347–357 µs) |
| Why stat costs more | SOURCE | probe stat = 3 `Lookup` + `GetAttr`; TKV `doLookup` is two gets (entry, then inode) = 2 round trips each → 7 round trips per stat; Redis `Lookup` is one EVALSHA → 4 commands per stat |
| Not measured | NOTE | replication (P2), 39 M through the server, thousands of client processes, mixed read/write load, failure behaviour |

## metad server-side lookup (`feat/metad` b7108445, `results/vm/metad-growth-lookup*`, `metad-ab*`, `metad-creategap.txt`)

New `OpLookup`: the server reads the dentry, derives the attribute key from the inode in it and returns both from one snapshot. `kvMeta.doLookup` uses it when the driver offers it (`kvTxn.lookup`); other TKV drivers keep two gets. `TestMetadLookup` checks the joined path through the prefix client, including the transaction's own buffered writes; `testMeta`, `testTKV`, mem and Badger suites pass.

| Item | Label | Value |
|---|---|---|
| Growth to 4 M, same script and caps as P1 | RUN | stat p50 hot / all 204–232 / 241–254 µs, p99 ≤ 459 µs (P1 371–377 / 383–404); server CPU per probe op 496–534 µs (P1 578–634); gen 103–123 s per 1 M; cgroup 1 305 MB at 4 M, anon 696 MB; 0 OOM |
| Interleaved A/B, one server on the 4 M data, P1 client (15823bd5) vs lookup client, 3 pairs | RUN | stat p50 372–411 → 227–261 µs; server CPU per probe op 667–736 → 577–625 µs; create p50 830–902 → 998–1 015 µs (absolute create is higher than in the growth run: freshly reopened server) |
| Stat against Valkey | RUN | ≈ 230–250 µs vs Valkey 208–221 µs: gap down from 1.8× to ≈ 1.1× |
| Why create looked slower | RUN + SOURCE | `Unlink` of a closed file starts `doDeleteFileData` in a goroutine (`base.go:3052`): a scan and one more synced commit. A create that reaches the server while that fsync runs waits for it. `rnd/creategap` (one dir, n = 2000): with unlink, create p50 ≈ 700 µs after nothing or a 300 µs sleep, ≈ 1 075–1 105 µs right after a lookup or getattr, for the P1 and the new client alike; without unlink the read before create costs nothing (647–663 vs 676–682 µs). The faster resolve in the probe moves its create into that window; the create path itself did not change |
| Follow-up | INFERENCE | the server commits one batch at a time (write, fsync, reply, then the next batch), so a commit that arrives during an fsync pays up to two. Pipelining (write batch N+1 while N syncs) is the fix; it belongs to the Raft log path in P2 |
