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

## PostgreSQL 17 (control, PG-only)

| Item | Label | Value |
|---|---|---|
| Namespace 1 → 4 M files (100 users × 100 dirs × 100 files per round, per-user dir quota) | RUN | DB 599 / 1188 / 1769 / 2337 MB (≈ 585 B/file on disk), PSS of all postgres processes 0.48 / 0.58 / 0.58 / 0.59 GB (plateau at shared_buffers) (`rnd/results/pg`) |
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
| Q = 100 000 uid quotas, one client, 10 uids in use (`zz_ugbench_test.go`, base vs change, same Valkey) | RUN | refresh 316 ms → 0.16 ms, 7.3 MB → 562 B per refresh, a flush during refresh waited 305 ms → 0.18 ms |
| C = 1 000 clients, 12 s heartbeat, extrapolated | CALC | ≈ 610 MB/s → ≈ 47 KB/s out of Valkey |
| Miss on first write of a uid/gid: one HMGET for uid and gid together, fail-closed EIO after 3 failed loads | RUN | `testUserGroupQuotaRefresh` on Redis, SQLite, MemKV |
| Refresh outside the flush lock; a flush during the read keeps local usage and the stale ids are reread | RUN | mutations removing the generation check or the reread fail the test |
| Adversarial review | RUN | found an SQLite deadlock (fetch under the flush lock inside a txn) in an intermediate version; fixed, regression test hangs on the old code |
