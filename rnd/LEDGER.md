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
