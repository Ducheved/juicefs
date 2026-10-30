# Decision status

No production metadata backend is chosen. Branches in this fork are research or upstream fixes, not an architecture decision.

## Goal

Metadata on local SSD with a bounded RAM cache; object storage holds file data and rare batch backups. A cold stat or lookup must not become an object storage request. Criteria: less CPU, less RAM, fewer object storage requests, lower latency, thousands of clients.

## Gates before choosing a backend

1. Redis/Lua dependency inventory of JuiceFS and of every external writer, verified on each candidate. Status: JuiceFS done for Kvrocks 2.17.0 (`rnd/LEDGER.md`, "Redis/Lua dependency gate"); external writers are checked outside this repository.
2. Comparative measurements on the same corpus and stand: RAM slope under a cap, p50/p99 of lookup, stat and create, CPU per operation, bytes per file, disk including WAL, behaviour on a full disk and after kill -9. Status: Kvrocks growth done to 8 M files; PostgreSQL, Badger and Valkey under the same cap pending.
3. Fault semantics: atomicity of EXEC and scripts under storage errors, recovery without operator action. Status: Kvrocks full-disk done (atomic, needs restart); kill -9 with `sync yes` done (counters exact); lost-ACK replay pending.

## Kvrocks after gate 1

- Works with JuiceFS without code changes except one: read-only JuiceFS scripts must use EVALSHA_RO, otherwise every lookup aborts concurrent writes in the same directory (`fix/kvrocks-guards` 9fdbc1e).
- External writers whose scripts touch keys outside KEYS need `lua-strict-key-accessing no`, which runs every EVAL under a global lock; JuiceFS scripts no longer take it after the fix.
- Operational costs to price: archived WAL for 3 h of writes, manual restart after a full disk, about 2× lookup and 3× create cost of Redis on the same box.

## Archived

Metadata capsules in object storage (`poc/metadata-capsule`): excluded by the owner (cold access must not become a paid GET); the Big-O audit in the ledger lists the per-client costs that also rule it out at 1 000 clients.

## Next

- Same growth and mixload runs on PostgreSQL 16 and Badger under the same memory cap.
- Lost ACK: count operations committed on the server while the client reported EIO.
- Q2 from the owner (cross-account rename, hardlinks, shared directories) decides whether a partitioned metadata service is needed.
