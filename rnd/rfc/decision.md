# RFC: decision after the first R&D round

Inputs: `rnd/LEDGER.md`, raw results in `rnd/results`. Every number below is RUN on the stand unless marked.

## Options

| Option | What changes | RAM depends on | Status |
|---|---|---|---|
| A. Scoped quota refresh | client reads only quota chains it uses, TTL revalidation | active chains per client | branch `feat/quota-scoped-refresh`, reviewed, tests red on main / green |
| B. Coarse metadata capsules (whole subtree) | cold subtree metadata moves to an immutable object; whole-unit hydration on access | hot units + ~1 KB per cold unit | branch `poc/metadata-capsule`, offline PoC |
| C. Page-level cold metadata | read single inodes/dentries from indexed pages without hydration | hot records + page cache | not built |
| D. Disk-backed engine (Kvrocks, TiKV, PostgreSQL) | replace Valkey | block cache budget | Kvrocks measured as control |

## What the measurements say

- A removes the O(Q) per-client work: at Q = 10⁵ startup 1.4 ms instead of 154 ms (main) or 7.7 s (v1.4.2), steady traffic flat in Q.
- B removes the per-inode RAM of cold units: cold namespace ×11 (0.9 M → 9.9 M files) moved `used_memory` from 29.6 MB to 30.6 MB with a fixed hot set; hot latency unchanged. Hydration of a 10 000-file unit: ~87 ms locally, peak RAM = the unit itself.
- B does not bound RAM of active units: an active unit is fully expanded. RAM follows the size of what is active, not the number of files touched.
- D (Kvrocks) keeps the Redis API and plateaus at ~0.8 GB RSS with a 512 MB block cache from 3 M to 4 M files; stat is 1.6–1.8× slower (148–175 vs 95 µs p50), create is similar with fsync. Open correctness gates: #3555 reproduced, SELECT is a no-op by default, DBSIZE is async.

## Model for the owner's workload

Assumptions (PROPOSAL, replace with evidence): 3 projects × 20 000 files per account, unit = project, 234 B per hot file (USER), ~1 KB per cold unit, primary only (replicas ×3).

| Accounts | Files | Valkey today | B, 10 % projects active | B, 1 % active |
|---|---|---|---|---|
| 20 | 1.2 M | 0.28 GB | 0.03 GB | 0.003 GB |
| 1 000 | 60 M | 14 GB | 1.4 GB | 0.14 GB |
| 100 000 | 6 B | 1.4 TB | 140 GB | 14 GB + 0.3 GB of cold units |

Capsules in object storage: ~160 B per file uncompressed (~1 TB for 6 B files).

With 2 GiB `maxmemory` and ~4 GiB free per node, B fits 1 000 accounts at 10 % activity, and 100 000 accounts only if activity is well below 1 % or units are smaller. Beyond that the hot working set itself needs C (page-level) or D.

## Recommendation

1. Ship A as the first upstream PR series: independent value, current Redis layout, compatible with external writers.
2. Continue B to an online-capable RFC: pins and fencing for cached inodes, GC and dump over capsules, unit size limits. It already proves that cold namespace RAM can be decoupled without a new metadata server.
3. Decide C vs D with the owner's active-set numbers: if active projects × size exceeds the RAM budget, B alone is not enough.

## Next falsifying experiments

- Hydration under S3 latency (50–100 ms GET) and a 200 000-file unit: first-access delay and peak RAM.
- Many concurrent hydrations under a memory budget: admission and backpressure.
- Kvrocks: which JuiceFS transactions can hit #3555 (any multi-field write that can fail at runtime), and RSS at 10⁸ files.
