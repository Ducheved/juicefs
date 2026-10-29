# RFC: metadata capsules for cold subtrees (Redis engine)

Status: draft for the offline PoC. Nothing here exists in JuiceFS yet.

## Goal

Valkey RAM should depend on hot metadata and control state, not on the whole namespace. A cold subtree keeps its paths, inode numbers, file contents, quota usage and POSIX behavior, but its expanded metadata does not have to stay in Valkey.

Measured baseline (rnd/results/r0): 226–254 B of Valkey `used_memory` per small file, ~2 keys per file (`i<ino>` ≈ 97 B, `c<ino>_0` ≈ 72 B, dentry field 21–41 B, ~35 B main dictionary overhead). The cost is per-key overhead, so packing the same keys tighter in RAM is a constant-factor experiment, not a fix.

## Unit and state

A unit is a directory subtree with root `R`. JuiceFS works with directories only; it does not know about accounts or projects.

| State | Valkey holds | Object storage holds |
|---|---|---|
| HOT | all records as today | data blocks |
| COLD | `R` attr, `R` dentry in its parent, dir stats of `R`, quota records, unit descriptor | data blocks + immutable capsule |
| HYDRATING | as COLD plus a growing set of restored records invisible to clients | capsule (source of truth until HOT) |

Descriptor: `capsule<R>` hash with `state`, `epoch`, `owner` (session id), `object`, `sha256`, `size`, `inodes`, `runs` (inode ranges of the unit). A global `capsules` hash (`R` → epoch) and a `capsuleVer` counter let clients keep the set of cold roots in memory (O(units)) and reload it when the counter changes.

Capsule object: `meta/capsules/<R>/<epoch>` in the volume's bucket, outside `chunks/`, so data GC never lists it as a block. Content reuses the protobuf records of the binary dump (`pb.Node`, `pb.Edge`, `pb.Chunk`, `pb.Xattr`, `pb.Symlink`, `pb.Parent`), framed in sections with a footer of counts and a SHA-256.

## Eligibility (PoC)

- All clients of the volume are stopped, readonly and admin tools included; only the evicting process holds a session.
- No hardlink crosses the unit boundary; no open-unlinked or sustained inode, no lock, no pending delete inside.
- No directory quota target strictly inside the unit. Quota records of `R` stay in Valkey.
- Protected spine: dentries on the path from `/` to every quota target stay in Valkey, because external readers resolve targets with raw `HGET d<inode> <name>` and never pass through JuiceFS hooks.

## Evict

1. Txn: create descriptor `COOLING`, `epoch = e`.
2. Read the subtree from Valkey; write the capsule; read it back and verify size and SHA-256.
3. Txn on the descriptor: `COOLING/e → COLD/e`, add `R` to `capsules`, `INCR capsuleVer`. From here the capsule is authoritative and a GC root.
4. Delete hot keys of the unit in bounded batches. Every batch is a txn that WATCHes the descriptor and aborts unless it is still `COLD/e`.

Logical usage (`usedSpace`, `totalInodes`, quota and dir stats) is not changed: eviction is a change of representation, not unlink.

## Hydrate

Triggered by any operation that needs records inside `R`: lookup/readdir/create/unlink/rename with parent `R`, rmdir or rename-overwrite of `R`, inode access whose inode falls into the unit runs.

1. Txn: `COLD/e → HYDRATING/e+1`, `owner = sid`.
2. Download the capsule, verify SHA-256 and counts.
3. Restore records in bounded batches; each batch WATCHes the descriptor and requires `HYDRATING/e+1` with our `owner`. A stale worker of an older epoch cannot write.
4. Txn: `HYDRATING/e+1 → HOT`, remove `R` from `capsules`, `INCR capsuleVer`, delete the descriptor. The capsule object is deleted after that.

Other operations on the unit wait while it is `HYDRATING`. A hydrator whose session expires is replaced by a new one with a new epoch; restore is idempotent because the capsule is immutable and nothing inside the unit could change while it was cold.

## Failure handling

| Crash point | Recovery |
|---|---|
| before `COLD` commit | hot keys intact; delete descriptor and object |
| after `COLD`, during key deletion | resume deletion, or hydrate (overwrites identical records) |
| during hydrate | new hydrator, new epoch, restore again |
| lost ACK of a txn | re-read the descriptor; every transition is checked by state and epoch |
| capsule missing or corrupt | unit stays `COLD`, operations return EIO with the damage state; never ENOENT or an empty directory; destructive GC refuses to run |

## Maintenance

- `ScanSlices` streams slices of every capsule as live references (bounded memory), so `juicefs gc` keeps their blocks.
- `cleanupLeakedInodes/Chunks`: hot keys of a cold unit are gone, `R` stays referenced, nothing is reported as leaked.
- PoC rejects `fsck`, `dump`, `backup`, `load`, `clone` while any unit is cold.

## Version gate

Enabling capsules raises `MetaVersion` to 2 (fork `MaxVersion = 2`). Old mounts and tools refuse to start, and running old mounts exit at the next `reloadFormat` (it checks `MetaVersion`, not `MinClientVersion`). The PoC still requires all clients stopped before each eviction.

## What the PoC must measure

- `used_memory` freed per unit and residual per unit (descriptor, `R` records, runs).
- Hydrate time and peak Valkey memory vs unit size; concurrent hydrations.
- Inode numbers, types, modes, owners, links, xattrs and content hashes equal to an independent manifest after hydrate.
- Every crash point above.

## Not in the PoC

Online eviction (pins, leases, fencing of cached inodes), page-level reads without full hydration, mutation overlays and compaction. Whole-unit hydration can bring back all of a unit's RAM at once, so unit size and concurrent hydrations need limits.
