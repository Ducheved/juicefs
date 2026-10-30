---
title: Evicting Cold Metadata (Experimental)
sidebar_position: 3
slug: /metadata_capsules
---

With the Redis metadata engine, every file costs memory in Redis even when nobody uses it. `juicefs metadata` moves the metadata of a cold directory tree into an immutable object in the volume's object storage, keeps the tree in the namespace, and loads it back automatically on the first access. File data is not touched.

Only the Redis engine is supported.

## Commands

```shell
# move metadata of /accounts/a1/gen3 out of Redis
juicefs metadata evict redis://host:6379/1 /accounts/a1/gen3

# list evicted directories
juicefs metadata status redis://host:6379/1

# load a directory back ahead of use
juicefs metadata prefetch redis://host:6379/1 /accounts/a1/gen3
```

`evict` refuses trees larger than `--max-inodes` (default 1 000 000).

## What stays the same

- Paths, inode numbers, permissions, links, xattrs and file contents.
- Quota usage and limits. Quota records and the directory entries leading to quota targets stay in Redis, so tools that read them directly keep working.
- `juicefs summary` and `du` of an evicted directory are answered from totals stored at eviction time, without loading it.

## How it works with running clients

Mounts do not need to be stopped. `evict` publishes the directory as cooling; every live client blocks new changes inside it, finishes running transactions, flushes its statistics and confirms. The tree is then written to `meta/capsules/<inode>/<epoch>`, verified, committed, and removed from Redis after all clients confirm again. Any access afterwards — lookup, listing, open, a cached inode in the kernel — loads the whole tree back before the operation continues.

`evict` fails without changing anything when a client has a file open in the tree, when a client does not confirm within 30 seconds, or when the tree is used again before the eviction finishes.

## Requirements and limits

- Enabling the first eviction raises the volume's metadata version to 2. Older clients and tools refuse to start, and running older mounts exit at their next setting reload.
- Read-only mounts and clients without a session cannot load evicted trees and return an error for their contents.
- A tree must not contain a directory quota below its root, an open or locked file, a hard link to a file outside the tree, or another evicted tree.
- `juicefs gc` reads the evicted metadata and keeps its data blocks; it fails if a capsule cannot be read.
- `juicefs dump`, `fsck`, `clone` and the automatic metadata backup refuse to run while any tree is evicted. Load trees back with `prefetch` first.
- Loading is per tree: the first access to a large tree takes about as long as writing its metadata to Redis (about 2 s for 200 000 files on a local Redis), and the whole tree uses Redis memory again.
