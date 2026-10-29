package meta

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/redis/go-redis/v9"
)

const capsuleBatch = 500

func (m *redisMeta) capsuleKey(root Ino) string {
	return m.prefix + "capsule" + root.String()
}

func (m *redisMeta) capsulesKey() string {
	return m.prefix + "capsules"
}

func (m *redisMeta) capsuleVerKey() string {
	return m.prefix + "capsuleVer"
}

func parseCapsuleInfo(root Ino, vals map[string]string) *CapsuleInfo {
	if len(vals) == 0 {
		return nil
	}
	u := func(k string) uint64 {
		v, _ := strconv.ParseUint(vals[k], 10, 64)
		return v
	}
	return &CapsuleInfo{
		Root:    root,
		State:   vals["state"],
		Epoch:   u("epoch"),
		Owner:   u("owner"),
		Object:  vals["object"],
		Sum:     vals["sum"],
		Size:    int64(u("size")),
		Records: int64(u("records")),
		Inodes:  int64(u("inodes")),
		Keys:    int64(u("keys")),
	}
}

func (m *redisMeta) getCapsule(ctx context.Context, c redis.Cmdable, root Ino) (*CapsuleInfo, error) {
	vals, err := c.HGetAll(ctx, m.capsuleKey(root)).Result()
	if err != nil {
		return nil, err
	}
	return parseCapsuleInfo(root, vals), nil
}

func (m *redisMeta) capsuleTxn(ctx Context, root Ino, check func(info *CapsuleInfo) error, effects func(pipe redis.Pipeliner)) error {
	key := m.capsuleKey(root)
	return m.txn(ctx, func(tx *redis.Tx) error {
		info, err := m.getCapsule(ctx, tx, root)
		if err != nil {
			return err
		}
		if err = check(info); err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			effects(pipe)
			return nil
		})
		return err
	}, key)
}

func (m *redisMeta) sessionAlive(ctx context.Context, sid uint64) bool {
	_, err := m.rdb.ZScore(ctx, m.allSessions(), strconv.FormatUint(sid, 10)).Result()
	return err == nil
}

func (m *redisMeta) relKey(key string) string {
	return strings.TrimPrefix(key, m.prefix)
}

type capsuleSnapshot struct {
	recs   []capsuleRecord
	keys   []string
	fields map[string][]string
	inodes int64
}

func (m *redisMeta) snapshotUnit(ctx Context, root Ino, maxInodes int64) (*capsuleSnapshot, error) {
	snap := &capsuleSnapshot{fields: make(map[string][]string)}
	inUnit := map[Ino]bool{root: true}
	dirs := []Ino{root}
	var others []Ino
	for i := 0; i < len(dirs); i++ {
		dir := dirs[i]
		key := m.entryKey(dir)
		vals, err := m.rdb.HGetAll(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		if len(vals) == 0 {
			continue
		}
		rec := capsuleRecord{kind: capsuleRecHash, key: m.relKey(key)}
		names := make([]string, 0, len(vals))
		for name := range vals {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			v := vals[name]
			rec.vals = append(rec.vals, []byte(name), []byte(v))
			typ, ino := m.parseEntry([]byte(v))
			if inUnit[ino] {
				continue
			}
			inUnit[ino] = true
			others = append(others, ino)
			if typ == TypeDirectory {
				dirs = append(dirs, ino)
			}
		}
		snap.recs = append(snap.recs, rec)
		snap.keys = append(snap.keys, key)
		if maxInodes > 0 && int64(len(inUnit)) > maxInodes {
			return nil, fmt.Errorf("unit has more than %d inodes: %w", maxInodes, syscall.EFBIG)
		}
	}
	snap.inodes = int64(len(inUnit))
	isDir := make(map[Ino]bool, len(dirs))
	for _, d := range dirs {
		isDir[d] = true
	}
	stats := []string{m.dirDataLengthKey(), m.dirUsedSpaceKey(), m.dirUsedInodesKey()}
	for i := 0; i < len(others); i += capsuleBatch {
		batch := others[i:min(i+capsuleBatch, len(others))]
		cmds, err := m.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, ino := range batch {
				pipe.Get(ctx, m.inodeKey(ino))
				pipe.HGetAll(ctx, m.xattrKey(ino))
				pipe.Exists(ctx, m.flockKey(ino), m.plockKey(ino))
				pipe.HExists(ctx, m.dirQuotaKey(), ino.String())
			}
			return nil
		})
		if err != nil && err != redis.Nil {
			return nil, err
		}
		var second []func(pipe redis.Pipeliner)
		var collect []func([]redis.Cmder, int) (int, error)
		for j, ino := range batch {
			ino := ino
			buf, err := cmds[4*j].(*redis.StringCmd).Bytes()
			if err != nil {
				return nil, fmt.Errorf("attribute of inode %d: %w", ino, err)
			}
			if n := cmds[4*j+2].(*redis.IntCmd).Val(); n > 0 {
				return nil, fmt.Errorf("inode %d is locked: %w", ino, syscall.EBUSY)
			}
			if cmds[4*j+3].(*redis.BoolCmd).Val() {
				return nil, fmt.Errorf("directory %d inside the unit has a quota: %w", ino, syscall.EPERM)
			}
			key := m.inodeKey(ino)
			snap.recs = append(snap.recs, capsuleRecord{kind: capsuleRecString, key: m.relKey(key), vals: [][]byte{buf}})
			snap.keys = append(snap.keys, key)
			if xs := cmds[4*j+1].(*redis.MapStringStringCmd).Val(); len(xs) > 0 {
				rec := capsuleRecord{kind: capsuleRecHash, key: m.relKey(m.xattrKey(ino))}
				for k, v := range xs {
					rec.vals = append(rec.vals, []byte(k), []byte(v))
				}
				snap.recs = append(snap.recs, rec)
				snap.keys = append(snap.keys, m.xattrKey(ino))
			}
			var attr Attr
			m.parseAttr(buf, &attr)
			if attr.Parent == 0 {
				second = append(second, func(pipe redis.Pipeliner) { pipe.HGetAll(ctx, m.parentKey(ino)) })
				collect = append(collect, func(cs []redis.Cmder, k int) (int, error) {
					ps := cs[k].(*redis.MapStringStringCmd).Val()
					if len(ps) == 0 {
						return k + 1, nil
					}
					rec := capsuleRecord{kind: capsuleRecHash, key: m.relKey(m.parentKey(ino))}
					for p, v := range ps {
						pi, _ := strconv.ParseUint(p, 10, 64)
						if n, _ := strconv.Atoi(v); n > 0 && !isDir[Ino(pi)] {
							return 0, fmt.Errorf("inode %d has a link outside the unit: %w", ino, syscall.EXDEV)
						}
						rec.vals = append(rec.vals, []byte(p), []byte(v))
					}
					snap.recs = append(snap.recs, rec)
					snap.keys = append(snap.keys, m.parentKey(ino))
					return k + 1, nil
				})
			}
			switch attr.Typ {
			case TypeFile:
				for indx := uint32(0); uint64(indx)*ChunkSize < attr.Length; indx++ {
					indx := indx
					second = append(second, func(pipe redis.Pipeliner) { pipe.LRange(ctx, m.chunkKey(ino, indx), 0, -1) })
					collect = append(collect, func(cs []redis.Cmder, k int) (int, error) {
						items := cs[k].(*redis.StringSliceCmd).Val()
						if len(items) > 0 {
							rec := capsuleRecord{kind: capsuleRecList, key: m.relKey(m.chunkKey(ino, indx))}
							for _, it := range items {
								rec.vals = append(rec.vals, []byte(it))
							}
							snap.recs = append(snap.recs, rec)
							snap.keys = append(snap.keys, m.chunkKey(ino, indx))
						}
						return k + 1, nil
					})
				}
			case TypeSymlink:
				second = append(second, func(pipe redis.Pipeliner) { pipe.Get(ctx, m.symKey(ino)) })
				collect = append(collect, func(cs []redis.Cmder, k int) (int, error) {
					target, err := cs[k].(*redis.StringCmd).Bytes()
					if err != nil {
						return 0, fmt.Errorf("symlink %d: %w", ino, err)
					}
					snap.recs = append(snap.recs, capsuleRecord{kind: capsuleRecString, key: m.relKey(m.symKey(ino)), vals: [][]byte{target}})
					snap.keys = append(snap.keys, m.symKey(ino))
					return k + 1, nil
				})
			case TypeDirectory:
				second = append(second, func(pipe redis.Pipeliner) {
					for _, h := range stats {
						pipe.HGet(ctx, h, ino.String())
					}
				})
				collect = append(collect, func(cs []redis.Cmder, k int) (int, error) {
					for _, h := range stats {
						if v, err := cs[k].(*redis.StringCmd).Bytes(); err == nil {
							snap.recs = append(snap.recs, capsuleRecord{kind: capsuleRecField, key: m.relKey(h), vals: [][]byte{[]byte(ino.String()), v}})
							snap.fields[h] = append(snap.fields[h], ino.String())
						}
						k++
					}
					return k, nil
				})
			}
		}
		if len(second) == 0 {
			continue
		}
		cmds, err = m.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, f := range second {
				f(pipe)
			}
			return nil
		})
		if err != nil && err != redis.Nil {
			return nil, err
		}
		k := 0
		for _, c := range collect {
			if k, err = c(cmds, k); err != nil {
				return nil, err
			}
		}
	}
	return snap, nil
}

func (m *redisMeta) doEvictCapsule(ctx Context, root Ino, store object.ObjectStorage, maxInodes int64) (*CapsuleInfo, error) {
	if m.sid == 0 {
		return nil, fmt.Errorf("a session is required to evict metadata")
	}
	if n, err := m.rdb.ZCard(ctx, m.allSessions()).Result(); err != nil {
		return nil, err
	} else if n != 1 {
		return nil, fmt.Errorf("%d sessions are alive, stop all other clients first: %w", n, syscall.EBUSY)
	}
	var attr Attr
	if st := m.doGetAttr(ctx, root, &attr); st != 0 {
		return nil, st
	}
	if attr.Typ != TypeDirectory {
		return nil, syscall.ENOTDIR
	}
	e, err := m.incrCounter("nextCapsuleEpoch", 1)
	if err != nil {
		return nil, err
	}
	epoch := uint64(e)
	err = m.capsuleTxn(ctx, root, func(info *CapsuleInfo) error {
		if info != nil {
			return fmt.Errorf("inode %d is already %s: %w", root, info.State, syscall.EEXIST)
		}
		return nil
	}, func(pipe redis.Pipeliner) {
		pipe.HSet(ctx, m.capsuleKey(root), "state", CapsuleCooling, "epoch", epoch, "owner", m.sid)
	})
	if err != nil {
		return nil, err
	}
	abort := func(cause error) (*CapsuleInfo, error) {
		_ = m.capsuleTxn(ctx, root, func(info *CapsuleInfo) error {
			if info == nil || info.State != CapsuleCooling || info.Epoch != epoch {
				return syscall.ESTALE
			}
			return nil
		}, func(pipe redis.Pipeliner) { pipe.Del(ctx, m.capsuleKey(root)) })
		return nil, cause
	}
	snap, err := m.snapshotUnit(ctx, root, maxInodes)
	if err != nil {
		return abort(err)
	}
	data, sum := encodeCapsule(root, epoch, snap.recs)
	obj := capsuleObject(root, epoch)
	if err = store.Put(ctx, obj, bytes.NewReader(data)); err != nil {
		return abort(fmt.Errorf("upload capsule: %w", err))
	}
	if err = m.verifyCapsule(ctx, store, obj, root, epoch, sum); err != nil {
		return abort(err)
	}
	info := &CapsuleInfo{Root: root, State: CapsuleCold, Epoch: epoch, Owner: m.sid, Object: obj, Sum: sum, Size: int64(len(data)),
		Records: int64(len(snap.recs)), Inodes: snap.inodes, Keys: int64(len(snap.keys))}
	err = m.capsuleTxn(ctx, root, func(cur *CapsuleInfo) error {
		if cur == nil || cur.State != CapsuleCooling || cur.Epoch != epoch {
			return fmt.Errorf("capsule of inode %d changed during eviction: %w", root, syscall.ESTALE)
		}
		return nil
	}, func(pipe redis.Pipeliner) {
		pipe.HSet(ctx, m.capsuleKey(root), "state", CapsuleCold, "object", obj, "sum", sum, "size", info.Size,
			"records", info.Records, "inodes", info.Inodes, "keys", info.Keys)
		pipe.HSet(ctx, m.capsulesKey(), root.String(), epoch)
		pipe.Incr(ctx, m.capsuleVerKey())
	})
	if err != nil {
		return abort(err)
	}
	if err = m.dropUnitKeys(ctx, root, epoch, snap); err != nil {
		return info, fmt.Errorf("capsule committed, cleanup of hot keys failed, run evict again: %w", err)
	}
	return info, nil
}

func (m *redisMeta) dropUnitKeys(ctx Context, root Ino, epoch uint64, snap *capsuleSnapshot) error {
	check := func(info *CapsuleInfo) error {
		if info == nil || info.State != CapsuleCold || info.Epoch != epoch {
			return fmt.Errorf("capsule of inode %d is no longer cold at epoch %d: %w", root, epoch, syscall.ESTALE)
		}
		return nil
	}
	for i := 0; i < len(snap.keys); i += capsuleBatch {
		batch := snap.keys[i:min(i+capsuleBatch, len(snap.keys))]
		if err := m.capsuleTxn(ctx, root, check, func(pipe redis.Pipeliner) { pipe.Del(ctx, batch...) }); err != nil {
			return err
		}
	}
	for h, fields := range snap.fields {
		for i := 0; i < len(fields); i += capsuleBatch {
			batch := fields[i:min(i+capsuleBatch, len(fields))]
			if err := m.capsuleTxn(ctx, root, check, func(pipe redis.Pipeliner) { pipe.HDel(ctx, h, batch...) }); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *redisMeta) readCapsule(ctx context.Context, store object.ObjectStorage, obj string) ([]byte, error) {
	r, err := store.Get(ctx, obj, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("download capsule %s: %w", obj, err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

func (m *redisMeta) verifyCapsule(ctx context.Context, store object.ObjectStorage, obj string, root Ino, epoch uint64, sum string) error {
	data, err := m.readCapsule(ctx, store, obj)
	if err != nil {
		return err
	}
	_, err = decodeCapsule(data, root, epoch, sum)
	return err
}

func (m *redisMeta) doHydrateCapsule(ctx Context, root Ino, store object.ObjectStorage, force bool) (*CapsuleInfo, error) {
	if m.sid == 0 {
		return nil, fmt.Errorf("a session is required to hydrate metadata")
	}
	info, err := m.getCapsule(ctx, m.rdb, root)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("inode %d has no capsule: %w", root, syscall.ENOENT)
	}
	takeover := force || info.Owner == m.sid || !m.sessionAlive(ctx, info.Owner)
	switch info.State {
	case CapsuleCold:
	case CapsuleHydrating:
		if !takeover {
			return nil, fmt.Errorf("inode %d is being hydrated by session %d: %w", root, info.Owner, syscall.EAGAIN)
		}
	case CapsuleCooling:
		if !takeover {
			return nil, fmt.Errorf("inode %d is being evicted by session %d: %w", root, info.Owner, syscall.EAGAIN)
		}
		err = m.capsuleTxn(ctx, root, func(cur *CapsuleInfo) error {
			if cur == nil || cur.State != CapsuleCooling || cur.Epoch != info.Epoch {
				return syscall.ESTALE
			}
			return nil
		}, func(pipe redis.Pipeliner) { pipe.Del(ctx, m.capsuleKey(root)) })
		if err == nil {
			_ = store.Delete(ctx, capsuleObject(root, info.Epoch))
		}
		return info, err
	default:
		return nil, fmt.Errorf("inode %d has capsule in unknown state %q: %w", root, info.State, syscall.EIO)
	}
	e, err := m.incrCounter("nextCapsuleEpoch", 1)
	if err != nil {
		return nil, err
	}
	epoch := uint64(e)
	err = m.capsuleTxn(ctx, root, func(cur *CapsuleInfo) error {
		if cur == nil || cur.State != info.State || cur.Epoch != info.Epoch {
			return fmt.Errorf("capsule of inode %d changed: %w", root, syscall.ESTALE)
		}
		return nil
	}, func(pipe redis.Pipeliner) {
		pipe.HSet(ctx, m.capsuleKey(root), "state", CapsuleHydrating, "epoch", epoch, "owner", m.sid)
	})
	if err != nil {
		return nil, err
	}
	data, err := m.readCapsule(ctx, store, info.Object)
	if err != nil {
		return nil, err
	}
	var objEpoch uint64
	if _, err = fmt.Sscanf(info.Object[strings.LastIndex(info.Object, "/")+1:], "%d", &objEpoch); err != nil {
		return nil, fmt.Errorf("bad capsule object name %q", info.Object)
	}
	recs, err := decodeCapsule(data, root, objEpoch, info.Sum)
	if err != nil {
		return nil, fmt.Errorf("capsule of inode %d is damaged: %s: %w", root, err, syscall.EIO)
	}
	check := func(cur *CapsuleInfo) error {
		if cur == nil || cur.State != CapsuleHydrating || cur.Epoch != epoch || cur.Owner != m.sid {
			return fmt.Errorf("hydration of inode %d at epoch %d was taken over: %w", root, epoch, syscall.ESTALE)
		}
		return nil
	}
	for i := 0; i < len(recs); i += capsuleBatch {
		batch := recs[i:min(i+capsuleBatch, len(recs))]
		err = m.capsuleTxn(ctx, root, check, func(pipe redis.Pipeliner) {
			for _, r := range batch {
				key := m.prefix + r.key
				switch r.kind {
				case capsuleRecString:
					pipe.Set(ctx, key, r.vals[0], 0)
				case capsuleRecHash:
					pipe.Del(ctx, key)
					args := make([]interface{}, len(r.vals))
					for j, v := range r.vals {
						args[j] = v
					}
					pipe.HSet(ctx, key, args...)
				case capsuleRecList:
					pipe.Del(ctx, key)
					args := make([]interface{}, len(r.vals))
					for j, v := range r.vals {
						args[j] = v
					}
					pipe.RPush(ctx, key, args...)
				case capsuleRecField:
					pipe.HSet(ctx, key, r.vals[0], r.vals[1])
				}
			}
		})
		if err != nil {
			return nil, err
		}
	}
	err = m.capsuleTxn(ctx, root, check, func(pipe redis.Pipeliner) {
		pipe.Del(ctx, m.capsuleKey(root))
		pipe.HDel(ctx, m.capsulesKey(), root.String())
		pipe.Incr(ctx, m.capsuleVerKey())
	})
	if err != nil {
		return nil, err
	}
	if err = store.Delete(ctx, info.Object); err != nil {
		logger.Warnf("Delete capsule %s: %s", info.Object, err)
	}
	info.State, info.Epoch, info.Owner = "", epoch, m.sid
	return info, nil
}

func (m *redisMeta) doListCapsules(ctx Context) ([]*CapsuleInfo, error) {
	roots, err := m.rdb.HKeys(ctx, m.capsulesKey()).Result()
	if err != nil {
		return nil, err
	}
	var keys []string
	if err = m.scan(ctx, "capsule[0-9]*", func(ks []string) error {
		keys = append(keys, ks...)
		return nil
	}); err != nil {
		return nil, err
	}
	seen := make(map[Ino]bool)
	var infos []*CapsuleInfo
	add := func(root Ino) error {
		if seen[root] {
			return nil
		}
		seen[root] = true
		info, err := m.getCapsule(ctx, m.rdb, root)
		if err != nil {
			return err
		}
		if info != nil {
			infos = append(infos, info)
		}
		return nil
	}
	for _, r := range roots {
		v, _ := strconv.ParseUint(r, 10, 64)
		if err = add(Ino(v)); err != nil {
			return nil, err
		}
	}
	for _, k := range keys {
		v, _ := strconv.ParseUint(strings.TrimPrefix(m.relKey(k), "capsule"), 10, 64)
		if err = add(Ino(v)); err != nil {
			return nil, err
		}
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Root < infos[j].Root })
	return infos, nil
}
