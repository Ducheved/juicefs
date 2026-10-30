package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/redis/go-redis/v9"
)

const (
	capsuleBatch       = 500
	capsuleBatchValues = 20000
)

var (
	capsuleAckTimeout = 30 * time.Second
	capsuleDropGrace  = 2 * time.Second
)

type capsuleRestoreOp struct {
	rec   *capsuleRecord
	vals  [][]byte
	first bool
}

func capsuleRestoreBatches(recs []capsuleRecord, limit int) [][]capsuleRestoreOp {
	var batches [][]capsuleRestoreOp
	var cur []capsuleRestoreOp
	size := 0
	flush := func() {
		if len(cur) > 0 {
			batches = append(batches, cur)
			cur, size = nil, 0
		}
	}
	for i := range recs {
		r := &recs[i]
		step := len(r.vals)
		if r.kind == capsuleRecHash {
			step = limit &^ 1
		} else if r.kind == capsuleRecList {
			step = limit
		}
		for off := 0; off < len(r.vals); off += step {
			vals := r.vals[off:min(off+step, len(r.vals))]
			if size+len(vals) > limit {
				flush()
			}
			cur = append(cur, capsuleRestoreOp{rec: r, vals: vals, first: off == 0})
			size += len(vals)
		}
	}
	flush()
	return batches
}

var _ capsuleEngine = (*redisMeta)(nil)

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
	runs := decodeRuns([]byte(vals["runs"]))
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
		Runs:    int64(len(runs)),
		Summary: Summary{Length: u("length"), Size: u("space"), Files: u("files"), Dirs: u("dirs")},
		runs:    runs,
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
	recs    []capsuleRecord
	keys    []string
	fields  map[string][]string
	inodes  int64
	runs    []inoRun
	summary Summary
}

func (m *redisMeta) snapshotUnit(ctx Context, root Ino, maxInodes int64) (*capsuleSnapshot, error) {
	snap := &capsuleSnapshot{fields: make(map[string][]string)}
	inUnit := map[Ino]bool{root: true}
	links := make(map[Ino]uint64)
	dirs := []Ino{root}
	var others []Ino
	cold, err := m.rdb.HKeys(ctx, m.capsulesKey()).Result()
	if err != nil {
		return nil, err
	}
	coldRoots := make(map[Ino]bool, len(cold))
	for _, c := range cold {
		v, _ := strconv.ParseUint(c, 10, 64)
		coldRoots[Ino(v)] = true
	}
	for i := 0; i < len(dirs); i++ {
		dir := dirs[i]
		if i > 0 && coldRoots[dir] {
			return nil, fmt.Errorf("directory %d inside the unit is already evicted: %w", dir, syscall.EBUSY)
		}
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
			links[ino]++
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
	snap.runs = inoRuns(append(others, root))
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
			n := links[ino]
			if attr.Typ == TypeDirectory {
				snap.summary.Dirs += n
				snap.summary.Size += n * uint64(align4K(0))
			} else {
				snap.summary.Files += n
				snap.summary.Size += n * uint64(align4K(attr.Length))
				if attr.Typ == TypeFile {
					snap.summary.Length += n * attr.Length
				}
			}
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

func (m *redisMeta) evictingKey() string {
	return m.prefix + "capsuleEvicting"
}

func (m *redisMeta) lockEviction(ctx Context) error {
	return m.txn(ctx, func(tx *redis.Tx) error {
		owner, err := tx.Get(ctx, m.evictingKey()).Uint64()
		if err != nil && err != redis.Nil {
			return err
		}
		if err == nil && owner != m.sid && m.sessionAlive(ctx, owner) {
			return fmt.Errorf("metadata eviction is running in session %d: %w", owner, syscall.EBUSY)
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, m.evictingKey(), m.sid, 0)
			return nil
		})
		return err
	}, m.evictingKey())
}

func (m *redisMeta) unlockEviction(ctx Context) {
	_ = m.txn(ctx, func(tx *redis.Tx) error {
		if owner, err := tx.Get(ctx, m.evictingKey()).Uint64(); err != nil || owner != m.sid {
			return err
		}
		_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Del(ctx, m.evictingKey())
			return nil
		})
		return err
	}, m.evictingKey())
}

func (m *redisMeta) capsuleAckKey() string {
	return m.prefix + "capsuleAck"
}

func (m *redisMeta) doAckCapsules(ctx Context, ver int64, busy string) error {
	v := strconv.FormatInt(ver, 10)
	if busy != "" {
		v += " " + busy
	}
	return m.rdb.HSet(ctx, m.capsuleAckKey(), strconv.FormatUint(m.sid, 10), v).Err()
}

func (m *redisMeta) unitOfKey(s *capsuleSet, key string) (Ino, bool) {
	k := strings.TrimPrefix(key, m.prefix)
	if len(k) < 2 || !strings.ContainsRune("idcxps", rune(k[0])) {
		return 0, false
	}
	k = k[1:]
	if i := strings.IndexByte(k, '_'); i > 0 && key[len(m.prefix)] == 'c' {
		k = k[:i]
	}
	ino, err := strconv.ParseUint(k, 10, 64)
	if err != nil {
		return 0, false
	}
	return s.unitOf(Ino(ino))
}

func (m *redisMeta) capsuleGate(ctx Context, keys []string) (func(), syscall.Errno) {
	for i := 0; ; i++ {
		m.capsuleSetMu.RLock()
		s := m.capsules.Load()
		var root Ino
		if s != nil && len(s.roots) > 0 {
			for _, k := range keys {
				if r, ok := m.unitOfKey(s, k); ok {
					root = r
					break
				}
			}
		}
		if root == 0 {
			return m.capsuleSetMu.RUnlock, 0
		}
		state := s.roots[root].State
		m.capsuleSetMu.RUnlock()
		if state == CapsuleCooling {
			if i >= 600 {
				return nil, syscall.EAGAIN
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if st := m.hydrateUnit(ctx, root); st != 0 {
			return nil, st
		}
	}
}

func (m *redisMeta) capsuleVersion(ctx context.Context, c redis.Cmdable) (int64, error) {
	v, err := c.Get(ctx, m.capsuleVerKey()).Int64()
	if err == redis.Nil {
		return 0, nil
	}
	return v, err
}

func (m *redisMeta) pendingAcks(ctx context.Context, c redis.Cmdable, ver int64) (int, error) {
	live, err := c.ZRangeByScore(ctx, m.allSessions(), &redis.ZRangeBy{Min: strconv.FormatInt(time.Now().Unix(), 10), Max: "+inf"}).Result()
	if err != nil {
		return 0, err
	}
	acks, err := c.HGetAll(ctx, m.capsuleAckKey()).Result()
	if err != nil {
		return 0, err
	}
	pending := 0
	for _, sid := range live {
		if sid == strconv.FormatUint(m.sid, 10) {
			continue
		}
		parts := strings.SplitN(acks[sid], " ", 2)
		v, _ := strconv.ParseInt(parts[0], 10, 64)
		if v < ver {
			pending++
		} else if len(parts) == 2 {
			return 0, fmt.Errorf("session %s refused: %s: %w", sid, parts[1], syscall.EBUSY)
		}
	}
	return pending, nil
}

func (m *redisMeta) waitAcks(ctx Context, ver int64) error {
	deadline := time.Now().Add(capsuleAckTimeout)
	for {
		pending, err := m.pendingAcks(ctx, m.rdb, ver)
		if err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%d sessions did not confirm version %d: %w", pending, ver, syscall.ETIMEDOUT)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (m *redisMeta) doEvictCapsule(ctx Context, root Ino, store object.ObjectStorage, maxInodes int64) (*CapsuleInfo, error) {
	if m.sid == 0 {
		return nil, fmt.Errorf("a session is required to evict metadata")
	}
	if err := m.lockEviction(ctx); err != nil {
		return nil, err
	}
	defer m.unlockEviction(ctx)
	cur, err := m.getCapsule(ctx, m.rdb, root)
	if err != nil {
		return nil, err
	}
	if cur != nil && cur.State == CapsuleCold {
		ver, err := m.capsuleVersion(ctx, m.rdb)
		if err != nil {
			return nil, err
		}
		if err = m.waitAcks(ctx, ver); err != nil {
			return cur, err
		}
		time.Sleep(capsuleDropGrace)
		return cur, m.resumeDrop(ctx, store, cur)
	}
	var attr Attr
	if st := m.doGetAttr(ctx, root, &attr); st != 0 {
		return nil, st
	}
	if attr.Typ != TypeDirectory {
		return nil, syscall.ENOTDIR
	}
	snap, err := m.snapshotUnit(ctx, root, maxInodes)
	if err != nil {
		return nil, err
	}
	e, err := m.incrCounter("nextCapsuleEpoch", 1)
	if err != nil {
		return nil, err
	}
	epoch := uint64(e)
	var verCmd *redis.IntCmd
	err = m.capsuleTxn(ctx, root, func(info *CapsuleInfo) error {
		if info != nil {
			return fmt.Errorf("inode %d is already %s: %w", root, info.State, syscall.EEXIST)
		}
		return nil
	}, func(pipe redis.Pipeliner) {
		pipe.HSet(ctx, m.capsuleKey(root), "state", CapsuleCooling, "epoch", epoch, "owner", m.sid, "runs", encodeRuns(snap.runs))
		pipe.HSet(ctx, m.capsulesKey(), root.String(), epoch)
		verCmd = pipe.Incr(ctx, m.capsuleVerKey())
	})
	if err != nil {
		return nil, err
	}
	ver := verCmd.Val()
	obj := capsuleObject(root, epoch)
	uploaded := false
	abort := func(cause error) (*CapsuleInfo, error) {
		err := m.capsuleTxn(ctx, root, func(info *CapsuleInfo) error {
			if info == nil || info.State != CapsuleCooling || info.Epoch != epoch {
				return syscall.ESTALE
			}
			return nil
		}, func(pipe redis.Pipeliner) {
			pipe.Del(ctx, m.capsuleKey(root))
			pipe.HDel(ctx, m.capsulesKey(), root.String())
			pipe.Incr(ctx, m.capsuleVerKey())
		})
		if err == nil && uploaded {
			if err = store.Delete(ctx, obj); err != nil {
				logger.Warnf("Delete capsule %s: %s", obj, err)
			}
		}
		return nil, cause
	}
	cooling := func(info *CapsuleInfo) error {
		if info == nil || info.State != CapsuleCooling || info.Epoch != epoch {
			return fmt.Errorf("capsule of inode %d changed during eviction: %w", root, syscall.ESTALE)
		}
		return nil
	}
	for i := 0; ; i++ {
		if err = m.waitAcks(ctx, ver); err != nil {
			return abort(err)
		}
		runs := snap.runs
		if snap, err = m.snapshotUnit(ctx, root, maxInodes); err != nil {
			return abort(err)
		}
		if slices.Equal(runs, snap.runs) {
			break
		}
		if i >= 3 {
			return abort(fmt.Errorf("unit %d keeps changing: %w", root, syscall.EAGAIN))
		}
		if err = m.capsuleTxn(ctx, root, cooling, func(pipe redis.Pipeliner) {
			pipe.HSet(ctx, m.capsuleKey(root), "runs", encodeRuns(snap.runs))
			verCmd = pipe.Incr(ctx, m.capsuleVerKey())
		}); err != nil {
			return abort(err)
		}
		ver = verCmd.Val()
	}
	data, sum := encodeCapsule(root, epoch, snap.recs)
	uploaded = true
	if err = store.Put(ctx, obj, bytes.NewReader(data)); err != nil {
		return abort(fmt.Errorf("upload capsule: %w", err))
	}
	if err = m.verifyCapsule(ctx, store, obj, root, epoch, sum); err != nil {
		return abort(err)
	}
	if err = m.enableCapsules(ctx); err != nil {
		return abort(err)
	}
	info := &CapsuleInfo{Root: root, State: CapsuleCold, Epoch: epoch, Owner: m.sid, Object: obj, Sum: sum, Size: int64(len(data)),
		Records: int64(len(snap.recs)), Inodes: snap.inodes, Keys: int64(len(snap.keys)), Runs: int64(len(snap.runs)), Summary: snap.summary, runs: snap.runs}
	err = m.txn(ctx, func(tx *redis.Tx) error {
		cur, err := m.getCapsule(ctx, tx, root)
		if err != nil {
			return err
		}
		if err = cooling(cur); err != nil {
			return err
		}
		if pending, err := m.pendingAcks(ctx, tx, ver); err != nil {
			return err
		} else if pending > 0 {
			return fmt.Errorf("%d sessions did not confirm version %d: %w", pending, ver, syscall.EAGAIN)
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, m.capsuleKey(root), "state", CapsuleCold, "object", obj, "sum", sum, "size", info.Size,
				"records", info.Records, "inodes", info.Inodes, "keys", info.Keys, "runs", encodeRuns(snap.runs),
				"files", snap.summary.Files, "dirs", snap.summary.Dirs, "space", snap.summary.Size, "length", snap.summary.Length)
			verCmd = pipe.Incr(ctx, m.capsuleVerKey())
			return nil
		})
		return err
	}, m.capsuleKey(root), m.allSessions(), m.capsuleAckKey())
	if err != nil {
		return abort(err)
	}
	if err = m.waitAcks(ctx, verCmd.Val()); err != nil {
		return info, fmt.Errorf("capsule committed, hot keys are kept until all sessions confirm, run evict again: %w", err)
	}
	time.Sleep(capsuleDropGrace)
	if err = m.dropUnitKeys(ctx, root, epoch, snap); errors.Is(err, syscall.ESTALE) {
		return nil, fmt.Errorf("inode %d was used again and loaded back during eviction: %w", root, syscall.EAGAIN)
	} else if err != nil {
		return info, fmt.Errorf("capsule committed, cleanup of hot keys failed, run evict again: %w", err)
	}
	return info, nil
}

func (m *redisMeta) resumeDrop(ctx Context, store object.ObjectStorage, info *CapsuleInfo) error {
	data, err := m.readCapsule(ctx, store, info.Object)
	if err != nil {
		return err
	}
	recs, err := decodeCapsule(data, info.Root, capsuleObjectEpoch(info.Object), info.Sum)
	if err != nil {
		return err
	}
	snap := &capsuleSnapshot{fields: make(map[string][]string)}
	for _, r := range recs {
		if r.kind == capsuleRecField {
			snap.fields[m.prefix+r.key] = append(snap.fields[m.prefix+r.key], string(r.vals[0]))
		} else {
			snap.keys = append(snap.keys, m.prefix+r.key)
		}
	}
	return m.dropUnitKeys(ctx, info.Root, info.Epoch, snap)
}

func capsuleObjectEpoch(obj string) uint64 {
	v, _ := strconv.ParseUint(obj[strings.LastIndex(obj, "/")+1:], 10, 64)
	return v
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
		}, func(pipe redis.Pipeliner) {
			pipe.Del(ctx, m.capsuleKey(root))
			pipe.HDel(ctx, m.capsulesKey(), root.String())
			pipe.Incr(ctx, m.capsuleVerKey())
		})
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
	for _, batch := range capsuleRestoreBatches(recs, capsuleBatchValues) {
		err = m.capsuleTxn(ctx, root, check, func(pipe redis.Pipeliner) {
			for _, op := range batch {
				key := m.prefix + op.rec.key
				args := make([]interface{}, len(op.vals))
				for j, v := range op.vals {
					args[j] = v
				}
				switch op.rec.kind {
				case capsuleRecString:
					pipe.Set(ctx, key, op.vals[0], 0)
				case capsuleRecHash:
					if op.first {
						pipe.Del(ctx, key)
					}
					pipe.HSet(ctx, key, args...)
				case capsuleRecList:
					if op.first {
						pipe.Del(ctx, key)
					}
					pipe.RPush(ctx, key, args...)
				case capsuleRecField:
					pipe.HSet(ctx, key, op.vals[0], op.vals[1])
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

func (m *redisMeta) enableCapsules(ctx Context) error {
	return m.txn(ctx, func(tx *redis.Tx) error {
		body, err := tx.Get(ctx, m.setting()).Bytes()
		if err != nil {
			return err
		}
		var f map[string]interface{}
		if err = json.Unmarshal(body, &f); err != nil {
			return err
		}
		if v, _ := f["MetaVersion"].(float64); int(v) >= capsuleMetaVersion {
			return nil
		}
		f["MetaVersion"] = capsuleMetaVersion
		if body, err = json.MarshalIndent(f, "", "\t"); err != nil {
			return err
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, m.setting(), body, 0)
			return nil
		})
		return err
	}, m.setting())
}

func (m *redisMeta) doLoadCapsuleSet(ctx Context, ver int64) (*capsuleSet, error) {
	var verCmd *redis.StringCmd
	var rootsCmd *redis.StringSliceCmd
	_, err := m.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		verCmd = pipe.Get(ctx, m.capsuleVerKey())
		rootsCmd = pipe.HKeys(ctx, m.capsulesKey())
		return nil
	})
	if err != nil && err != redis.Nil {
		return nil, err
	}
	cur, err := verCmd.Int64()
	if err == redis.Nil {
		cur = 0
	} else if err != nil {
		return nil, err
	}
	if cur == ver {
		return nil, nil
	}
	roots := rootsCmd.Val()
	infos := make([]*CapsuleInfo, 0, len(roots))
	for i := 0; i < len(roots); i += capsuleBatch {
		batch := roots[i:min(i+capsuleBatch, len(roots))]
		cmds, err := m.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, r := range batch {
				v, _ := strconv.ParseUint(r, 10, 64)
				pipe.HGetAll(ctx, m.capsuleKey(Ino(v)))
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		for j, c := range cmds {
			v, _ := strconv.ParseUint(batch[j], 10, 64)
			if info := parseCapsuleInfo(Ino(v), c.(*redis.MapStringStringCmd).Val()); info != nil {
				infos = append(infos, info)
			}
		}
	}
	return newCapsuleSet(cur, infos), nil
}

func (m *redisMeta) doScanCapsuleSlices(ctx Context, store object.ObjectStorage, info *CapsuleInfo, fn func(Ino, Slice) error) error {
	data, err := m.readCapsule(ctx, store, info.Object)
	if err != nil {
		return err
	}
	var objEpoch uint64
	if _, err = fmt.Sscanf(info.Object[strings.LastIndex(info.Object, "/")+1:], "%d", &objEpoch); err != nil {
		return fmt.Errorf("bad capsule object name %q", info.Object)
	}
	recs, err := decodeCapsule(data, info.Root, objEpoch, info.Sum)
	if err != nil {
		return err
	}
	for _, r := range recs {
		if r.kind != capsuleRecList || !strings.HasPrefix(r.key, "c") {
			continue
		}
		inode, _ := strconv.ParseUint(strings.Split(r.key[1:], "_")[0], 10, 64)
		vals := make([]string, len(r.vals))
		for i, v := range r.vals {
			vals[i] = string(v)
		}
		ss := readSlices(vals)
		if ss == nil {
			return fmt.Errorf("corrupt chunk %s in capsule", r.key)
		}
		for _, s := range ss {
			if s.id > 0 {
				if err = fn(Ino(inode), Slice{Id: s.id, Size: s.size}); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
