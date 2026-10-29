package meta

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

const capsuleMagic = "JFSCAP1\n"

const capsuleMetaVersion = 2

const (
	capsuleRecEnd uint8 = iota
	capsuleRecString
	capsuleRecHash
	capsuleRecList
	capsuleRecField
)

const (
	CapsuleCooling   = "cooling"
	CapsuleCold      = "cold"
	CapsuleHydrating = "hydrating"
)

type CapsuleInfo struct {
	Root    Ino
	State   string
	Epoch   uint64
	Owner   uint64
	Object  string
	Sum     string
	Size    int64
	Records int64
	Inodes  int64
	Keys    int64
	Runs    int64
	Summary Summary
	runs    []inoRun
}

type inoRun struct {
	start, end Ino
}

type capsuleRun struct {
	inoRun
	root Ino
}

type capsuleSet struct {
	ver   int64
	roots map[Ino]*CapsuleInfo
	runs  []capsuleRun
}

func newCapsuleSet(ver int64, infos []*CapsuleInfo) *capsuleSet {
	s := &capsuleSet{ver: ver, roots: make(map[Ino]*CapsuleInfo, len(infos))}
	for _, info := range infos {
		s.roots[info.Root] = info
		for _, r := range info.runs {
			s.runs = append(s.runs, capsuleRun{r, info.Root})
		}
	}
	sort.Slice(s.runs, func(i, j int) bool { return s.runs[i].start < s.runs[j].start })
	return s
}

func (s *capsuleSet) unitOf(ino Ino) (Ino, bool) {
	if _, ok := s.roots[ino]; ok {
		return ino, true
	}
	i := sort.Search(len(s.runs), func(i int) bool { return s.runs[i].end >= ino })
	if i < len(s.runs) && s.runs[i].start <= ino {
		return s.runs[i].root, true
	}
	return 0, false
}

func inoRuns(inodes []Ino) []inoRun {
	sorted := append([]Ino(nil), inodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var runs []inoRun
	for _, ino := range sorted {
		if n := len(runs); n > 0 && runs[n-1].end+1 >= ino {
			runs[n-1].end = max(runs[n-1].end, ino)
			continue
		}
		runs = append(runs, inoRun{ino, ino})
	}
	return runs
}

func encodeRuns(runs []inoRun) []byte {
	buf := make([]byte, 16*len(runs))
	for i, r := range runs {
		binary.BigEndian.PutUint64(buf[16*i:], uint64(r.start))
		binary.BigEndian.PutUint64(buf[16*i+8:], uint64(r.end))
	}
	return buf
}

func decodeRuns(buf []byte) []inoRun {
	runs := make([]inoRun, len(buf)/16)
	for i := range runs {
		runs[i] = inoRun{Ino(binary.BigEndian.Uint64(buf[16*i:])), Ino(binary.BigEndian.Uint64(buf[16*i+8:]))}
	}
	return runs
}

type capsuleRecord struct {
	kind uint8
	key  string
	vals [][]byte
}

type capsuleEngine interface {
	doEvictCapsule(ctx Context, root Ino, store object.ObjectStorage, maxInodes int64) (*CapsuleInfo, error)
	doHydrateCapsule(ctx Context, root Ino, store object.ObjectStorage, force bool) (*CapsuleInfo, error)
	doListCapsules(ctx Context) ([]*CapsuleInfo, error)
	doLoadCapsuleSet(ctx Context, ver int64) (*capsuleSet, error)
	doScanCapsuleSlices(ctx Context, store object.ObjectStorage, info *CapsuleInfo, fn func(Ino, Slice) error) error
}

func (m *baseMeta) capsuleEngine() (capsuleEngine, error) {
	if c, ok := m.en.(capsuleEngine); ok {
		return c, nil
	}
	return nil, fmt.Errorf("metadata capsules are not supported by this engine: %w", syscall.ENOTSUP)
}

func (m *baseMeta) SetCapsuleStorage(store object.ObjectStorage) {
	m.capsuleStore = store
}

func (m *baseMeta) refreshCapsules(ctx Context) {
	c, ok := m.en.(capsuleEngine)
	if !ok {
		return
	}
	ver := int64(-1)
	if cur := m.capsules.Load(); cur != nil {
		ver = cur.ver
	}
	set, err := c.doLoadCapsuleSet(ctx, ver)
	if err != nil {
		logger.Warnf("Load capsules: %s", err)
		return
	}
	if set != nil {
		m.capsules.Store(set)
	}
}

func (m *baseMeta) hasCapsules() bool {
	s := m.capsules.Load()
	return s != nil && len(s.roots) > 0
}

func (m *baseMeta) checkNoCapsules(ctx Context) error {
	c, ok := m.en.(capsuleEngine)
	if !ok {
		return nil
	}
	infos, err := c.doListCapsules(ctx)
	if err != nil {
		return err
	}
	if len(infos) > 0 {
		return fmt.Errorf("%d directories are evicted, load them with `juicefs metadata prefetch` first: %w", len(infos), syscall.ENOTSUP)
	}
	return nil
}

func (m *baseMeta) coldSummary(ino Ino) (*Summary, bool) {
	s := m.capsules.Load()
	if s == nil {
		return nil, false
	}
	if info := s.roots[ino]; info != nil {
		return &info.Summary, true
	}
	return nil, false
}

func (m *baseMeta) ensureHot(ctx Context, ino Ino, entries bool) syscall.Errno {
	s := m.capsules.Load()
	if s == nil || len(s.roots) == 0 {
		return 0
	}
	root, ok := s.unitOf(ino)
	if !ok || (root == ino && !entries) {
		return 0
	}
	return m.hydrateUnit(ctx, root)
}

func (m *baseMeta) ensureHotChild(ctx Context, parent Ino, name string) syscall.Errno {
	if st := m.ensureHot(ctx, parent, true); st != 0 {
		return st
	}
	if !m.hasCapsules() {
		return 0
	}
	var ino Ino
	if st := m.en.doLookup(ctx, parent, name, &ino, nil); st == 0 {
		return m.ensureHot(ctx, ino, true)
	}
	return 0
}

func (m *baseMeta) hydrateUnit(ctx Context, root Ino) syscall.Errno {
	c, err := m.capsuleEngine()
	if err != nil {
		return syscall.ENOTSUP
	}
	if m.capsuleStore == nil {
		logger.Errorf("Inode %d is evicted and this client has no object storage to load it", root)
		return syscall.EIO
	}
	m.capsuleMu.Lock()
	defer m.capsuleMu.Unlock()
	deadline := time.Now().Add(time.Minute)
	for {
		start := time.Now()
		info, err := c.doHydrateCapsule(ctx, root, m.capsuleStore, false)
		switch {
		case err == nil:
			logger.Infof("Loaded metadata of inode %d (%d inodes) in %s", root, info.Inodes, time.Since(start))
			m.refreshCapsules(ctx)
			return 0
		case errors.Is(err, syscall.ENOENT):
			m.refreshCapsules(ctx)
			return 0
		case errors.Is(err, syscall.EAGAIN) && time.Now().Before(deadline):
			time.Sleep(100 * time.Millisecond)
		default:
			logger.Errorf("Load metadata of inode %d: %s", root, err)
			var eno syscall.Errno
			if errors.As(err, &eno) && eno != syscall.ESTALE {
				return eno
			}
			return syscall.EIO
		}
	}
}

func (m *baseMeta) readdirHot(ctx Context, inode Ino, plus uint8, entries *[]*Entry, limit int) syscall.Errno {
	if st := m.ensureHot(ctx, inode, true); st != 0 {
		return st
	}
	return m.en.doReaddir(ctx, inode, plus, entries, limit)
}

func (m *baseMeta) scanCapsuleSlices(ctx Context, fn func(Ino, Slice) error) error {
	c, ok := m.en.(capsuleEngine)
	if !ok {
		return nil
	}
	infos, err := c.doListCapsules(ctx)
	if err != nil || len(infos) == 0 {
		return err
	}
	if m.capsuleStore == nil {
		return fmt.Errorf("%d directories are evicted and no object storage is set to read their slices", len(infos))
	}
	for _, info := range infos {
		if err = c.doScanCapsuleSlices(ctx, m.capsuleStore, info, fn); err != nil {
			return fmt.Errorf("scan capsule of inode %d: %w", info.Root, err)
		}
	}
	return nil
}

func (m *baseMeta) EvictMetadata(ctx Context, root Ino, store object.ObjectStorage, maxInodes int64) (*CapsuleInfo, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	if root <= RootInode || root.IsTrash() {
		return nil, fmt.Errorf("cannot evict inode %d: %w", root, syscall.EINVAL)
	}
	return c.doEvictCapsule(ctx, root, store, maxInodes)
}

func (m *baseMeta) HydrateMetadata(ctx Context, root Ino, store object.ObjectStorage, force bool) (*CapsuleInfo, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	return c.doHydrateCapsule(ctx, root, store, force)
}

func (m *baseMeta) ListCapsules(ctx Context) ([]*CapsuleInfo, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	return c.doListCapsules(ctx)
}

func capsuleObject(root Ino, epoch uint64) string {
	return fmt.Sprintf("meta/capsules/%d/%d", root, epoch)
}

func encodeCapsule(root Ino, epoch uint64, recs []capsuleRecord) ([]byte, string) {
	var buf bytes.Buffer
	buf.WriteString(capsuleMagic)
	_ = binary.Write(&buf, binary.BigEndian, uint64(root))
	_ = binary.Write(&buf, binary.BigEndian, epoch)
	put := func(b []byte) {
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(b)))
		buf.Write(b)
	}
	for _, r := range recs {
		buf.WriteByte(r.kind)
		put([]byte(r.key))
		_ = binary.Write(&buf, binary.BigEndian, uint32(len(r.vals)))
		for _, v := range r.vals {
			put(v)
		}
	}
	buf.WriteByte(capsuleRecEnd)
	_ = binary.Write(&buf, binary.BigEndian, uint64(len(recs)))
	sum := sha256.Sum256(buf.Bytes())
	buf.Write(sum[:])
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

func decodeCapsule(data []byte, root Ino, epoch uint64, want string) ([]capsuleRecord, error) {
	head := len(capsuleMagic) + 16
	if len(data) < head+1+8+sha256.Size {
		return nil, fmt.Errorf("capsule too short: %d bytes", len(data))
	}
	body := data[:len(data)-sha256.Size]
	sum := sha256.Sum256(body)
	if got := hex.EncodeToString(sum[:]); got != hex.EncodeToString(data[len(body):]) || (want != "" && got != want) {
		return nil, fmt.Errorf("capsule checksum mismatch")
	}
	if string(body[:len(capsuleMagic)]) != capsuleMagic {
		return nil, fmt.Errorf("bad capsule magic")
	}
	r := bytes.NewReader(body[len(capsuleMagic):])
	var ino, ep uint64
	_ = binary.Read(r, binary.BigEndian, &ino)
	_ = binary.Read(r, binary.BigEndian, &ep)
	if Ino(ino) != root || ep != epoch {
		return nil, fmt.Errorf("capsule is for inode %d epoch %d, want %d epoch %d", ino, ep, root, epoch)
	}
	get := func() ([]byte, error) {
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		if int(n) > r.Len() {
			return nil, fmt.Errorf("truncated capsule")
		}
		b := make([]byte, n)
		if n == 0 {
			return b, nil
		}
		_, err := r.Read(b)
		return b, err
	}
	var recs []capsuleRecord
	for {
		kind, err := r.ReadByte()
		if err != nil {
			return nil, fmt.Errorf("truncated capsule: %w", err)
		}
		if kind == capsuleRecEnd {
			break
		}
		if kind > capsuleRecField {
			return nil, fmt.Errorf("unknown capsule record %d", kind)
		}
		key, err := get()
		if err != nil {
			return nil, err
		}
		var n uint32
		if err = binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		rec := capsuleRecord{kind: kind, key: string(key), vals: make([][]byte, n)}
		for i := range rec.vals {
			if rec.vals[i], err = get(); err != nil {
				return nil, err
			}
		}
		recs = append(recs, rec)
	}
	var count uint64
	if err := binary.Read(r, binary.BigEndian, &count); err != nil || count != uint64(len(recs)) || r.Len() != 0 {
		return nil, fmt.Errorf("capsule record count mismatch: %d != %d", count, len(recs))
	}
	return recs, nil
}
