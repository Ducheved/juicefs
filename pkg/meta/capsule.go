package meta

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"syscall"

	"github.com/juicedata/juicefs/pkg/object"
)

const capsuleMagic = "JFSCAP1\n"

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
}

func (m *baseMeta) capsuleEngine() (capsuleEngine, error) {
	if c, ok := m.en.(capsuleEngine); ok {
		return c, nil
	}
	return nil, fmt.Errorf("metadata capsules are not supported by this engine: %w", syscall.ENOTSUP)
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
