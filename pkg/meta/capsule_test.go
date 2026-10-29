package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/redis/go-redis/v9"
)

func capsuleTestMeta(t *testing.T) (*redisMeta, object.ObjectStorage, string) {
	addr := os.Getenv("CAPSULE_REDIS")
	if addr == "" {
		addr = "127.0.0.1:6379/11"
	}
	m, err := newRedisMeta("redis", addr, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if err = m.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err = m.Init(&Format{Name: "capsule", DirStats: true}, false); err != nil {
		t.Fatalf("init: %s", err)
	}
	if err = m.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}
	t.Cleanup(func() { _ = m.CloseSession() })
	store, err := object.CreateStorage("file", t.TempDir()+"/", "", "", "")
	if err != nil {
		t.Fatalf("create storage: %s", err)
	}
	return m.(*redisMeta), store, addr
}

func capsuleRawDump(t *testing.T, m *redisMeta) map[string]string {
	ctx := context.Background()
	dump := make(map[string]string)
	skip := func(k string) bool {
		for _, p := range []string{"allSessions", "sessionInfos", "session", "nextCapsuleEpoch", "capsule", "lastCleanup", "nextCleanup", "nextsession"} {
			if strings.HasPrefix(k, p) {
				return true
			}
		}
		return false
	}
	err := m.scan(ctx, "*", func(keys []string) error {
		for _, key := range keys {
			k := m.relKey(key)
			if skip(k) {
				continue
			}
			typ, err := m.rdb.Type(ctx, key).Result()
			if err != nil {
				return err
			}
			var v string
			switch typ {
			case "string":
				v, err = m.rdb.Get(ctx, key).Result()
			case "hash":
				var h map[string]string
				h, err = m.rdb.HGetAll(ctx, key).Result()
				var fs []string
				for f, x := range h {
					fs = append(fs, f+"="+x)
				}
				sort.Strings(fs)
				v = strings.Join(fs, "\x00")
			case "list":
				var l []string
				l, err = m.rdb.LRange(ctx, key, 0, -1).Result()
				v = strings.Join(l, "\x00")
			case "set":
				var l []string
				l, err = m.rdb.SMembers(ctx, key).Result()
				sort.Strings(l)
				v = strings.Join(l, "\x00")
			case "zset":
				var l []redis.Z
				l, err = m.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
				v = fmt.Sprint(l)
			}
			if err != nil {
				return err
			}
			dump[k] = typ + ":" + v
		}
		return nil
	})
	if err != nil {
		t.Fatalf("dump: %s", err)
	}
	return dump
}

func capsuleManifest(t *testing.T, m *redisMeta, root Ino) []string {
	ctx := Background()
	var out []string
	var walk func(dir Ino, path string)
	walk = func(dir Ino, path string) {
		var entries []*Entry
		if st := m.Readdir(ctx, dir, 1, &entries); st != 0 {
			t.Fatalf("readdir %s: %s", path, st)
		}
		for _, e := range entries {
			name := string(e.Name)
			if name == "." || name == ".." {
				continue
			}
			a := e.Attr
			line := fmt.Sprintf("%s/%s ino=%d typ=%d mode=%o uid=%d gid=%d nlink=%d len=%d", path, name, e.Inode, a.Typ, a.Mode, a.Uid, a.Gid, a.Nlink, a.Length)
			switch a.Typ {
			case TypeFile:
				for indx := uint32(0); uint64(indx)*ChunkSize < a.Length; indx++ {
					var ss []Slice
					if st := m.Read(ctx, e.Inode, indx, &ss); st != 0 {
						t.Fatalf("read %s: %s", name, st)
					}
					line += fmt.Sprintf(" c%d=%v", indx, ss)
				}
			case TypeSymlink:
				var target []byte
				if st := m.ReadLink(ctx, e.Inode, &target); st != 0 {
					t.Fatalf("readlink %s: %s", name, st)
				}
				line += " ->" + string(target)
			}
			var xs []byte
			if st := m.ListXattr(ctx, e.Inode, &xs); st == 0 && len(xs) > 0 {
				line += fmt.Sprintf(" xattr=%q", xs)
			}
			out = append(out, line)
			if a.Typ == TypeDirectory {
				walk(e.Inode, path+"/"+name)
			}
		}
	}
	walk(root, "")
	sort.Strings(out)
	return out
}

func capsuleTree(t *testing.T, m *redisMeta) (Ino, Ino) {
	ctx := Background()
	mkdir := func(parent Ino, name string) Ino {
		var ino Ino
		var attr Attr
		if st := m.Mkdir(ctx, parent, name, 0755, 0, 0, &ino, &attr); st != 0 {
			t.Fatalf("mkdir %s: %s", name, st)
		}
		return ino
	}
	file := func(parent Ino, name string, chunks int) Ino {
		var ino Ino
		var attr Attr
		if st := m.Create(ctx, parent, name, 0640, 0, 0, &ino, &attr); st != 0 {
			t.Fatalf("create %s: %s", name, st)
		}
		for c := 0; c < chunks; c++ {
			var id uint64
			if st := m.NewSlice(ctx, &id); st != 0 {
				t.Fatalf("new slice: %s", st)
			}
			size := uint32(1 << 20)
			if st := m.Write(ctx, ino, uint32(c), 0, Slice{Id: id, Size: size, Len: size}, time.Now()); st != 0 {
				t.Fatalf("write %s: %s", name, st)
			}
		}
		m.Close(ctx, ino)
		return ino
	}
	proj := mkdir(RootInode, "proj")
	other := mkdir(RootInode, "other")
	file(other, "keep", 1)
	a := mkdir(proj, "a")
	b := mkdir(a, "b")
	mkdir(proj, "empty")
	for i := 0; i < 20; i++ {
		file(a, fmt.Sprintf("f%02d", i), 1)
	}
	big := file(b, "big", 3)
	file(b, "zero", 0)
	var attr Attr
	if st := m.Link(ctx, big, a, "big-link", &attr); st != 0 {
		t.Fatalf("link: %s", st)
	}
	var sino Ino
	if st := m.Symlink(ctx, b, "sym", "../f00", &sino, &attr); st != 0 {
		t.Fatalf("symlink: %s", st)
	}
	if st := m.SetXattr(ctx, big, "user.k", []byte("v"), 0); st != 0 {
		t.Fatalf("setxattr: %s", st)
	}
	if err := m.HandleQuota(ctx, QuotaSet, "/proj", DirQuotaType, map[string]*Quota{"/proj": {MaxSpace: 1 << 40, MaxInodes: 1 << 20}}, false, false, false); err != nil {
		t.Fatalf("set quota: %s", err)
	}
	m.FlushSession()
	return proj, other
}

func TestRedisCapsuleEvictHydrate(t *testing.T) {
	m, store, _ := capsuleTestMeta(t)
	ctx := Background()
	proj, _ := capsuleTree(t, m)
	before := capsuleRawDump(t, m)
	manifest := capsuleManifest(t, m, proj)
	keysBefore, _ := m.rdb.DBSize(ctx).Result()

	info, err := m.EvictMetadata(ctx, proj, store, 0)
	if err != nil {
		t.Fatalf("evict: %s", err)
	}
	if info.State != CapsuleCold || info.Inodes != 27 {
		t.Fatalf("evict info: %+v", info)
	}
	keysCold, _ := m.rdb.DBSize(ctx).Result()
	if keysBefore-keysCold != info.Keys-4 {
		t.Fatalf("keys before %d, cold %d, removed %d", keysBefore, keysCold, info.Keys)
	}
	cold := capsuleRawDump(t, m)
	for k := range cold {
		if _, ok := before[k]; !ok {
			t.Fatalf("new key while cold: %s", k)
		}
	}
	for _, k := range []string{"i" + proj.String(), "d1"} {
		if cold[k] != before[k] {
			t.Fatalf("%s changed while cold", k)
		}
	}
	var entries []*Entry
	if st := m.Readdir(ctx, proj, 0, &entries); st != 0 || len(entries) != 2 {
		t.Fatalf("cold root readdir: %s %d", st, len(entries))
	}
	if infos, err := m.ListCapsules(ctx); err != nil || len(infos) != 1 || infos[0].Root != proj {
		t.Fatalf("list capsules: %v %v", infos, err)
	}

	if _, err = m.HydrateMetadata(ctx, proj, store, false); err != nil {
		t.Fatalf("hydrate: %s", err)
	}
	after := capsuleRawDump(t, m)
	if len(after) != len(before) {
		t.Fatalf("keys after hydrate %d, before %d", len(after), len(before))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("key %s differs after hydrate", k)
		}
	}
	if got := capsuleManifest(t, m, proj); strings.Join(got, "\n") != strings.Join(manifest, "\n") {
		t.Fatalf("manifest differs:\n%s\n---\n%s", strings.Join(manifest, "\n"), strings.Join(got, "\n"))
	}
	if infos, _ := m.ListCapsules(ctx); len(infos) != 0 {
		t.Fatalf("capsules left: %v", infos)
	}
	if _, err = store.Head(context.Background(), info.Object); err == nil {
		t.Fatalf("capsule object %s not deleted", info.Object)
	}
}

func TestRedisCapsuleRejects(t *testing.T) {
	m, store, addr := capsuleTestMeta(t)
	ctx := Background()
	proj, other := capsuleTree(t, m)

	var a, ino Ino
	var attr Attr
	if st := m.Lookup(ctx, proj, "a", &a, &attr, false); st != 0 {
		t.Fatalf("lookup: %s", st)
	}
	if err := m.HandleQuota(ctx, QuotaSet, "/proj/a", DirQuotaType, map[string]*Quota{"/proj/a": {MaxInodes: 100}}, false, false, false); err != nil {
		t.Fatalf("set quota: %s", err)
	}
	if _, err := m.EvictMetadata(ctx, proj, store, 0); !errors.Is(err, syscall.EPERM) {
		t.Fatalf("quota inside unit: %v", err)
	}
	if err := m.HandleQuota(ctx, QuotaDel, "/proj/a", DirQuotaType, nil, false, false, false); err != nil {
		t.Fatalf("del quota: %s", err)
	}
	if _, err := m.EvictMetadata(ctx, proj, store, 10); !errors.Is(err, syscall.EFBIG) {
		t.Fatalf("too large unit: %v", err)
	}
	if st := m.Lookup(ctx, a, "f00", &ino, &attr, false); st != 0 {
		t.Fatalf("lookup: %s", st)
	}
	if st := m.Link(ctx, ino, other, "outside", &attr); st != 0 {
		t.Fatalf("link: %s", st)
	}
	if _, err := m.EvictMetadata(ctx, proj, store, 0); !errors.Is(err, syscall.EXDEV) {
		t.Fatalf("link outside unit: %v", err)
	}
	if st := m.Unlink(ctx, other, "outside"); st != 0 {
		t.Fatalf("unlink: %s", st)
	}
	if infos, _ := m.ListCapsules(ctx); len(infos) != 0 {
		t.Fatalf("aborted evictions left capsules: %v", infos)
	}

	m2, err := newRedisMeta("redis", addr, testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	if _, err = m2.Load(true); err != nil {
		t.Fatalf("load: %s", err)
	}
	if err = m2.NewSession(true); err != nil {
		t.Fatalf("new session: %s", err)
	}
	if _, err = m.EvictMetadata(ctx, proj, store, 0); !errors.Is(err, syscall.EBUSY) {
		t.Fatalf("other session alive: %v", err)
	}
	_ = m2.CloseSession()

	info, err := m.EvictMetadata(ctx, proj, store, 0)
	if err != nil {
		t.Fatalf("evict: %s", err)
	}
	if _, err = m.EvictMetadata(ctx, proj, store, 0); !errors.Is(err, syscall.EEXIST) {
		t.Fatalf("evict twice: %v", err)
	}
	if err = m.dropUnitKeys(ctx, proj, info.Epoch-1, &capsuleSnapshot{keys: []string{m.inodeKey(other)}}); !errors.Is(err, syscall.ESTALE) {
		t.Fatalf("stale epoch: %v", err)
	}
	var oattr Attr
	if st := m.GetAttr(ctx, other, &oattr); st != 0 {
		t.Fatalf("stale worker removed a key: %s", st)
	}

	if err = store.Put(context.Background(), info.Object, strings.NewReader("garbage")); err != nil {
		t.Fatalf("put: %s", err)
	}
	if _, err = m.HydrateMetadata(ctx, proj, store, false); !errors.Is(err, syscall.EIO) {
		t.Fatalf("corrupt capsule: %v", err)
	}
	if infos, _ := m.ListCapsules(ctx); len(infos) != 1 || infos[0].State != CapsuleHydrating {
		t.Fatalf("corrupt capsule state: %v", infos)
	}
}
