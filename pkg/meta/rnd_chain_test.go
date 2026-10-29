package meta

import (
	"os"
	"syscall"
	"testing"
	"time"
)

func rndMetaURL() string {
	if u := os.Getenv("RND_META"); u != "" {
		return u
	}
	return "redis://127.0.0.1:6379/12"
}

func rndTwoClients(t *testing.T) (Meta, Meta) {
	conf := testConfig()
	conf.Heartbeat = time.Second
	m1 := NewClient(rndMetaURL(), conf)
	if err := m1.Reset(); err != nil {
		t.Fatalf("reset: %s", err)
	}
	if err := m1.Init(testFormat(), false); err != nil {
		t.Fatalf("init: %s", err)
	}
	conf2 := testConfig()
	conf2.Heartbeat = time.Second
	m2 := NewClient(rndMetaURL(), conf2)
	if _, err := m2.Load(true); err != nil {
		t.Fatalf("load: %s", err)
	}
	for _, m := range []Meta{m1, m2} {
		if err := m.NewSession(true); err != nil {
			t.Fatalf("new session: %s", err)
		}
	}
	t.Cleanup(func() {
		_ = m1.CloseSession()
		_ = m2.CloseSession()
	})
	return m1, m2
}

func rndSetQuota(t *testing.T, m Meta, p string, maxInodes int64) {
	if err := m.HandleQuota(Background(), QuotaSet, p, DirQuotaType, map[string]*Quota{p: {MaxSpace: 1 << 40, MaxInodes: maxInodes}}, false, false, false); err != nil {
		t.Fatalf("set quota %s: %s", p, err)
	}
}

func rndUsage(t *testing.T, m Meta, p string) *Quota {
	qs := make(map[string]*Quota)
	if err := m.HandleQuota(Background(), QuotaGet, p, DirQuotaType, qs, false, false, false); err != nil {
		t.Fatalf("get quota %s: %s", p, err)
	}
	if qs[p] == nil {
		t.Fatalf("no quota %s", p)
	}
	return qs[p]
}

func rndCreate(t *testing.T, m Meta, parent Ino, name string) syscall.Errno {
	var ino Ino
	var attr Attr
	st := m.Create(Background(), parent, name, 0644, 0, 0, &ino, &attr)
	if st == 0 {
		m.Close(Background(), ino)
	}
	return st
}

func rndMkdir(t *testing.T, m Meta, parent Ino, name string) Ino {
	var ino Ino
	var attr Attr
	if st := m.Mkdir(Background(), parent, name, 0755, 0, 0, &ino, &attr); st != 0 {
		t.Fatalf("mkdir %s: %s", name, st)
	}
	return ino
}

func rndFlush(m Meta) {
	m.getBase().doFlushDirStat()
	m.getBase().doFlushQuotas()
}

func rndReload(ms ...Meta) {
	for _, m := range ms {
		m.getBase().loadQuotas()
	}
}

func TestRndForeignRenameDir(t *testing.T) {
	m1, m2 := rndTwoClients(t)
	ctx := Background()
	a := rndMkdir(t, m1, RootInode, "a")
	b := rndMkdir(t, m1, RootInode, "b")
	rndSetQuota(t, m1, "/a", 1<<30)
	rndSetQuota(t, m1, "/b", 1<<30)
	rndReload(m1, m2)
	x := rndMkdir(t, m1, a, "x")
	if st := rndCreate(t, m1, x, "f1"); st != 0 {
		t.Fatalf("create f1: %s", st)
	}
	rndFlush(m1)
	var ino Ino
	var attr Attr
	if st := m2.Rename(ctx, a, "x", b, "x", 0, &ino, &attr); st != 0 {
		t.Fatalf("rename: %s", st)
	}
	rndFlush(m2)
	qa, qb := rndUsage(t, m1, "/a"), rndUsage(t, m1, "/b")
	t.Logf("after foreign rename: /a inodes=%d /b inodes=%d", qa.UsedInodes, qb.UsedInodes)
	if st := rndCreate(t, m1, x, "f2"); st != 0 {
		t.Fatalf("create f2: %s", st)
	}
	rndFlush(m1)
	qa, qb = rndUsage(t, m1, "/a"), rndUsage(t, m1, "/b")
	t.Logf("create via stale client: /a inodes=%d /b inodes=%d", qa.UsedInodes, qb.UsedInodes)
	time.Sleep(3 * time.Second)
	if st := rndCreate(t, m1, x, "f3"); st != 0 {
		t.Fatalf("create f3: %s", st)
	}
	rndFlush(m1)
	qa, qb = rndUsage(t, m1, "/a"), rndUsage(t, m1, "/b")
	t.Logf("after 3 heartbeats: /a inodes=%d /b inodes=%d", qa.UsedInodes, qb.UsedInodes)
	if qa.UsedInodes != 0 || qb.UsedInodes != 4 {
		t.Fatalf("want /a=0 /b=4, got /a=%d /b=%d", qa.UsedInodes, qb.UsedInodes)
	}
}

func TestRndForeignRenameAncestor(t *testing.T) {
	m1, m2 := rndTwoClients(t)
	ctx := Background()
	a := rndMkdir(t, m1, RootInode, "a")
	b := rndMkdir(t, m1, RootInode, "b")
	rndSetQuota(t, m1, "/a", 1<<30)
	rndSetQuota(t, m1, "/b", 1<<30)
	rndReload(m1, m2)
	x := rndMkdir(t, m1, a, "x")
	y := rndMkdir(t, m1, x, "y")
	if st := rndCreate(t, m1, y, "f1"); st != 0 {
		t.Fatalf("create f1: %s", st)
	}
	rndFlush(m1)
	var ino Ino
	var attr Attr
	if st := m2.Rename(ctx, a, "x", b, "x", 0, &ino, &attr); st != 0 {
		t.Fatalf("rename: %s", st)
	}
	rndFlush(m2)
	time.Sleep(3 * time.Second)
	if st := rndCreate(t, m1, y, "f2"); st != 0 {
		t.Fatalf("create f2: %s", st)
	}
	rndFlush(m1)
	qa, qb := rndUsage(t, m1, "/a"), rndUsage(t, m1, "/b")
	t.Logf("create under moved ancestor: /a inodes=%d /b inodes=%d", qa.UsedInodes, qb.UsedInodes)
	if qa.UsedInodes != 0 || qb.UsedInodes != 4 {
		t.Fatalf("want /a=0 /b=4, got /a=%d /b=%d", qa.UsedInodes, qb.UsedInodes)
	}
}

func TestRndForeignRenameBypassesLimit(t *testing.T) {
	m1, m2 := rndTwoClients(t)
	ctx := Background()
	a := rndMkdir(t, m1, RootInode, "a")
	b := rndMkdir(t, m1, RootInode, "b")
	rndSetQuota(t, m1, "/a", 1<<30)
	rndSetQuota(t, m1, "/b", 2)
	rndReload(m1, m2)
	x := rndMkdir(t, m1, a, "x")
	if st := rndCreate(t, m1, x, "f1"); st != 0 {
		t.Fatalf("create f1: %s", st)
	}
	rndFlush(m1)
	var ino Ino
	var attr Attr
	if st := m2.Rename(ctx, a, "x", b, "x", 0, &ino, &attr); st != 0 {
		t.Fatalf("rename: %s", st)
	}
	rndFlush(m2)
	time.Sleep(3 * time.Second)
	if st := rndCreate(t, m2, x, "g"); st != syscall.EDQUOT {
		t.Fatalf("fresh client: want EDQUOT, got %v", st)
	}
	st := rndCreate(t, m1, x, "f2")
	rndFlush(m1)
	qb := rndUsage(t, m1, "/b")
	t.Logf("stale client create in full /b: errno=%v /b inodes=%d max=%d", st, qb.UsedInodes, qb.MaxInodes)
	if st != syscall.EDQUOT {
		t.Fatalf("stale client: want EDQUOT, got %v", st)
	}
}

func TestRndForeignHardlink(t *testing.T) {
	m1, m2 := rndTwoClients(t)
	ctx := Background()
	a := rndMkdir(t, m1, RootInode, "a")
	b := rndMkdir(t, m1, RootInode, "b")
	rndSetQuota(t, m1, "/a", 1<<30)
	rndSetQuota(t, m1, "/b", 1<<30)
	rndReload(m1, m2)
	var f Ino
	var attr Attr
	if st := m1.Create(ctx, a, "f", 0644, 0, 0, &f, &attr); st != 0 {
		t.Fatalf("create: %s", st)
	}
	write := func(m Meta, off uint32) {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatalf("new slice: %s", st)
		}
		if st := m.Write(ctx, f, 0, off, Slice{Id: id, Size: 4096, Len: 4096}, time.Now()); st != 0 {
			t.Fatalf("write: %s", st)
		}
	}
	write(m1, 0)
	rndFlush(m1)
	if st := m2.Link(ctx, f, b, "f2", &attr); st != 0 {
		t.Fatalf("link: %s", st)
	}
	rndFlush(m2)
	write(m1, 4096)
	time.Sleep(500 * time.Millisecond)
	rndFlush(m1)
	m1.Close(ctx, f)
	qa, qb := rndUsage(t, m1, "/a"), rndUsage(t, m1, "/b")
	t.Logf("write after foreign link: /a space=%d /b space=%d", qa.UsedSpace, qb.UsedSpace)
	if qa.UsedSpace != 8192 || qb.UsedSpace != 8192 {
		t.Fatalf("want /a=8192 /b=8192, got /a=%d /b=%d", qa.UsedSpace, qb.UsedSpace)
	}
}
