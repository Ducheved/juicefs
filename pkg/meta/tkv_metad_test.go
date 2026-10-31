/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/metad/proto"
	"github.com/juicedata/juicefs/pkg/metad/server"
)

type metadCluster struct {
	t       *testing.T
	ttl     time.Duration
	retain  uint64
	dedup   time.Duration
	netMu   sync.Mutex
	cut     map[int]bool
	links   map[int][]net.Conn
	dirs    []string
	addrs   []string
	peers   map[uint64]string
	servers []*server.Server
}

func newMetadCluster(t *testing.T, n int, ttl time.Duration, retain uint64, dedup time.Duration) *metadCluster {
	c := &metadCluster{t: t, ttl: ttl, retain: retain, dedup: dedup, peers: make(map[uint64]string), servers: make([]*server.Server, n),
		cut: make(map[int]bool), links: make(map[int][]net.Conn)}
	lns := make([]net.Listener, n)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %s", err)
		}
		lns[i] = ln
		c.dirs = append(c.dirs, t.TempDir())
		c.addrs = append(c.addrs, ln.Addr().String())
		c.peers[uint64(i+1)] = ln.Addr().String()
	}
	for i, ln := range lns {
		c.serve(i, ln)
	}
	t.Cleanup(func() {
		for i := range c.servers {
			c.stop(i)
		}
	})
	return c
}

func (c *metadCluster) serve(i int, ln net.Listener) {
	s, err := server.Open(server.Options{Dir: c.dirs[i], CacheMB: 64, MemtableMB: 8, SnapshotTTL: c.ttl, MaxBatch: 1024,
		ID: uint64(i + 1), Peers: c.peers, Tick: 20 * time.Millisecond, LogRetain: c.retain, DedupWindow: c.dedup, Dial: c.dial(i)})
	if err != nil {
		c.t.Fatalf("open metad %d: %s", i+1, err)
	}
	c.servers[i] = s
	go func() { _ = s.Serve(ln) }()
}

func (c *metadCluster) dial(from int) func(network, addr string, timeout time.Duration) (net.Conn, error) {
	return func(network, addr string, timeout time.Duration) (net.Conn, error) {
		to := -1
		for i, a := range c.addrs {
			if a == addr {
				to = i
			}
		}
		c.netMu.Lock()
		defer c.netMu.Unlock()
		if c.cut[from] || c.cut[to] {
			return nil, errors.New("partitioned")
		}
		nc, err := net.DialTimeout(network, addr, timeout)
		if err == nil {
			c.links[from] = append(c.links[from], nc)
			c.links[to] = append(c.links[to], nc)
		}
		return nc, err
	}
}

func (c *metadCluster) isolate(i int, on bool) {
	c.netMu.Lock()
	defer c.netMu.Unlock()
	c.cut[i] = on
	if on {
		for _, nc := range c.links[i] {
			_ = nc.Close()
		}
		c.links[i] = nil
	}
}

func (c *metadCluster) stop(i int) {
	if c.servers[i] != nil {
		_ = c.servers[i].Close()
		c.servers[i] = nil
	}
}

func (c *metadCluster) start(i int) {
	ln, err := net.Listen("tcp", c.addrs[i])
	if err != nil {
		c.t.Fatalf("listen %s: %s", c.addrs[i], err)
	}
	c.serve(i, ln)
}

func (c *metadCluster) leader() int {
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for i, s := range c.servers {
			if s != nil && s.Status().Ready {
				return i
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader")
	return -1
}

func (c *metadCluster) url(path string) string {
	return strings.Join(c.addrs, ",") + path
}

func startMetad(t *testing.T, ttl time.Duration) string {
	return newMetadCluster(t, 1, ttl, 1000, time.Minute).url("")
}

func TestMetadKV(t *testing.T) {
	c, err := newMetadClient(startMetad(t, 5*time.Second) + "/jfs")
	if err != nil {
		t.Fatal(err)
	}
	testTKV(t, c)
}

func TestMetadClient(t *testing.T) {
	m, err := newKVMeta("metad", newMetadCluster(t, 3, 5*time.Second, 1000, time.Minute).url("/jfs-unit-test"), testConfig())
	if err != nil || m.Name() != "metad" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
}

func TestMetadLookup(t *testing.T) {
	c, err := newMetadClient(startMetad(t, 5*time.Second) + "/jfs")
	if err != nil {
		t.Fatal(err)
	}
	m := &kvMeta{}
	head, tail := m.fmtKey("A"), []byte("I")
	check := func(tx *kvTxn, name string, ino Ino, attr string) {
		v, a, joined := tx.lookup(m.entryKey(1, name), head, tail)
		if !joined {
			t.Fatalf("lookup %s: not joined", name)
		}
		if ino == 0 {
			if v != nil || a != nil {
				t.Fatalf("lookup %s: expect nothing, got %x %q", name, v, a)
			}
			return
		}
		if _, got := m.parseEntry(v); got != ino {
			t.Fatalf("lookup %s: expect inode %d, got %d", name, ino, got)
		}
		if string(a) != attr {
			t.Fatalf("lookup %s: expect attr %q, got %q", name, attr, a)
		}
	}
	if err := c.txn(Background(), func(tx *kvTxn) error {
		tx.set(m.entryKey(1, "f"), m.packEntry(TypeFile, 0x0102030405))
		tx.set(m.inodeKey(0x0102030405), []byte("attr-f"))
		tx.set(m.entryKey(1, "g"), m.packEntry(TypeFile, 7))
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}
	if err := c.txn(Background(), func(tx *kvTxn) error {
		check(tx, "f", 0x0102030405, "attr-f")
		check(tx, "g", 7, "")
		check(tx, "none", 0, "")
		tx.set(m.inodeKey(7), []byte("attr-g"))
		check(tx, "g", 7, "attr-g")
		tx.set(m.entryKey(1, "h"), m.packEntry(TypeFile, 0x0102030405))
		check(tx, "h", 0x0102030405, "attr-f")
		tx.delete(m.inodeKey(0x0102030405))
		check(tx, "f", 0x0102030405, "")
		return nil
	}, 0); err != nil {
		t.Fatal(err)
	}
}

func TestMetadTransactions(t *testing.T) {
	c, err := newMetadClient(startMetad(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	run := func(f func(tx *kvTxn)) error {
		return c.txn(Background(), func(tx *kvTxn) error {
			f(tx)
			return nil
		}, 0)
	}
	if err := run(func(tx *kvTxn) { tx.set([]byte("a"), []byte("1")) }); err != nil {
		t.Fatal(err)
	}

	err = run(func(tx *kvTxn) {
		tx.get([]byte("a"))
		if err := run(func(tx2 *kvTxn) { tx2.set([]byte("a"), []byte("2")) }); err != nil {
			t.Fatal(err)
		}
		if v := tx.get([]byte("a")); string(v) != "1" {
			t.Fatalf("snapshot read: expect 1, got %q", v)
		}
		tx.set([]byte("b"), []byte("x"))
	})
	if !errors.Is(err, proto.ErrConflict) {
		t.Fatalf("lost update: expect conflict, got %v", err)
	}

	err = run(func(tx *kvTxn) {
		tx.scan([]byte("d"), []byte("e"), true, func(k, v []byte) bool { return true })
		if err := run(func(tx2 *kvTxn) { tx2.set([]byte("d1"), []byte("x")) }); err != nil {
			t.Fatal(err)
		}
		tx.set([]byte("c"), []byte("x"))
	})
	if !errors.Is(err, proto.ErrConflict) {
		t.Fatalf("phantom: expect conflict, got %v", err)
	}

	err = run(func(tx *kvTxn) {
		tx.get([]byte("a"))
		if err := run(func(tx2 *kvTxn) { tx2.set([]byte("z"), []byte("x")) }); err != nil {
			t.Fatal(err)
		}
		tx.set([]byte("y"), []byte("x"))
	})
	if err != nil {
		t.Fatalf("disjoint writes must commit: %s", err)
	}

	var seen []string
	if err := run(func(tx *kvTxn) {
		tx.set([]byte("d0"), []byte("x"))
		tx.delete([]byte("d1"))
		tx.set([]byte("d2"), []byte("x"))
		tx.scan([]byte("d"), []byte("e"), true, func(k, v []byte) bool {
			seen = append(seen, string(k))
			return true
		})
	}); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] != "d0" || seen[1] != "d2" {
		t.Fatalf("scan must see own writes and deletes: %v", seen)
	}

	err = run(func(tx *kvTxn) {
		tx.get([]byte("a"))
		time.Sleep(2500 * time.Millisecond)
		if err := run(func(tx2 *kvTxn) { tx2.set([]byte("q"), []byte("x")) }); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1500 * time.Millisecond)
		tx.set([]byte("r"), []byte("x"))
	})
	if !c.shouldRetry(err) {
		t.Fatalf("expired snapshot: expect a retryable error, got %v", err)
	}
}

func metadSet(c tkvClient, key, value string) error {
	return c.txn(Background(), func(tx *kvTxn) error {
		tx.get([]byte(key))
		tx.set([]byte(key), []byte(value))
		return nil
	}, 0)
}

func metadGet(t *testing.T, c tkvClient, key string) string {
	var v []byte
	if err := c.simpleTxn(Background(), func(tx *kvTxn) error {
		v = tx.get([]byte(key))
		return nil
	}, 0); err != nil {
		t.Fatalf("get %s: %s", key, err)
	}
	return string(v)
}

func TestMetadFailover(t *testing.T) {
	mc := newMetadCluster(t, 3, 5*time.Second, 1000, time.Minute)
	c, err := newMetadClient(mc.url("/jfs"))
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 3; round++ {
		for i := 0; i < 50; i++ {
			if err := metadSet(c, fmt.Sprintf("k%d-%d", round, i), "v"); err != nil {
				t.Fatalf("round %d set %d: %s", round, i, err)
			}
		}
		old := mc.leader()
		mc.stop(old)
		for r := 0; r <= round; r++ {
			for i := 0; i < 50; i++ {
				if v := metadGet(t, c, fmt.Sprintf("k%d-%d", r, i)); v != "v" {
					t.Fatalf("after losing node %d: k%d-%d = %q", old+1, r, i, v)
				}
			}
		}
		if now := mc.leader(); now == old {
			t.Fatalf("node %d still leads after stop", old+1)
		}
		mc.start(old)
	}
}

func TestMetadSnapshotInstall(t *testing.T) {
	mc := newMetadCluster(t, 3, 5*time.Second, 20, time.Minute)
	c, err := newMetadClient(mc.url("/jfs"))
	if err != nil {
		t.Fatal(err)
	}
	lead := mc.leader()
	lag := (lead + 1) % 3
	mc.stop(lag)
	for i := 0; i < 300; i++ {
		if err := metadSet(c, fmt.Sprintf("s%d", i), fmt.Sprint(i)); err != nil {
			t.Fatalf("set %d: %s", i, err)
		}
	}
	if first := mc.servers[lead].Status().First; first <= 100 {
		t.Fatalf("leader log not compacted: first index %d", first)
	}
	mc.start(lag)
	target := mc.servers[lead].Status().Applied
	deadline := time.Now().Add(10 * time.Second)
	for mc.servers[lag].Status().Applied < target {
		if time.Now().After(deadline) {
			t.Fatalf("node %d stuck at %+v, leader at %d", lag+1, mc.servers[lag].Status(), target)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if st := mc.servers[lag].Status(); st.First <= 100 {
		t.Fatalf("node %d caught up without a snapshot: %+v", lag+1, st)
	}
	mc.stop(lag)
	mc.start(lag)
	other := 3 - lead - lag
	mc.stop(other)
	for i := 0; i < 300; i += 7 {
		if v := metadGet(t, c, fmt.Sprintf("s%d", i)); v != fmt.Sprint(i) {
			t.Fatalf("s%d = %q", i, v)
		}
	}
	mc.stop(lead)
	mc.start(other)
	for i := 0; i < 300; i += 7 {
		if v := metadGet(t, c, fmt.Sprintf("s%d", i)); v != fmt.Sprint(i) {
			t.Fatalf("without the old leader: s%d = %q", i, v)
		}
	}
}

func metadRaw(t *testing.T, addr string, frame []byte) (byte, error) {
	cn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer cn.Close()
	if _, err := cn.Write(frame); err != nil {
		t.Fatal(err)
	}
	status, _, err := proto.ReadFrame(bufio.NewReader(cn))
	return status, err
}

func metadResolve(t *testing.T, addr string, id []byte) byte {
	e := proto.NewEncoder(proto.OpResolve)
	e.Bytes(id)
	status, err := metadRaw(t, addr, e.Frame())
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func TestMetadResolve(t *testing.T) {
	mc := newMetadCluster(t, 1, 5*time.Second, 1000, time.Minute)
	addr := mc.addrs[0]
	c, err := newMetadClient(addr)
	if err != nil {
		t.Fatal(err)
	}
	write := func(id []byte, key string) []byte {
		return proto.EncodeCommit(&proto.Commit{ID: id, Writes: []proto.Write{{Op: proto.WriteSet, Key: []byte(key), Value: []byte("x")}}})
	}

	id := newTxnID()
	if st, err := metadRaw(t, addr, write(id, "r1")); err != nil || st != proto.StatusOK {
		t.Fatalf("commit r1: %d %v", st, err)
	}
	if st := metadResolve(t, addr, id); st != proto.StatusOK {
		t.Fatalf("resolve of a committed transaction: status %d", st)
	}

	id = newTxnID()
	if st := metadResolve(t, addr, id); st != proto.StatusConflict {
		t.Fatalf("resolve of an unknown transaction: status %d", st)
	}
	if st, err := metadRaw(t, addr, write(id, "r2")); err != nil || st != proto.StatusConflict {
		t.Fatalf("commit after resolve: %d %v", st, err)
	}
	if v := metadGet(t, c, "r2"); v != "" {
		t.Fatalf("aborted transaction applied: r2 = %q", v)
	}

	for i := 0; i < 20; i++ {
		id = newTxnID()
		key := fmt.Sprintf("r3-%d", i)
		cn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cn.Write(write(id, key)); err != nil {
			t.Fatal(err)
		}
		_ = cn.Close()
		st := metadResolve(t, addr, id)
		if v := metadGet(t, c, key); (st == proto.StatusOK) != (v == "x") {
			t.Fatalf("resolve says %d but %s = %q", st, key, v)
		}
	}

	id = newTxnID()
	err = c.(*metadClient).commit(id, write(id, "r4"))
	if err != nil || metadGet(t, c, "r4") != "x" {
		t.Fatalf("client commit: %v", err)
	}
}

func TestMetadExpiryThroughput(t *testing.T) {
	c, err := newMetadClient(newMetadCluster(t, 1, 5*time.Second, 1000, 200*time.Millisecond).url("/x?conns=16"))
	if err != nil {
		t.Fatal(err)
	}
	rate := func(w int, d time.Duration) int {
		var n atomic.Int64
		stop := time.Now().Add(d)
		var wg sync.WaitGroup
		for g := 0; g < 16; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; time.Now().Before(stop); i++ {
					if err := metadSet(c, fmt.Sprintf("w%d-%d-%d", w, g, i), "v"); err != nil {
						t.Errorf("set: %s", err)
						return
					}
					n.Add(1)
				}
			}(g)
		}
		wg.Wait()
		return int(n.Load())
	}
	first := rate(0, time.Second)
	rate(1, 4*time.Second)
	last := rate(2, time.Second)
	if last*3 < first {
		t.Fatalf("throughput fell from %d to %d txn/s once transaction records expire", first, last)
	}
}

func TestMetadLeaseRead(t *testing.T) {
	mc := newMetadCluster(t, 3, 5*time.Second, 1000, time.Minute)
	a, err := newMetadClient(mc.url(""))
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 10; round++ {
		old, fresh := fmt.Sprintf("old%d", round), fmt.Sprintf("new%d", round)
		if err := metadSet(a, "k", old); err != nil {
			t.Fatal(err)
		}
		l := mc.leader()
		if v := metadGet(t, a, "k"); v != old {
			t.Fatalf("round %d: read %q, expect %q", round, v, old)
		}
		mc.isolate(l, true)
		var rest []string
		for i, addr := range mc.addrs {
			if i != l {
				rest = append(rest, addr)
			}
		}
		b, err := newMetadClient(strings.Join(rest, ","))
		if err != nil {
			t.Fatal(err)
		}
		if err := metadSet(b, "k", fresh); err != nil {
			t.Fatal(err)
		}
		if v := metadGet(t, a, "k"); v != fresh {
			t.Fatalf("round %d: read %q through the isolated leader after %q was committed by the new one", round, v, fresh)
		}
		_ = b.close()
		mc.isolate(l, false)
		deadline := time.Now().Add(10 * time.Second)
		for mc.servers[l].Status().Applied < mc.servers[mc.leader()].Status().Applied {
			if time.Now().After(deadline) {
				t.Fatalf("round %d: node %d did not catch up", round, l+1)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
