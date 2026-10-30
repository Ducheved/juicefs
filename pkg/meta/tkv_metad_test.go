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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/metad/proto"
	"github.com/juicedata/juicefs/pkg/metad/server"
)

func startMetad(t *testing.T, ttl time.Duration) string {
	s, err := server.Open(server.Options{Dir: t.TempDir(), CacheMB: 64, MemtableMB: 8, SnapshotTTL: ttl, MaxBatch: 1024})
	if err != nil {
		t.Fatalf("open metad: %s", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %s", err)
	}
	go func() { _ = s.Serve(ln) }()
	t.Cleanup(func() { _ = s.Close() })
	return ln.Addr().String()
}

func TestMetadKV(t *testing.T) {
	c, err := newMetadClient(startMetad(t, 5*time.Second) + "/jfs")
	if err != nil {
		t.Fatal(err)
	}
	testTKV(t, c)
}

func TestMetadClient(t *testing.T) {
	m, err := newKVMeta("metad", startMetad(t, 5*time.Second)+"/jfs-unit-test", testConfig())
	if err != nil || m.Name() != "metad" {
		t.Fatalf("create meta: %s", err)
	}
	testMeta(t, m)
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
