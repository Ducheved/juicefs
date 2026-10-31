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

package server

import (
	"bufio"
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/metad/proto"
)

func TestDroppedResolveIsNotAnAbort(t *testing.T) {
	lns := make([]net.Listener, 3)
	peers := make(map[uint64]string)
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		peers[uint64(i+1)] = ln.Addr().String()
	}
	servers := make([]*Server, 3)
	for i, ln := range lns {
		s, err := Open(Options{Dir: t.TempDir(), CacheMB: 16, MemtableMB: 4, SnapshotTTL: 5 * time.Second, MaxBatch: 64,
			ID: uint64(i + 1), Peers: peers, Tick: 20 * time.Millisecond, LogRetain: 1000, DedupWindow: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		servers[i] = s
		go func() { _ = s.Serve(ln) }()
	}
	t.Cleanup(func() {
		for _, s := range servers {
			if s != nil {
				_ = s.Close()
			}
		}
	})
	leader := -1
	for deadline := time.Now().Add(10 * time.Second); leader < 0 && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		for i, s := range servers {
			if s.Status().Ready {
				leader = i
			}
		}
	}
	if leader < 0 {
		t.Fatal("no leader")
	}
	target := (leader + 1) % 3
	_ = servers[target].Close()
	servers[target] = nil
	l := servers[leader]
	l.node.TransferLeadership(context.Background(), uint64(leader+1), uint64(target+1))
	for deadline := time.Now().Add(time.Second); l.node.Status().LeadTransferee != uint64(target+1); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("leadership transfer did not start")
		}
	}

	c, err := net.Dial("tcp", peers[uint64(leader+1)])
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := make([]byte, proto.TxnIDSize)
	_, _ = rand.Read(id)
	e := proto.NewEncoder(proto.OpResolve)
	e.Bytes(id)
	if _, err := c.Write(e.Frame()); err != nil {
		t.Fatal(err)
	}
	status, _, err := proto.ReadFrame(bufio.NewReader(c))
	if err != nil {
		t.Fatal(err)
	}
	if status == proto.StatusConflict {
		t.Fatal("a resolve the leader could not propose was answered as aborted")
	}
}
