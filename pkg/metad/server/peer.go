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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/juicedata/juicefs/pkg/metad/proto"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const snapshotChunk = 1 << 20

var snapshotRanges = [][2][]byte{{keyLastTime, {0, 'N'}}, {stateStart, stateEnd}, {userStart, userEnd}}

type peer struct {
	id   uint64
	addr string
	msgs chan raftpb.Message
}

type peerConn struct {
	c net.Conn
	w *bufio.Writer
}

func (s *Server) send(msgs []raftpb.Message) {
	for _, m := range msgs {
		p := s.peers[m.To]
		if p == nil {
			continue
		}
		if m.Type == raftpb.MsgSnap {
			s.wg.Add(1)
			go s.sendSnapshot(p, m)
			continue
		}
		select {
		case p.msgs <- m:
		default:
			s.node.ReportUnreachable(m.To)
		}
	}
}

func (s *Server) sendLoop(p *peer) {
	defer s.wg.Done()
	var pc *peerConn
	drop := func() {
		if pc != nil {
			_ = pc.c.Close()
			pc = nil
		}
		s.node.ReportUnreachable(p.id)
	}
	defer func() {
		if pc != nil {
			_ = pc.c.Close()
		}
	}()
	for {
		var m raftpb.Message
		select {
		case m = <-p.msgs:
		case <-s.stop:
			return
		}
		if pc == nil {
			c, err := net.DialTimeout("tcp", p.addr, time.Second)
			if err != nil {
				drop()
				continue
			}
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetNoDelay(true)
			}
			pc = &peerConn{c: c, w: bufio.NewWriterSize(c, 256<<10)}
		}
		data, err := m.Marshal()
		if err != nil {
			logger.Errorf("metad: marshal %s: %s", m.Type, err)
			continue
		}
		e := proto.NewEncoder(proto.OpRaft)
		e.Bytes(data)
		_ = pc.c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if _, err = pc.w.Write(e.Frame()); err == nil && len(p.msgs) == 0 {
			err = pc.w.Flush()
		}
		if err != nil {
			drop()
		}
	}
}

func (s *Server) sendSnapshot(p *peer, m raftpb.Message) {
	defer s.wg.Done()
	status := raft.SnapshotFinish
	if err := s.streamSnapshot(p.addr, m); err != nil {
		logger.Warnf("metad: snapshot %d to node %d: %s", m.Snapshot.Metadata.Index, p.id, err)
		status = raft.SnapshotFailure
	}
	s.node.ReportSnapshot(m.To, status)
}

func (s *Server) streamSnapshot(addr string, m raftpb.Message) error {
	data, err := m.Marshal()
	if err != nil {
		return err
	}
	snap := s.db.NewSnapshot()
	defer snap.Close()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return err
	}
	defer c.Close()
	go func() {
		<-s.stop
		_ = c.Close()
	}()
	w := bufio.NewWriterSize(c, 256<<10)
	e := proto.NewEncoder(proto.OpSnapshot)
	e.Bytes(data)
	if _, err := w.Write(e.Frame()); err != nil {
		return err
	}
	for _, r := range snapshotRanges {
		it, err := snap.NewIter(&pebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
		if err != nil {
			return err
		}
		e := proto.NewEncoder(proto.OpSnapshotChunk)
		size := 0
		for it.First(); it.Valid(); it.Next() {
			v, err := it.ValueAndErr()
			if err != nil {
				_ = it.Close()
				return err
			}
			e.Bytes(it.Key())
			e.Bytes(v)
			size += len(it.Key()) + len(v) + 8
			if size >= snapshotChunk {
				if _, err := w.Write(e.Frame()); err != nil {
					_ = it.Close()
					return err
				}
				e = proto.NewEncoder(proto.OpSnapshotChunk)
				size = 0
			}
		}
		err = it.Error()
		if cerr := it.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return err
		}
		if size > 0 {
			if _, err := w.Write(e.Frame()); err != nil {
				return err
			}
		}
	}
	if _, err := w.Write(proto.NewEncoder(proto.OpSnapshotEnd).Frame()); err != nil {
		return err
	}
	if err := w.Flush(); err != nil {
		return err
	}
	status, body, err := proto.ReadFrame(bufio.NewReader(c))
	if err != nil {
		return err
	}
	if status != proto.StatusOK {
		return fmt.Errorf("receiver: %s", proto.NewDecoder(body).Bytes())
	}
	return nil
}

func (s *Server) stagingDir(meta raftpb.SnapshotMetadata) string {
	return fmt.Sprintf("%s.install-%d-%d", s.opts.Dir, meta.Index, meta.Term)
}

func openStaging(dir string) (*pebble.DB, error) {
	return pebble.Open(dir, &pebble.Options{DisableWAL: true, MemTableSize: 32 << 20})
}

func (s *Server) cleanStaging() error {
	dirs, err := filepath.Glob(s.opts.Dir + ".install-*")
	if err != nil {
		return err
	}
	s.mu.Lock()
	applied := s.applied
	s.mu.Unlock()
	for _, dir := range dirs {
		var index, term uint64
		if _, err := fmt.Sscanf(strings.TrimPrefix(dir, s.opts.Dir), ".install-%d-%d", &index, &term); err != nil || index > applied {
			continue
		}
		if err := os.RemoveAll(dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) receiveSnapshot(body []byte, r *bufio.Reader) []byte {
	d := proto.NewDecoder(body)
	var m raftpb.Message
	if err := m.Unmarshal(d.Bytes()); err != nil || d.Err() != nil {
		return failure(proto.StatusError, fmt.Errorf("bad snapshot message: %v %v", err, d.Err()))
	}
	s.install.Lock()
	err := s.cleanStaging()
	if err == nil {
		err = s.stage(m.Snapshot.Metadata, r)
	}
	s.install.Unlock()
	if err != nil {
		return failure(proto.StatusError, err)
	}
	if err := s.node.Step(context.Background(), m); err != nil {
		return failure(proto.StatusError, err)
	}
	logger.Infof("metad: node %d staged snapshot %d from node %d", s.opts.ID, m.Snapshot.Metadata.Index, m.From)
	return proto.NewEncoder(proto.StatusOK).Frame()
}

func (s *Server) stage(meta raftpb.SnapshotMetadata, r *bufio.Reader) error {
	dir := s.stagingDir(meta)
	if err := os.RemoveAll(dir); err != nil {
		return err
	}
	db, err := openStaging(dir)
	if err != nil {
		return err
	}
	defer db.Close()
	for {
		op, body, err := proto.ReadFrame(r)
		if err != nil {
			return err
		}
		if op == proto.OpSnapshotEnd {
			break
		}
		if op != proto.OpSnapshotChunk {
			return fmt.Errorf("unexpected op %d in snapshot stream", op)
		}
		b := db.NewBatch()
		d := proto.NewDecoder(body)
		for d.More() {
			if err := b.Set(d.Bytes(), d.Bytes(), nil); err != nil {
				_ = b.Close()
				return err
			}
		}
		if err := d.Err(); err != nil {
			_ = b.Close()
			return err
		}
		err = b.Commit(pebble.NoSync)
		_ = b.Close()
		if err != nil {
			return err
		}
	}
	b := db.NewBatch()
	defer b.Close()
	if err := setProto(b, keyInstall, &meta); err != nil {
		return err
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		return err
	}
	return db.Flush()
}

func (s *Server) resumeInstall() error {
	var meta raftpb.SnapshotMetadata
	found, err := readProto(s.db, keyInstall, &meta)
	if err != nil || !found {
		return err
	}
	logger.Infof("metad: resuming install of snapshot %d", meta.Index)
	return s.installStaged(meta, false)
}

func (s *Server) installStaged(meta raftpb.SnapshotMetadata, mark bool) error {
	dir := s.stagingDir(meta)
	if _, err := os.Stat(dir); err != nil {
		return fmt.Errorf("staged snapshot %d/%d: %w", meta.Index, meta.Term, err)
	}
	staged, err := openStaging(dir)
	if err != nil {
		return err
	}
	err = s.copyStaged(staged, meta, mark)
	if cerr := staged.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.RemoveAll(dir)
}

func (s *Server) copyStaged(staged *pebble.DB, meta raftpb.SnapshotMetadata, mark bool) error {
	var got raftpb.SnapshotMetadata
	found, err := readProto(staged, keyInstall, &got)
	if err != nil {
		return err
	}
	if !found || got.Index != meta.Index || got.Term != meta.Term {
		return fmt.Errorf("staged snapshot %d/%d does not match %d/%d", got.Index, got.Term, meta.Index, meta.Term)
	}
	if mark {
		b := s.db.NewBatch()
		err := setProto(b, keyInstall, &meta)
		if err == nil {
			err = b.Commit(pebble.Sync)
		}
		_ = b.Close()
		if err != nil {
			return err
		}
	}
	b := s.db.NewBatch()
	for _, r := range snapshotRanges {
		if err := b.DeleteRange(r[0], r[1], nil); err != nil {
			_ = b.Close()
			return err
		}
	}
	for _, r := range snapshotRanges {
		it, err := staged.NewIter(&pebble.IterOptions{LowerBound: r[0], UpperBound: r[1]})
		if err != nil {
			_ = b.Close()
			return err
		}
		for it.First(); it.Valid(); it.Next() {
			v, err := it.ValueAndErr()
			if err == nil {
				err = b.Set(it.Key(), v, nil)
			}
			if err == nil && b.Len() >= 4<<20 {
				err = b.Commit(pebble.NoSync)
				_ = b.Close()
				b = s.db.NewBatch()
			}
			if err != nil {
				_ = it.Close()
				_ = b.Close()
				return err
			}
		}
		err = it.Error()
		if cerr := it.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			_ = b.Close()
			return err
		}
	}
	err = s.log.resetTo(b, meta)
	if err == nil {
		err = b.Set(keyApplied, u64(meta.Index), nil)
	}
	if err == nil {
		err = b.Delete(keyInstall, nil)
	}
	if err == nil {
		err = b.Commit(pebble.Sync)
	}
	_ = b.Close()
	if err != nil {
		return err
	}
	s.expired = nil
	last, err := readU64(s.db, keyLastTime)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.applied = meta.Index
	s.lastTime = max(s.lastTime, int64(last))
	s.mu.Unlock()
	return nil
}
