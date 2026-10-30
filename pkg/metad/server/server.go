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
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/google/btree"
	"github.com/juicedata/juicefs/pkg/metad/proto"
	"github.com/juicedata/juicefs/pkg/utils"
)

var logger = utils.GetLogger("juicefs")

type Options struct {
	Dir         string
	CacheMB     int64
	MemtableMB  uint64
	SnapshotTTL time.Duration
	MaxBatch    int
}

type snapshot struct {
	seq  uint64
	snap *pebble.Snapshot
	refs int
	last time.Time
}

type written struct {
	key []byte
	seq uint64
}

type batchKeys struct {
	seq  uint64
	keys [][]byte
}

type request struct {
	commit *proto.Commit
	begin  []byte
	end    []byte
	done   chan byte
}

type Server struct {
	opts    Options
	db      *pebble.DB
	cache   *pebble.Cache
	pending chan *request

	mu        sync.Mutex
	committed uint64
	current   *snapshot
	snapshots map[uint64]*snapshot
	recent    *btree.BTreeG[written]
	history   []batchKeys
	floor     uint64

	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	ln     net.Listener
	stop   chan struct{}
	wg     sync.WaitGroup
}

func Open(opts Options) (*Server, error) {
	if opts.CacheMB <= 0 || opts.MemtableMB == 0 || opts.SnapshotTTL <= 0 || opts.MaxBatch <= 0 {
		return nil, fmt.Errorf("metad: invalid options %+v", opts)
	}
	cache := pebble.NewCache(opts.CacheMB << 20)
	po := &pebble.Options{Cache: cache, MemTableSize: opts.MemtableMB << 20, MemTableStopWritesThreshold: 4}
	po.EnsureDefaults()
	for i := range po.Levels {
		po.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	db, err := pebble.Open(opts.Dir, po)
	if err != nil {
		cache.Unref()
		return nil, err
	}
	s := &Server{
		opts:      opts,
		db:        db,
		cache:     cache,
		pending:   make(chan *request, 4*opts.MaxBatch),
		committed: 1,
		snapshots: make(map[uint64]*snapshot),
		recent:    btree.NewG(32, func(a, b written) bool { return bytes.Compare(a.key, b.key) < 0 }),
		floor:     1,
		conns:     make(map[net.Conn]struct{}),
		stop:      make(chan struct{}),
	}
	s.wg.Add(2)
	go s.commitLoop()
	go s.expireLoop()
	return s, nil
}

func (s *Server) Serve(ln net.Listener) error {
	s.ln = ln
	for {
		c, err := ln.Accept()
		if err != nil {
			select {
			case <-s.stop:
				return nil
			default:
				return err
			}
		}
		s.connMu.Lock()
		s.conns[c] = struct{}{}
		s.connMu.Unlock()
		s.wg.Add(1)
		go s.handle(c)
	}
}

func (s *Server) Close() error {
	close(s.stop)
	if s.ln != nil {
		_ = s.ln.Close()
	}
	s.connMu.Lock()
	for c := range s.conns {
		_ = c.Close()
	}
	s.connMu.Unlock()
	s.wg.Wait()
	s.mu.Lock()
	for _, sn := range s.snapshots {
		_ = sn.snap.Close()
	}
	s.snapshots = nil
	s.mu.Unlock()
	err := s.db.Close()
	s.cache.Unref()
	return err
}

func (s *Server) handle(c net.Conn) {
	defer s.wg.Done()
	defer func() {
		s.connMu.Lock()
		delete(s.conns, c)
		s.connMu.Unlock()
		_ = c.Close()
	}()
	r := bufio.NewReaderSize(c, 64<<10)
	w := bufio.NewWriterSize(c, 64<<10)
	for {
		op, body, err := proto.ReadFrame(r)
		if err != nil {
			return
		}
		resp := s.dispatch(op, body)
		if _, err = w.Write(resp); err == nil {
			err = w.Flush()
		}
		if err != nil {
			return
		}
	}
}

func failure(status byte, err error) []byte {
	e := proto.NewEncoder(status)
	e.Bytes([]byte(err.Error()))
	return e.Frame()
}

func (s *Server) dispatch(op byte, body []byte) []byte {
	switch op {
	case proto.OpGet:
		return s.get(body)
	case proto.OpScan:
		return s.scan(body)
	case proto.OpCommit:
		c, err := proto.DecodeCommit(body)
		if err != nil {
			return failure(proto.StatusError, err)
		}
		return s.submit(&request{commit: c})
	case proto.OpDeleteRange:
		d := proto.NewDecoder(body)
		begin, end := d.Bytes(), d.Bytes()
		if err := d.Err(); err != nil {
			return failure(proto.StatusError, err)
		}
		return s.submit(&request{begin: begin, end: end})
	case proto.OpPing:
		s.mu.Lock()
		seq := s.committed
		s.mu.Unlock()
		e := proto.NewEncoder(proto.StatusOK)
		e.U64(seq)
		return e.Frame()
	default:
		return failure(proto.StatusError, fmt.Errorf("unknown op %d", op))
	}
}

func (s *Server) acquire(seq uint64) (*snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshots == nil {
		return nil, errors.New("metad: closed")
	}
	var sn *snapshot
	if seq == 0 {
		if s.current == nil || s.current.seq != s.committed {
			s.current = &snapshot{seq: s.committed, snap: s.db.NewSnapshot()}
			s.snapshots[s.current.seq] = s.current
		}
		sn = s.current
	} else if sn = s.snapshots[seq]; sn == nil {
		return nil, proto.ErrSnapshotGone
	}
	sn.refs++
	sn.last = time.Now()
	return sn, nil
}

func (s *Server) release(sn *snapshot) {
	s.mu.Lock()
	sn.refs--
	sn.last = time.Now()
	s.mu.Unlock()
}

func readFailure(err error) []byte {
	if errors.Is(err, proto.ErrSnapshotGone) {
		return failure(proto.StatusSnapshotGone, err)
	}
	return failure(proto.StatusError, err)
}

func (s *Server) get(body []byte) []byte {
	d := proto.NewDecoder(body)
	seq := d.U64()
	n := d.U32()
	keys := make([][]byte, 0, n)
	for i := uint32(0); i < n && d.Valid(); i++ {
		keys = append(keys, d.Bytes())
	}
	if err := d.Err(); err != nil {
		return failure(proto.StatusError, err)
	}
	sn, err := s.acquire(seq)
	if err != nil {
		return readFailure(err)
	}
	defer s.release(sn)
	e := proto.NewEncoder(proto.StatusOK)
	e.U64(sn.seq)
	for _, k := range keys {
		v, closer, err := sn.snap.Get(k)
		if errors.Is(err, pebble.ErrNotFound) {
			e.U8(0)
			continue
		}
		if err != nil {
			return failure(proto.StatusError, err)
		}
		e.U8(1)
		e.Bytes(v)
		_ = closer.Close()
	}
	return e.Frame()
}

func (s *Server) scan(body []byte) []byte {
	d := proto.NewDecoder(body)
	seq := d.U64()
	begin, end := d.Bytes(), d.Bytes()
	keysOnly := d.U8() == 1
	limit := int(d.U32())
	if err := d.Err(); err != nil {
		return failure(proto.StatusError, err)
	}
	if limit <= 0 || limit > proto.ScanLimit {
		limit = proto.ScanLimit
	}
	sn, err := s.acquire(seq)
	if err != nil {
		return readFailure(err)
	}
	defer s.release(sn)
	iopts := &pebble.IterOptions{LowerBound: begin}
	if len(end) > 0 {
		iopts.UpperBound = end
	}
	it, err := sn.snap.NewIter(iopts)
	if err != nil {
		return failure(proto.StatusError, err)
	}
	defer it.Close()
	var keys, values [][]byte
	more := byte(0)
	for it.First(); it.Valid(); it.Next() {
		if len(keys) == limit {
			more = 1
			break
		}
		keys = append(keys, append([]byte(nil), it.Key()...))
		if keysOnly {
			values = append(values, nil)
			continue
		}
		v, err := it.ValueAndErr()
		if err != nil {
			return failure(proto.StatusError, err)
		}
		values = append(values, append([]byte(nil), v...))
	}
	if err := it.Error(); err != nil {
		return failure(proto.StatusError, err)
	}
	e := proto.NewEncoder(proto.StatusOK)
	e.U64(sn.seq)
	e.U8(more)
	e.U32(uint32(len(keys)))
	for i := range keys {
		e.Bytes(keys[i])
		e.Bytes(values[i])
	}
	return e.Frame()
}

func (s *Server) submit(r *request) []byte {
	r.done = make(chan byte, 1)
	select {
	case s.pending <- r:
	case <-s.stop:
		return failure(proto.StatusError, errors.New("metad: closed"))
	}
	var status byte
	select {
	case status = <-r.done:
	case <-s.stop:
		return failure(proto.StatusError, errors.New("metad: closed"))
	}
	if status == proto.StatusOK {
		return proto.NewEncoder(proto.StatusOK).Frame()
	}
	return failure(status, fmt.Errorf("commit status %d", status))
}

func (s *Server) conflicts(c *proto.Commit) byte {
	if len(c.Keys) == 0 && len(c.Ranges) == 0 {
		return proto.StatusOK
	}
	if c.Snapshot < s.floor {
		return proto.StatusSnapshotGone
	}
	for _, k := range c.Keys {
		if w, ok := s.recent.Get(written{key: k}); ok && w.seq > c.Snapshot {
			return proto.StatusConflict
		}
	}
	for _, r := range c.Ranges {
		hit := false
		check := func(w written) bool {
			hit = w.seq > c.Snapshot
			return !hit
		}
		if len(r.End) == 0 {
			s.recent.AscendGreaterOrEqual(written{key: r.Begin}, check)
		} else {
			s.recent.AscendRange(written{key: r.Begin}, written{key: r.End}, check)
		}
		if hit {
			return proto.StatusConflict
		}
	}
	return proto.StatusOK
}

func (s *Server) commitLoop() {
	defer s.wg.Done()
	for {
		var reqs []*request
		select {
		case r := <-s.pending:
			reqs = append(reqs, r)
		case <-s.stop:
			return
		}
	drain:
		for len(reqs) < s.opts.MaxBatch {
			select {
			case r := <-s.pending:
				reqs = append(reqs, r)
			default:
				break drain
			}
		}
		s.commitBatch(reqs)
	}
}

func (s *Server) commitBatch(reqs []*request) {
	b := s.db.NewBatch()
	defer b.Close()
	status := make([]byte, len(reqs))
	var keys [][]byte
	s.mu.Lock()
	seq := s.committed + 1
	for i, r := range reqs {
		if r.commit == nil {
			end := r.end
			if len(end) == 0 {
				end = bytes.Repeat([]byte{0xff}, 64)
			}
			if err := b.DeleteRange(r.begin, end, nil); err != nil {
				status[i] = proto.StatusError
			}
			continue
		}
		if status[i] = s.conflicts(r.commit); status[i] != proto.StatusOK {
			continue
		}
		for _, w := range r.commit.Writes {
			var err error
			if w.Op == proto.WriteSet {
				err = b.Set(w.Key, w.Value, nil)
			} else {
				err = b.Delete(w.Key, nil)
			}
			if err != nil {
				status[i] = proto.StatusError
				break
			}
			s.recent.ReplaceOrInsert(written{key: w.Key, seq: seq})
			keys = append(keys, w.Key)
		}
	}
	s.history = append(s.history, batchKeys{seq: seq, keys: keys})
	s.mu.Unlock()
	if err := b.Commit(pebble.Sync); err != nil {
		logger.Errorf("metad: commit batch %d: %s", seq, err)
		for i := range status {
			if status[i] == proto.StatusOK {
				status[i] = proto.StatusError
			}
		}
	} else {
		s.mu.Lock()
		s.committed = seq
		s.mu.Unlock()
	}
	for i, r := range reqs {
		r.done <- status[i]
	}
}

func (s *Server) expireLoop() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.SnapshotTTL / 4)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.expire(time.Now())
		case <-s.stop:
			return
		}
	}
}

func (s *Server) expire(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	floor := s.committed
	for seq, sn := range s.snapshots {
		if sn.refs == 0 && now.Sub(sn.last) > s.opts.SnapshotTTL {
			_ = sn.snap.Close()
			delete(s.snapshots, seq)
			if s.current == sn {
				s.current = nil
			}
			continue
		}
		floor = min(floor, sn.seq)
	}
	s.floor = floor
	n := 0
	for ; n < len(s.history) && s.history[n].seq <= floor; n++ {
		for _, k := range s.history[n].keys {
			if w, ok := s.recent.Get(written{key: k}); ok && w.seq == s.history[n].seq {
				s.recent.Delete(w)
			}
		}
	}
	s.history = append(s.history[:0], s.history[n:]...)
}
