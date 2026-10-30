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
	"context"
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
	"go.etcd.io/raft/v3"
)

var logger = utils.GetLogger("juicefs")

type Options struct {
	Dir         string
	CacheMB     int64
	MemtableMB  uint64
	SnapshotTTL time.Duration
	MaxBatch    int
	ID          uint64
	Peers       map[uint64]string
	Tick        time.Duration
	LogRetain   uint64
	DedupWindow time.Duration
}

type snapshot struct {
	seq  uint64
	snap *pebble.Snapshot
	refs int
	last time.Time
}

type written struct {
	key     []byte
	seq     uint64
	pending int
}

type batchKeys struct {
	seq  uint64
	keys [][]byte
}

type request struct {
	commit  *proto.Commit
	resolve []byte
	done    chan byte
}

type Server struct {
	opts    Options
	db      *pebble.DB
	cache   *pebble.Cache
	log     *logStore
	node    raft.Node
	peers   map[uint64]*peer
	pending chan *request
	applyMu sync.Mutex
	install sync.Mutex

	mu        sync.Mutex
	applied   uint64
	lastTime  int64
	term      uint64
	lead      uint64
	leader    bool
	ready     bool
	base      uint64
	current   *snapshot
	snapshots map[uint64]*snapshot
	recent    *btree.BTreeG[written]
	history   []batchKeys
	floor     uint64
	prop      uint64
	waiters   map[uint64][]*request
	closed    bool

	connMu sync.Mutex
	conns  map[net.Conn]struct{}
	ln     net.Listener
	stop   chan struct{}
	wg     sync.WaitGroup
}

func Open(opts Options) (*Server, error) {
	if opts.CacheMB <= 0 || opts.MemtableMB == 0 || opts.SnapshotTTL <= 0 || opts.MaxBatch <= 0 || opts.Tick <= 0 || opts.LogRetain == 0 || opts.DedupWindow <= 0 {
		return nil, fmt.Errorf("metad: invalid options %+v", opts)
	}
	if _, ok := opts.Peers[opts.ID]; !ok || opts.ID == 0 {
		return nil, fmt.Errorf("metad: node %d is not among peers %v", opts.ID, opts.Peers)
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
		peers:     make(map[uint64]*peer),
		pending:   make(chan *request, 4*opts.MaxBatch),
		snapshots: make(map[uint64]*snapshot),
		recent:    btree.NewG(32, func(a, b written) bool { return bytes.Compare(a.key, b.key) < 0 }),
		prop:      uint64(time.Now().UnixNano()),
		waiters:   make(map[uint64][]*request),
		conns:     make(map[net.Conn]struct{}),
		stop:      make(chan struct{}),
	}
	if err := s.start(); err != nil {
		_ = db.Close()
		cache.Unref()
		return nil, err
	}
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
	s.node.Stop()
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
	s.closed = true
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
		var resp []byte
		if op == proto.OpSnapshot {
			resp = s.receiveSnapshot(body, r)
		} else {
			resp = s.dispatch(op, body)
		}
		if resp == nil {
			continue
		}
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

func (s *Server) notLeader() []byte {
	s.mu.Lock()
	hint := s.opts.Peers[s.lead]
	s.mu.Unlock()
	e := proto.NewEncoder(proto.StatusNotLeader)
	e.Bytes([]byte(hint))
	return e.Frame()
}

var errNotLeader = errors.New("metad: not the leader")

func (s *Server) dispatch(op byte, body []byte) []byte {
	switch op {
	case proto.OpGet:
		return s.get(body)
	case proto.OpScan:
		return s.scan(body)
	case proto.OpLookup:
		return s.lookup(body)
	case proto.OpCommit:
		c, err := proto.DecodeCommit(body)
		if err != nil {
			return failure(proto.StatusError, err)
		}
		return s.submit(&request{commit: c})
	case proto.OpDeleteRange:
		d := proto.NewDecoder(body)
		id, begin, end := d.Bytes(), d.Bytes(), d.Bytes()
		if err := d.Err(); err != nil {
			return failure(proto.StatusError, err)
		}
		if len(id) != proto.TxnIDSize {
			return failure(proto.StatusError, fmt.Errorf("metad: transaction id of %d bytes", len(id)))
		}
		return s.submit(&request{commit: &proto.Commit{ID: id, Writes: []proto.Write{{Op: proto.WriteDeleteRange, Key: begin, Value: end}}}})
	case proto.OpResolve:
		d := proto.NewDecoder(body)
		id := d.Bytes()
		if err := d.Err(); err != nil {
			return failure(proto.StatusError, err)
		}
		if len(id) != proto.TxnIDSize {
			return failure(proto.StatusError, fmt.Errorf("metad: transaction id of %d bytes", len(id)))
		}
		return s.submit(&request{resolve: id})
	case proto.OpPing:
		s.mu.Lock()
		ready, seq := s.ready, s.applied
		s.mu.Unlock()
		if !ready {
			return s.notLeader()
		}
		e := proto.NewEncoder(proto.StatusOK)
		e.U64(seq)
		return e.Frame()
	case proto.OpRaft:
		s.step(body)
		return nil
	default:
		return failure(proto.StatusError, fmt.Errorf("unknown op %d", op))
	}
}

func (s *Server) acquire(seq uint64) (*snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("metad: closed")
	}
	if !s.ready {
		return nil, errNotLeader
	}
	var sn *snapshot
	if seq == 0 {
		if s.current == nil || s.current.seq != s.applied {
			s.current = &snapshot{seq: s.applied, snap: s.db.NewSnapshot()}
			s.snapshots[s.current.seq] = s.current
		}
		sn = s.current
	} else if sn = s.snapshots[seq]; sn == nil || seq < s.base {
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

func (s *Server) readFailure(err error) []byte {
	if errors.Is(err, errNotLeader) {
		return s.notLeader()
	}
	if errors.Is(err, proto.ErrSnapshotGone) {
		return failure(proto.StatusSnapshotGone, err)
	}
	return failure(proto.StatusError, err)
}

func (s *Server) get(body []byte) []byte {
	d := proto.NewDecoder(body)
	seq := d.U64()
	n := d.U32()
	keys := make([][]byte, 0, min(n, proto.ScanLimit))
	for i := uint32(0); i < n && d.Valid(); i++ {
		keys = append(keys, d.Bytes())
	}
	if err := d.Err(); err != nil {
		return failure(proto.StatusError, err)
	}
	sn, err := s.acquire(seq)
	if err != nil {
		return s.readFailure(err)
	}
	defer s.release(sn)
	e := proto.NewEncoder(proto.StatusOK)
	e.U64(sn.seq)
	for _, k := range keys {
		if err := read(sn, userKey(k), e); err != nil {
			return failure(proto.StatusError, err)
		}
	}
	return e.Frame()
}

func read(sn *snapshot, key []byte, e *proto.Encoder) error {
	v, closer, err := sn.snap.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		e.U8(0)
		return nil
	}
	if err != nil {
		return err
	}
	e.U8(1)
	e.Bytes(v)
	return closer.Close()
}

func (s *Server) lookup(body []byte) []byte {
	d := proto.NewDecoder(body)
	seq := d.U64()
	entry, head, tail := d.Bytes(), d.Bytes(), d.Bytes()
	if err := d.Err(); err != nil {
		return failure(proto.StatusError, err)
	}
	sn, err := s.acquire(seq)
	if err != nil {
		return s.readFailure(err)
	}
	defer s.release(sn)
	e := proto.NewEncoder(proto.StatusOK)
	e.U64(sn.seq)
	v, closer, err := sn.snap.Get(userKey(entry))
	if errors.Is(err, pebble.ErrNotFound) {
		e.U8(0)
		return e.Frame()
	}
	if err != nil {
		return failure(proto.StatusError, err)
	}
	e.U8(1)
	e.Bytes(v)
	target, err := proto.EntryTarget(v, head, tail)
	if cerr := closer.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = read(sn, userKey(target), e)
	}
	if err != nil {
		return failure(proto.StatusError, err)
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
		return s.readFailure(err)
	}
	defer s.release(sn)
	iopts := &pebble.IterOptions{LowerBound: userKey(begin), UpperBound: userEnd}
	if len(end) > 0 {
		iopts.UpperBound = userKey(end)
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
		keys = append(keys, append([]byte(nil), it.Key()[len(userStart):]...))
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
		return failure(proto.StatusUnknown, errors.New("metad: closed"))
	}
	switch status {
	case proto.StatusOK:
		return proto.NewEncoder(proto.StatusOK).Frame()
	case proto.StatusNotLeader:
		return s.notLeader()
	default:
		return failure(status, fmt.Errorf("commit status %d", status))
	}
}

func (s *Server) conflicts(c *proto.Commit) byte {
	if len(c.Keys) == 0 && len(c.Ranges) == 0 {
		return proto.StatusOK
	}
	if c.Snapshot < s.floor {
		return proto.StatusSnapshotGone
	}
	for _, k := range c.Keys {
		if w, ok := s.recent.Get(written{key: k}); ok && (w.pending > 0 || w.seq > c.Snapshot) {
			return proto.StatusConflict
		}
	}
	for _, r := range c.Ranges {
		hit := false
		check := func(w written) bool {
			hit = w.pending > 0 || w.seq > c.Snapshot
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

func (s *Server) mark(key []byte) {
	w, _ := s.recent.Get(written{key: key})
	w.key = key
	w.pending++
	s.recent.ReplaceOrInsert(w)
}

func (s *Server) settle(key []byte, seq uint64, marked bool) {
	w, _ := s.recent.Get(written{key: key})
	w.key = key
	if marked && w.pending > 0 {
		w.pending--
	}
	w.seq = max(w.seq, seq)
	s.recent.ReplaceOrInsert(w)
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
		s.propose(reqs)
	}
}

func (s *Server) propose(reqs []*request) {
	s.mu.Lock()
	if !s.ready {
		s.mu.Unlock()
		for _, r := range reqs {
			r.done <- proto.StatusNotLeader
		}
		return
	}
	d := &entryData{origin: s.opts.ID, term: s.term, time: max(time.Now().UnixNano(), s.lastTime+1)}
	s.lastTime = d.time
	var commits, resolves []*request
	var marked [][]byte
	for _, r := range reqs {
		if r.commit == nil {
			d.resolves = append(d.resolves, r.resolve)
			resolves = append(resolves, r)
			continue
		}
		if st := s.conflicts(r.commit); st != proto.StatusOK {
			r.done <- st
			continue
		}
		for _, w := range r.commit.Writes {
			if w.Op != proto.WriteDeleteRange {
				s.mark(w.Key)
				marked = append(marked, w.Key)
			}
		}
		d.txns = append(d.txns, txnEntry{id: r.commit.ID, writes: r.commit.Writes})
		commits = append(commits, r)
	}
	if len(commits)+len(resolves) == 0 {
		s.mu.Unlock()
		return
	}
	s.prop++
	d.prop = s.prop
	s.waiters[d.prop] = append(commits, resolves...)
	s.mu.Unlock()
	err := s.node.Propose(context.Background(), d.encode())
	if err == nil {
		return
	}
	status := byte(proto.StatusUnknown)
	if errors.Is(err, raft.ErrProposalDropped) {
		status = proto.StatusConflict
	}
	s.mu.Lock()
	waiting, ok := s.waiters[d.prop]
	delete(s.waiters, d.prop)
	if s.leader && s.term == d.term {
		seq := s.applied + 1
		for _, k := range marked {
			s.settle(k, seq, true)
		}
		s.history = append(s.history, batchKeys{seq: seq, keys: marked})
	}
	s.mu.Unlock()
	if ok {
		for _, r := range waiting {
			r.done <- status
		}
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
	floor := s.applied
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
	s.floor = max(floor, s.base)
	n := 0
	for ; n < len(s.history) && s.history[n].seq <= s.floor; n++ {
		for _, k := range s.history[n].keys {
			if w, ok := s.recent.Get(written{key: k}); ok && w.pending == 0 && w.seq <= s.floor {
				s.recent.Delete(w)
			}
		}
	}
	s.history = append(s.history[:0], s.history[n:]...)
}

type Status struct {
	ID      uint64
	Lead    uint64
	Term    uint64
	Applied uint64
	First   uint64
	Ready   bool
}

func (s *Server) Status() Status {
	first, _ := s.log.FirstIndex()
	s.mu.Lock()
	defer s.mu.Unlock()
	return Status{ID: s.opts.ID, Lead: s.lead, Term: s.term, Applied: s.applied, First: first, Ready: s.ready}
}
