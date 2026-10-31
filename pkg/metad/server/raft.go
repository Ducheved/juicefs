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
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/juicedata/juicefs/pkg/metad/proto"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	outcomeCommitted byte = 1
	outcomeAborted   byte = 2
)

type txnEntry struct {
	id     []byte
	writes []proto.Write
}

type entryData struct {
	origin   uint64
	term     uint64
	prop     uint64
	time     int64
	txns     []txnEntry
	resolves [][]byte
}

func (d *entryData) encode() []byte {
	e := proto.NewEncoder(0)
	e.U64(d.origin)
	e.U64(d.term)
	e.U64(d.prop)
	e.U64(uint64(d.time))
	e.U32(uint32(len(d.txns)))
	for _, t := range d.txns {
		e.Bytes(t.id)
		proto.EncodeWrites(e, t.writes)
	}
	e.U32(uint32(len(d.resolves)))
	for _, id := range d.resolves {
		e.Bytes(id)
	}
	return e.Payload()
}

func decodeEntry(b []byte) (*entryData, error) {
	d := proto.NewDecoder(b)
	e := &entryData{origin: d.U64(), term: d.U64(), prop: d.U64(), time: int64(d.U64())}
	n := d.U32()
	for i := uint32(0); i < n && d.Valid(); i++ {
		t := txnEntry{id: d.Bytes()}
		ws, err := proto.DecodeWrites(d)
		if err != nil {
			return nil, err
		}
		t.writes = ws
		e.txns = append(e.txns, t)
	}
	n = d.U32()
	for i := uint32(0); i < n && d.Valid(); i++ {
		e.resolves = append(e.resolves, d.Bytes())
	}
	return e, d.Err()
}

type proposalDone struct {
	origin   uint64
	term     uint64
	prop     uint64
	index    uint64
	statuses []byte
	keys     [][]byte
}

type applyResult struct {
	index uint64
	time  int64
	noops []uint64
	done  []proposalDone
}

func (s *Server) start() error {
	log, err := openLog(s.db)
	if err != nil {
		return err
	}
	s.log = log
	if err := s.resumeInstall(); err != nil {
		return err
	}
	if s.applied, err = readU64(s.db, keyApplied); err != nil {
		return err
	}
	if err := s.cleanStaging(); err != nil {
		return err
	}
	last, err := readU64(s.db, keyLastTime)
	if err != nil {
		return err
	}
	s.lastTime = int64(last)
	cfg := &raft.Config{
		ID:                        s.opts.ID,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   s.log,
		Applied:                   s.applied,
		MaxSizePerMsg:             1 << 20,
		MaxCommittedSizePerReady:  16 << 20,
		MaxUncommittedEntriesSize: 64 << 20,
		MaxInflightMsgs:           256,
		CheckQuorum:               true,
		PreVote:                   true,
		DisableProposalForwarding: true,
		Logger:                    logger,
	}
	ids := make([]uint64, 0, len(s.opts.Peers))
	for id, addr := range s.opts.Peers {
		ids = append(ids, id)
		if id != s.opts.ID {
			s.peers[id] = &peer{id: id, addr: addr, msgs: make(chan raftpb.Message, 4096)}
		}
	}
	if s.log.empty() {
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		peers := make([]raft.Peer, len(ids))
		for i, id := range ids {
			peers[i] = raft.Peer{ID: id}
		}
		s.node = raft.StartNode(cfg, peers)
	} else {
		s.node = raft.RestartNode(cfg)
	}
	for _, p := range s.peers {
		s.wg.Add(1)
		go s.sendLoop(p)
	}
	s.wg.Add(3)
	go s.run()
	go s.commitLoop()
	go s.expireLoop()
	return nil
}

func (s *Server) run() {
	defer s.wg.Done()
	t := time.NewTicker(s.opts.Tick)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.node.Tick()
		case rd := <-s.node.Ready():
			if err := s.handleReady(rd); err != nil {
				logger.Fatalf("metad: node %d: %s", s.opts.ID, err)
			}
		case <-s.stop:
			return
		}
	}
}

func (s *Server) handleReady(rd raft.Ready) error {
	if rd.SoftState != nil {
		s.setRole(rd.SoftState)
	}
	s.mu.Lock()
	if !raft.IsEmptyHardState(rd.HardState) {
		s.term = rd.HardState.Term
	}
	leader := s.leader
	s.mu.Unlock()
	if leader {
		s.send(rd.Messages)
	}
	s.applyMu.Lock()
	b := s.db.NewIndexedBatch()
	var posts []func()
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := s.installStaged(rd.Snapshot.Metadata, true); err != nil {
			s.applyMu.Unlock()
			return fmt.Errorf("install snapshot %d: %s", rd.Snapshot.Metadata.Index, err)
		}
		logger.Infof("metad: node %d installed snapshot %d", s.opts.ID, rd.Snapshot.Metadata.Index)
	}
	post, err := s.log.append(b, rd.Entries)
	if err != nil {
		s.applyMu.Unlock()
		return err
	}
	posts = append(posts, post)
	if !raft.IsEmptyHardState(rd.HardState) {
		post, err := s.log.setHardState(b, rd.HardState)
		if err != nil {
			s.applyMu.Unlock()
			return err
		}
		posts = append(posts, post)
	}
	res, err := s.apply(b, rd.CommittedEntries, &posts)
	if err != nil {
		s.applyMu.Unlock()
		return err
	}
	opt := pebble.NoSync
	if rd.MustSync || !raft.IsEmptySnap(rd.Snapshot) {
		opt = pebble.Sync
	}
	err = b.Commit(opt)
	_ = b.Close()
	if err != nil {
		s.applyMu.Unlock()
		return err
	}
	for _, p := range posts {
		p()
	}
	s.applyMu.Unlock()
	s.finish(res)
	if !leader {
		s.send(rd.Messages)
	}
	s.node.Advance()
	return s.maybeCompact()
}

func (s *Server) apply(b *pebble.Batch, ents []raftpb.Entry, posts *[]func()) (*applyResult, error) {
	res := &applyResult{}
	s.mu.Lock()
	applied := s.applied
	s.mu.Unlock()
	for _, e := range ents {
		if e.Index <= applied {
			continue
		}
		switch e.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(e.Data); err != nil {
				return nil, err
			}
			post, err := s.log.setConfState(b, *s.node.ApplyConfChange(cc))
			if err != nil {
				return nil, err
			}
			*posts = append(*posts, post)
		case raftpb.EntryConfChangeV2:
			var cc raftpb.ConfChangeV2
			if err := cc.Unmarshal(e.Data); err != nil {
				return nil, err
			}
			post, err := s.log.setConfState(b, *s.node.ApplyConfChange(cc))
			if err != nil {
				return nil, err
			}
			*posts = append(*posts, post)
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				res.noops = append(res.noops, e.Term)
				break
			}
			d, err := decodeEntry(e.Data)
			if err != nil {
				return nil, fmt.Errorf("entry %d: %s", e.Index, err)
			}
			if err := s.applyEntry(b, e, d, res); err != nil {
				return nil, fmt.Errorf("entry %d: %s", e.Index, err)
			}
		}
		res.index = e.Index
	}
	if res.index == 0 {
		return res, nil
	}
	if err := b.Set(keyApplied, u64(res.index), nil); err != nil {
		return nil, err
	}
	if res.time > 0 {
		if err := b.Set(keyLastTime, u64(uint64(res.time)), nil); err != nil {
			return nil, err
		}
	}
	return res, nil
}

func txnKey(id []byte) []byte {
	return append(append([]byte(nil), prefixTxn...), id...)
}

func expiryKey(t int64, id []byte) []byte {
	k := binary.BigEndian.AppendUint64(append([]byte(nil), prefixExpiry...), uint64(t))
	return append(k, id...)
}

func outcome(b *pebble.Batch, id []byte) (byte, error) {
	v, closer, err := b.Get(txnKey(id))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(v) != 9 {
		return 0, fmt.Errorf("transaction record of %d bytes", len(v))
	}
	return v[0], nil
}

func record(b *pebble.Batch, id []byte, o byte, t int64) error {
	if err := b.Set(txnKey(id), binary.BigEndian.AppendUint64([]byte{o}, uint64(t)), nil); err != nil {
		return err
	}
	return b.Set(expiryKey(t, id), nil, nil)
}

func (s *Server) applyEntry(b *pebble.Batch, e raftpb.Entry, d *entryData, res *applyResult) error {
	done := proposalDone{origin: d.origin, term: d.term, prop: d.prop, index: e.Index}
	for _, t := range d.txns {
		prev, err := outcome(b, t.id)
		if err != nil {
			return err
		}
		status := proto.StatusConflict
		switch {
		case prev == outcomeCommitted:
			status = proto.StatusOK
		case prev == outcomeAborted:
		case d.term != e.Term:
			if err := record(b, t.id, outcomeAborted, d.time); err != nil {
				return err
			}
		default:
			if err := applyWrites(b, t.writes); err != nil {
				return err
			}
			if err := record(b, t.id, outcomeCommitted, d.time); err != nil {
				return err
			}
			status = proto.StatusOK
		}
		done.statuses = append(done.statuses, status)
		for _, w := range t.writes {
			if w.Op != proto.WriteDeleteRange {
				done.keys = append(done.keys, w.Key)
			}
		}
	}
	for _, id := range d.resolves {
		prev, err := outcome(b, id)
		if err != nil {
			return err
		}
		if prev == 0 {
			if err := record(b, id, outcomeAborted, d.time); err != nil {
				return err
			}
			prev = outcomeAborted
		}
		if prev == outcomeCommitted {
			done.statuses = append(done.statuses, proto.StatusOK)
		} else {
			done.statuses = append(done.statuses, proto.StatusConflict)
		}
	}
	if err := s.expireTxns(b, d.time); err != nil {
		return err
	}
	res.time = d.time
	res.done = append(res.done, done)
	return nil
}

func applyWrites(b *pebble.Batch, ws []proto.Write) error {
	for _, w := range ws {
		var err error
		switch w.Op {
		case proto.WriteSet:
			err = b.Set(userKey(w.Key), w.Value, nil)
		case proto.WriteDelete:
			err = b.Delete(userKey(w.Key), nil)
		case proto.WriteDeleteRange:
			end := userEnd
			if len(w.Value) > 0 {
				end = userKey(w.Value)
			}
			err = b.DeleteRange(userKey(w.Key), end, nil)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) expireTxns(b *pebble.Batch, now int64) error {
	edge := (now - s.opts.DedupWindow.Nanoseconds()) / int64(time.Second) * int64(time.Second)
	if edge <= 0 {
		return nil
	}
	lower, upper := s.expired, expiryKey(edge, nil)
	if lower == nil {
		lower = prefixExpiry
	}
	if bytes.Compare(lower, upper) >= 0 {
		return nil
	}
	it, err := b.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	var keys [][]byte
	for it.First(); it.Valid(); it.Next() {
		keys = append(keys, append([]byte(nil), it.Key()...))
	}
	err = it.Error()
	if cerr := it.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := b.Delete(k, nil); err != nil {
			return err
		}
		if err := b.Delete(txnKey(k[len(prefixExpiry)+8:]), nil); err != nil {
			return err
		}
	}
	s.expired = upper
	return nil
}

type reply struct {
	reqs     []*request
	statuses []byte
}

func (s *Server) finish(res *applyResult) {
	var replies []reply
	s.mu.Lock()
	if res.index > 0 {
		s.applied = res.index
	}
	s.lastTime = max(s.lastTime, res.time)
	for _, d := range res.done {
		mine := d.origin == s.opts.ID
		if s.leader {
			marked := mine && d.term == s.term
			for _, k := range d.keys {
				s.settle(k, d.index, marked)
			}
			s.history = append(s.history, batchKeys{seq: d.index, keys: d.keys})
		}
		if !mine {
			continue
		}
		if reqs, ok := s.waiters[d.prop]; ok {
			delete(s.waiters, d.prop)
			replies = append(replies, reply{reqs, d.statuses})
		}
	}
	for _, term := range res.noops {
		if s.leader && !s.ready && term == s.term {
			s.ready = true
			s.base = s.applied
			s.floor = s.applied
			s.recent.Clear(false)
			s.history = nil
			s.current = nil
			logger.Infof("metad: node %d leads term %d from index %d", s.opts.ID, s.term, s.applied)
		}
	}
	s.mu.Unlock()
	for _, r := range replies {
		for i, req := range r.reqs {
			req.done <- r.statuses[i]
		}
	}
}

func (s *Server) setRole(ss *raft.SoftState) {
	var failed []*request
	s.mu.Lock()
	s.lead = ss.Lead
	leader := ss.RaftState == raft.StateLeader
	if !leader {
		s.ready = false
		for prop, reqs := range s.waiters {
			failed = append(failed, reqs...)
			delete(s.waiters, prop)
		}
		s.recent.Clear(false)
		s.history = nil
		s.current = nil
	}
	s.leader = leader
	s.mu.Unlock()
	for _, r := range failed {
		r.done <- proto.StatusUnknown
	}
}

func (s *Server) maybeCompact() error {
	s.mu.Lock()
	applied := s.applied
	s.mu.Unlock()
	first, _ := s.log.FirstIndex()
	if applied <= s.opts.LogRetain {
		return nil
	}
	upto := applied - s.opts.LogRetain
	if upto < first+s.opts.LogRetain/8 {
		return nil
	}
	return s.log.compact(upto)
}

func (s *Server) step(body []byte) {
	d := proto.NewDecoder(body)
	var m raftpb.Message
	if err := m.Unmarshal(d.Bytes()); err != nil || d.Err() != nil {
		logger.Warnf("metad: bad raft message: %v %v", err, d.Err())
		return
	}
	if err := s.node.Step(context.Background(), m); err != nil && !errors.Is(err, raft.ErrStopped) {
		logger.Debugf("metad: step %s from %d: %s", m.Type, m.From, err)
	}
}
