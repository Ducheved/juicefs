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
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

var (
	keyHardState = []byte{0, 'H'}
	keyConfState = []byte{0, 'C'}
	keySnapMeta  = []byte{0, 'S'}
	keyApplied   = []byte{0, 'A'}
	keyInstall   = []byte{0, 'I'}
	keyLastTime  = []byte{0, 'M'}
	prefixLog    = []byte{0, 'L'}
	prefixTxn    = []byte{0, 'T'}
	prefixExpiry = []byte{0, 'X'}
	stateStart   = []byte{0, 'T'}
	stateEnd     = []byte{0, 'Y'}
	userStart    = []byte{1}
	userEnd      = []byte{2}
)

func u64(v uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, v)
}

func logKey(i uint64) []byte {
	return binary.BigEndian.AppendUint64(append([]byte(nil), prefixLog...), i)
}

func userKey(k []byte) []byte {
	return append(append(make([]byte, 0, len(k)+1), userStart...), k...)
}

func readU64(db pebble.Reader, key []byte) (uint64, error) {
	v, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	if len(v) != 8 {
		return 0, fmt.Errorf("metad: %q holds %d bytes", key, len(v))
	}
	return binary.BigEndian.Uint64(v), nil
}

func readProto(db pebble.Reader, key []byte, m interface{ Unmarshal([]byte) error }) (bool, error) {
	v, closer, err := db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer closer.Close()
	return true, m.Unmarshal(v)
}

func setProto(b *pebble.Batch, key []byte, m interface{ Marshal() ([]byte, error) }) error {
	v, err := m.Marshal()
	if err != nil {
		return err
	}
	return b.Set(key, v, nil)
}

type logStore struct {
	db *pebble.DB

	mu    sync.RWMutex
	first uint64
	last  uint64
	snap  raftpb.Snapshot
	hard  raftpb.HardState
	conf  raftpb.ConfState
}

func openLog(db *pebble.DB) (*logStore, error) {
	l := &logStore{db: db}
	if _, err := readProto(db, keyHardState, &l.hard); err != nil {
		return nil, err
	}
	if _, err := readProto(db, keyConfState, &l.conf); err != nil {
		return nil, err
	}
	if _, err := readProto(db, keySnapMeta, &l.snap.Metadata); err != nil {
		return nil, err
	}
	l.first = l.snap.Metadata.Index + 1
	l.last = l.snap.Metadata.Index
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: logKey(0), UpperBound: logKey(math.MaxUint64)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if it.Last() {
		l.last = binary.BigEndian.Uint64(it.Key()[len(prefixLog):])
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if l.last < l.snap.Metadata.Index {
		return nil, fmt.Errorf("metad: log ends at %d before snapshot %d", l.last, l.snap.Metadata.Index)
	}
	return l, nil
}

func (l *logStore) empty() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return raft.IsEmptyHardState(l.hard) && l.last == 0
}

func (l *logStore) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.hard, l.conf, nil
}

func (l *logStore) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if lo < l.first {
		return nil, raft.ErrCompacted
	}
	if hi > l.last+1 {
		return nil, fmt.Errorf("metad: entries [%d, %d) beyond last index %d", lo, hi, l.last)
	}
	it, err := l.db.NewIter(&pebble.IterOptions{LowerBound: logKey(lo), UpperBound: logKey(hi)})
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var ents []raftpb.Entry
	var size uint64
	next := lo
	for it.First(); it.Valid(); it.Next() {
		v, err := it.ValueAndErr()
		if err != nil {
			return nil, err
		}
		var e raftpb.Entry
		if err := e.Unmarshal(v[8:]); err != nil {
			return nil, err
		}
		if e.Index != next {
			return nil, raft.ErrUnavailable
		}
		size += uint64(e.Size())
		if len(ents) > 0 && size > maxSize {
			break
		}
		ents = append(ents, e)
		next++
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if len(ents) == 0 {
		return nil, raft.ErrUnavailable
	}
	return ents, nil
}

func (l *logStore) Term(i uint64) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if i == l.snap.Metadata.Index {
		return l.snap.Metadata.Term, nil
	}
	if i < l.first {
		return 0, raft.ErrCompacted
	}
	if i > l.last {
		return 0, raft.ErrUnavailable
	}
	v, closer, err := l.db.Get(logKey(i))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, raft.ErrUnavailable
	}
	if err != nil {
		return 0, err
	}
	defer closer.Close()
	return binary.BigEndian.Uint64(v), nil
}

func (l *logStore) LastIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.last, nil
}

func (l *logStore) FirstIndex() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.first, nil
}

func (l *logStore) Snapshot() (raftpb.Snapshot, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.snap, nil
}

func (l *logStore) append(b *pebble.Batch, ents []raftpb.Entry) (func(), error) {
	if len(ents) == 0 {
		return func() {}, nil
	}
	l.mu.RLock()
	last := l.last
	l.mu.RUnlock()
	if f := ents[0].Index; f <= last {
		if err := b.DeleteRange(logKey(f), logKey(last+1), nil); err != nil {
			return nil, err
		}
	}
	for i := range ents {
		v, err := ents[i].Marshal()
		if err != nil {
			return nil, err
		}
		if err := b.Set(logKey(ents[i].Index), append(u64(ents[i].Term), v...), nil); err != nil {
			return nil, err
		}
	}
	end := ents[len(ents)-1].Index
	return func() {
		l.mu.Lock()
		l.last = end
		l.mu.Unlock()
	}, nil
}

func (l *logStore) setHardState(b *pebble.Batch, hs raftpb.HardState) (func(), error) {
	if err := setProto(b, keyHardState, &hs); err != nil {
		return nil, err
	}
	return func() {
		l.mu.Lock()
		l.hard = hs
		l.mu.Unlock()
	}, nil
}

func (l *logStore) setConfState(b *pebble.Batch, cs raftpb.ConfState) (func(), error) {
	if err := setProto(b, keyConfState, &cs); err != nil {
		return nil, err
	}
	return func() {
		l.mu.Lock()
		l.conf = cs
		l.mu.Unlock()
	}, nil
}

func (l *logStore) resetTo(b *pebble.Batch, meta raftpb.SnapshotMetadata) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	hs := l.hard
	hs.Commit = max(hs.Commit, meta.Index)
	if err := b.DeleteRange(logKey(0), logKey(math.MaxUint64), nil); err != nil {
		return err
	}
	if err := setProto(b, keySnapMeta, &meta); err != nil {
		return err
	}
	if err := setProto(b, keyConfState, &meta.ConfState); err != nil {
		return err
	}
	if err := setProto(b, keyHardState, &hs); err != nil {
		return err
	}
	l.snap = raftpb.Snapshot{Metadata: meta}
	l.conf = meta.ConfState
	l.hard = hs
	l.first = meta.Index + 1
	l.last = meta.Index
	return nil
}

func (l *logStore) compact(index uint64) error {
	term, err := l.Term(index)
	if err != nil {
		return err
	}
	l.mu.Lock()
	if index < l.first || index > l.last {
		l.mu.Unlock()
		return nil
	}
	meta := raftpb.SnapshotMetadata{Index: index, Term: term, ConfState: l.conf}
	from := l.first
	l.first = index + 1
	l.snap = raftpb.Snapshot{Metadata: meta}
	l.mu.Unlock()
	b := l.db.NewBatch()
	defer b.Close()
	if err := setProto(b, keySnapMeta, &meta); err != nil {
		return err
	}
	if err := b.DeleteRange(logKey(from), logKey(index+1), nil); err != nil {
		return err
	}
	return b.Commit(pebble.NoSync)
}
