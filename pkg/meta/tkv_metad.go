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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/juicedata/juicefs/pkg/metad/proto"
)

func init() {
	Register("metad", newKVMeta)
	drivers["metad"] = newMetadClient
}

type metadConn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

type metadPool struct {
	addr  string
	idle  chan *metadConn
	slots chan struct{}
}

type metadClient struct {
	pools    []*metadPool
	leader   atomic.Int32
	failover time.Duration
}

func newMetadClient(addr string) (tkvClient, error) {
	hosts, rest, _ := strings.Cut(addr, "/")
	u, err := url.Parse("metad://placeholder/" + rest)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", addr, err)
	}
	conns := 16
	if v := u.Query().Get("conns"); v != "" {
		if conns, err = strconv.Atoi(v); err != nil || conns <= 0 {
			return nil, fmt.Errorf("invalid conns %q", v)
		}
	}
	c := &metadClient{failover: 30 * time.Second}
	if v := u.Query().Get("failover"); v != "" {
		if c.failover, err = time.ParseDuration(v); err != nil || c.failover <= 0 {
			return nil, fmt.Errorf("invalid failover %q", v)
		}
	}
	for _, h := range strings.Split(hosts, ",") {
		if h == "" {
			return nil, fmt.Errorf("empty address in %s", addr)
		}
		c.pools = append(c.pools, &metadPool{addr: h, idle: make(chan *metadConn, conns), slots: make(chan struct{}, conns)})
	}
	if _, _, err := c.call(proto.NewEncoder(proto.OpPing).Frame(), true); err != nil {
		return nil, fmt.Errorf("ping metad %s: %s", hosts, err)
	}
	if u.Path == "" || u.Path == "/" {
		return c, nil
	}
	return withPrefix(c, []byte(u.Path+"ý")), nil
}

func (p *metadPool) conn() (*metadConn, error) {
	select {
	case cn := <-p.idle:
		return cn, nil
	default:
	}
	select {
	case cn := <-p.idle:
		return cn, nil
	case p.slots <- struct{}{}:
	}
	nc, err := net.DialTimeout("tcp", p.addr, time.Second)
	if err != nil {
		<-p.slots
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &metadConn{nc, bufio.NewReaderSize(nc, 64<<10), bufio.NewWriterSize(nc, 64<<10)}, nil
}

func (p *metadPool) discard(cn *metadConn) {
	_ = cn.c.Close()
	<-p.slots
}

func (p *metadPool) roundTrip(frame []byte) (byte, []byte, bool, error) {
	cn, err := p.conn()
	if err != nil {
		return 0, nil, false, err
	}
	if _, err = cn.w.Write(frame); err == nil {
		err = cn.w.Flush()
	}
	var status byte
	var body []byte
	if err == nil {
		status, body, err = proto.ReadFrame(cn.r)
	}
	if err != nil {
		p.discard(cn)
		return 0, nil, true, err
	}
	p.idle <- cn
	return status, body, true, nil
}

func (c *metadClient) follow(from int32, hint string) {
	next := (from + 1) % int32(len(c.pools))
	for i, p := range c.pools {
		if p.addr == hint {
			next = int32(i)
		}
	}
	c.leader.CompareAndSwap(from, next)
}

func (c *metadClient) call(frame []byte, safe bool) (byte, []byte, error) {
	deadline := time.Now().Add(c.failover)
	var last error
	for tries := 0; ; tries++ {
		if tries > 0 {
			if time.Now().After(deadline) {
				return 0, nil, fmt.Errorf("metad: no leader within %s: %w", c.failover, last)
			}
			if tries%len(c.pools) == 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}
		i := c.leader.Load()
		status, body, sent, err := c.pools[i].roundTrip(frame)
		if err != nil {
			if sent && !safe {
				return 0, nil, fmt.Errorf("%w: %s", proto.ErrUnknown, err)
			}
			last = err
			c.follow(i, "")
			continue
		}
		switch status {
		case proto.StatusOK:
			return status, body, nil
		case proto.StatusNotLeader:
			d := proto.NewDecoder(body)
			hint := string(d.Bytes())
			last = fmt.Errorf("metad: %s is not the leader", c.pools[i].addr)
			c.follow(i, hint)
			continue
		case proto.StatusConflict:
			return status, nil, proto.ErrConflict
		case proto.StatusSnapshotGone:
			return status, nil, proto.ErrSnapshotGone
		case proto.StatusUnknown:
			return status, nil, proto.ErrUnknown
		default:
			d := proto.NewDecoder(body)
			return status, nil, fmt.Errorf("metad: %s", d.Bytes())
		}
	}
}

func newTxnID() []byte {
	id := make([]byte, proto.TxnIDSize)
	if _, err := rand.Read(id); err != nil {
		panic(err)
	}
	return id
}

func (c *metadClient) commit(id, frame []byte) error {
	_, _, err := c.call(frame, false)
	if !errors.Is(err, proto.ErrUnknown) {
		return err
	}
	e := proto.NewEncoder(proto.OpResolve)
	e.Bytes(id)
	resolve := e.Frame()
	deadline := time.Now().Add(c.failover)
	for {
		_, _, rerr := c.call(resolve, true)
		if rerr == nil || errors.Is(rerr, proto.ErrConflict) {
			return rerr
		}
		if !errors.Is(rerr, proto.ErrUnknown) || time.Now().After(deadline) {
			return fmt.Errorf("metad: transaction %x: %w after %s, resolve: %s", id, proto.ErrUnknown, err, rerr)
		}
	}
}

type metadPage struct {
	snapshot uint64
	keys     [][]byte
	values   [][]byte
	more     bool
}

func (c *metadClient) scanPage(snapshot uint64, begin, end []byte, keysOnly bool) (*metadPage, error) {
	e := proto.NewEncoder(proto.OpScan)
	e.U64(snapshot)
	e.Bytes(begin)
	e.Bytes(end)
	if keysOnly {
		e.U8(1)
	} else {
		e.U8(0)
	}
	e.U32(proto.ScanLimit)
	_, body, err := c.call(e.Frame(), true)
	if err != nil {
		return nil, err
	}
	d := proto.NewDecoder(body)
	p := &metadPage{snapshot: d.U64(), more: d.U8() == 1}
	n := d.U32()
	for i := uint32(0); i < n && d.Valid(); i++ {
		p.keys = append(p.keys, d.Bytes())
		p.values = append(p.values, d.Bytes())
	}
	return p, d.Err()
}

func (c *metadClient) name() string { return "metad" }

func (c *metadClient) shouldRetry(err error) bool {
	return errors.Is(err, proto.ErrConflict) || errors.Is(err, proto.ErrSnapshotGone)
}

func (c *metadClient) config(key string) interface{} { return nil }

func (c *metadClient) gc() {}

func (c *metadClient) rewind(id uint64, factor int) uint64 { return 0 }

func (c *metadClient) simpleTxn(ctx context.Context, f func(*kvTxn) error, retry int) error {
	return c.txn(ctx, f, retry)
}

func (c *metadClient) txn(ctx context.Context, f func(*kvTxn) error, retry int) (err error) {
	tx := &metadTxn{c: c, read: make(map[string]struct{}), buffer: make(map[string]metadWrite)}
	defer func() {
		if r := recover(); r != nil {
			fe, ok := r.(error)
			if !ok {
				panic(r)
			}
			err = fe
		}
	}()
	if err = f(&kvTxn{kvtxn: tx, retry: retry}); err != nil {
		return err
	}
	if len(tx.buffer) == 0 {
		return nil
	}
	return tx.commit()
}

func (c *metadClient) scan(prefix []byte, handler func(key, value []byte) bool) error {
	begin, end := prefix, nextKey(prefix)
	var snapshot uint64
	for {
		p, err := c.scanPage(snapshot, begin, end, false)
		if err != nil {
			return err
		}
		snapshot = p.snapshot
		for i, k := range p.keys {
			if !handler(k, p.values[i]) {
				return nil
			}
		}
		if !p.more {
			return nil
		}
		begin = append(p.keys[len(p.keys)-1], 0)
	}
}

func (c *metadClient) reset(prefix []byte) error {
	for {
		id := newTxnID()
		e := proto.NewEncoder(proto.OpDeleteRange)
		e.Bytes(id)
		e.Bytes(prefix)
		e.Bytes(nextKey(prefix))
		if err := c.commit(id, e.Frame()); !errors.Is(err, proto.ErrConflict) {
			return err
		}
	}
}

func (c *metadClient) close() error {
	for _, p := range c.pools {
	drain:
		for {
			select {
			case cn := <-p.idle:
				p.discard(cn)
			default:
				break drain
			}
		}
	}
	return nil
}

type metadWrite struct {
	value   []byte
	deleted bool
}

type metadTxn struct {
	c        *metadClient
	snapshot uint64
	read     map[string]struct{}
	ranges   []proto.Range
	buffer   map[string]metadWrite
}

func (tx *metadTxn) get(key []byte) []byte {
	return tx.gets(key)[0]
}

func (tx *metadTxn) gets(keys ...[]byte) [][]byte {
	values := make([][]byte, len(keys))
	var missing []int
	for i, k := range keys {
		if w, ok := tx.buffer[string(k)]; ok {
			if !w.deleted {
				values[i] = w.value
			}
			continue
		}
		missing = append(missing, i)
	}
	for len(missing) > 0 {
		n := min(len(missing), proto.ScanLimit)
		e := proto.NewEncoder(proto.OpGet)
		e.U64(tx.snapshot)
		e.U32(uint32(n))
		for _, i := range missing[:n] {
			e.Bytes(keys[i])
		}
		_, body, err := tx.c.call(e.Frame(), true)
		if err != nil {
			panic(err)
		}
		d := proto.NewDecoder(body)
		tx.snapshot = d.U64()
		for _, i := range missing[:n] {
			if d.U8() == 1 {
				values[i] = d.Bytes()
			}
			tx.read[string(keys[i])] = struct{}{}
		}
		if err := d.Err(); err != nil {
			panic(err)
		}
		missing = missing[n:]
	}
	return values
}

func (tx *metadTxn) lookup(entry, head, tail []byte) ([]byte, []byte, bool) {
	if _, ok := tx.buffer[string(entry)]; ok {
		v := tx.get(entry)
		if v == nil {
			return nil, nil, true
		}
		target, err := proto.EntryTarget(v, head, tail)
		if err != nil {
			panic(err)
		}
		return v, tx.get(target), true
	}
	e := proto.NewEncoder(proto.OpLookup)
	e.U64(tx.snapshot)
	e.Bytes(entry)
	e.Bytes(head)
	e.Bytes(tail)
	_, body, err := tx.c.call(e.Frame(), true)
	if err != nil {
		panic(err)
	}
	d := proto.NewDecoder(body)
	tx.snapshot = d.U64()
	tx.read[string(entry)] = struct{}{}
	var v, a []byte
	if d.U8() == 1 {
		v = d.Bytes()
		if d.U8() == 1 {
			a = d.Bytes()
		}
	}
	if err := d.Err(); err != nil {
		panic(err)
	}
	if v == nil {
		return nil, nil, true
	}
	target, err := proto.EntryTarget(v, head, tail)
	if err != nil {
		panic(err)
	}
	tx.read[string(target)] = struct{}{}
	if w, ok := tx.buffer[string(target)]; ok {
		a = nil
		if !w.deleted {
			a = w.value
		}
	}
	return v, a, true
}

func (tx *metadTxn) buffered(begin, end []byte) []string {
	var keys []string
	for k := range tx.buffer {
		if k >= string(begin) && (len(end) == 0 || k < string(end)) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys
}

func (tx *metadTxn) scan(begin, end []byte, keysOnly bool, handler func(k, v []byte) bool) {
	local := tx.buffered(begin, end)
	emit := func(k, v []byte) bool {
		if handler(k, v) {
			return true
		}
		tx.ranges = append(tx.ranges, proto.Range{Begin: begin, End: append(append([]byte(nil), k...), 0)})
		return false
	}
	flush := func(upto []byte, last bool) bool {
		for len(local) > 0 && (last || local[0] < string(upto)) {
			w := tx.buffer[local[0]]
			k := []byte(local[0])
			local = local[1:]
			if !w.deleted && !emit(k, w.value) {
				return false
			}
		}
		return true
	}
	start := begin
	for {
		p, err := tx.c.scanPage(tx.snapshot, start, end, keysOnly)
		if err != nil {
			panic(err)
		}
		tx.snapshot = p.snapshot
		for i, k := range p.keys {
			if !flush(k, false) {
				return
			}
			if len(local) > 0 && local[0] == string(k) {
				w := tx.buffer[local[0]]
				local = local[1:]
				if !w.deleted && !emit(k, w.value) {
					return
				}
				continue
			}
			if !emit(k, p.values[i]) {
				return
			}
		}
		if !p.more {
			break
		}
		start = append(p.keys[len(p.keys)-1], 0)
	}
	if flush(nil, true) {
		tx.ranges = append(tx.ranges, proto.Range{Begin: begin, End: end})
	}
}

func (tx *metadTxn) exist(prefix []byte) bool {
	var found bool
	tx.scan(prefix, nextKey(prefix), true, func(k, v []byte) bool {
		found = true
		return false
	})
	return found
}

func (tx *metadTxn) set(key, value []byte) {
	tx.buffer[string(key)] = metadWrite{value: value}
}

func (tx *metadTxn) append(key []byte, value []byte) {
	tx.set(key, append(tx.get(key), value...))
}

func (tx *metadTxn) incrBy(key []byte, value int64) int64 {
	n := parseCounter(tx.get(key))
	if value != 0 {
		n += value
		tx.set(key, packCounter(n))
	}
	return n
}

func (tx *metadTxn) delete(key []byte) {
	tx.buffer[string(key)] = metadWrite{deleted: true}
}

func (tx *metadTxn) id() uint64 { return 0 }

func (tx *metadTxn) commit() error {
	c := &proto.Commit{ID: newTxnID(), Snapshot: tx.snapshot, Ranges: tx.ranges}
	for k := range tx.read {
		c.Keys = append(c.Keys, []byte(k))
	}
	keys := make([]string, 0, len(tx.buffer))
	for k := range tx.buffer {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w := tx.buffer[k]
		if w.deleted {
			c.Writes = append(c.Writes, proto.Write{Op: proto.WriteDelete, Key: []byte(k)})
		} else {
			c.Writes = append(c.Writes, proto.Write{Op: proto.WriteSet, Key: []byte(k), Value: w.value})
		}
	}
	return tx.c.commit(c.ID, proto.EncodeCommit(c))
}
