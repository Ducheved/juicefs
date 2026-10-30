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
	"errors"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
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

type metadClient struct {
	addr  string
	idle  chan *metadConn
	slots chan struct{}
}

func newMetadClient(addr string) (tkvClient, error) {
	u, err := url.Parse("metad://" + addr)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %s", addr, err)
	}
	conns := 16
	if v := u.Query().Get("conns"); v != "" {
		if conns, err = strconv.Atoi(v); err != nil || conns <= 0 {
			return nil, fmt.Errorf("invalid conns %q", v)
		}
	}
	c := &metadClient{addr: u.Host, idle: make(chan *metadConn, conns), slots: make(chan struct{}, conns)}
	if _, _, err := c.call(proto.NewEncoder(proto.OpPing).Frame()); err != nil {
		return nil, fmt.Errorf("ping metad %s: %s", u.Host, err)
	}
	if u.Path == "" || u.Path == "/" {
		return c, nil
	}
	return withPrefix(c, []byte(u.Path+"\xFD")), nil
}

func (c *metadClient) conn() (*metadConn, error) {
	select {
	case cn := <-c.idle:
		return cn, nil
	default:
	}
	select {
	case cn := <-c.idle:
		return cn, nil
	case c.slots <- struct{}{}:
	}
	nc, err := net.DialTimeout("tcp", c.addr, 5*time.Second)
	if err != nil {
		<-c.slots
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return &metadConn{nc, bufio.NewReaderSize(nc, 64<<10), bufio.NewWriterSize(nc, 64<<10)}, nil
}

func (c *metadClient) discard(cn *metadConn) {
	_ = cn.c.Close()
	<-c.slots
}

func (c *metadClient) call(frame []byte) (byte, []byte, error) {
	cn, err := c.conn()
	if err != nil {
		return 0, nil, err
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
		c.discard(cn)
		return 0, nil, err
	}
	c.idle <- cn
	switch status {
	case proto.StatusOK:
		return status, body, nil
	case proto.StatusConflict:
		return status, nil, proto.ErrConflict
	case proto.StatusSnapshotGone:
		return status, nil, proto.ErrSnapshotGone
	default:
		d := proto.NewDecoder(body)
		return status, nil, fmt.Errorf("metad: %s", d.Bytes())
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
	_, body, err := c.call(e.Frame())
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
	e := proto.NewEncoder(proto.OpDeleteRange)
	e.Bytes(prefix)
	e.Bytes(nextKey(prefix))
	_, _, err := c.call(e.Frame())
	return err
}

func (c *metadClient) close() error {
	for {
		select {
		case cn := <-c.idle:
			c.discard(cn)
		default:
			return nil
		}
	}
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
		_, body, err := tx.c.call(e.Frame())
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
	c := &proto.Commit{Snapshot: tx.snapshot, Ranges: tx.ranges}
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
	_, _, err := tx.c.call(proto.EncodeCommit(c))
	return err
}
