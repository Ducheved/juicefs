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

package proto

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	OpGet byte = iota + 1
	OpScan
	OpCommit
	OpDeleteRange
	OpPing
	OpLookup
)

const (
	StatusOK byte = iota
	StatusConflict
	StatusSnapshotGone
	StatusError
)

const MaxFrame = 256 << 20

const ScanLimit = 1024

const (
	WriteSet byte = iota + 1
	WriteDelete
)

var ErrConflict = errors.New("metad: conflicting transaction")
var ErrSnapshotGone = errors.New("metad: snapshot expired")

type Range struct {
	Begin, End []byte
}

type Write struct {
	Op    byte
	Key   []byte
	Value []byte
}

type Commit struct {
	Snapshot uint64
	Keys     [][]byte
	Ranges   []Range
	Writes   []Write
}

type Encoder struct {
	buf []byte
}

func NewEncoder(op byte) *Encoder {
	return &Encoder{buf: append(make([]byte, 4, 256), op)}
}

func (e *Encoder) U8(v byte) { e.buf = append(e.buf, v) }

func (e *Encoder) U32(v uint32) { e.buf = binary.BigEndian.AppendUint32(e.buf, v) }

func (e *Encoder) U64(v uint64) { e.buf = binary.BigEndian.AppendUint64(e.buf, v) }

func (e *Encoder) Bytes(b []byte) {
	e.U32(uint32(len(b)))
	e.buf = append(e.buf, b...)
}

func (e *Encoder) Frame() []byte {
	binary.BigEndian.PutUint32(e.buf, uint32(len(e.buf)-4))
	return e.buf
}

type Decoder struct {
	buf []byte
	err error
}

func NewDecoder(b []byte) *Decoder { return &Decoder{buf: b} }

func (d *Decoder) fail(n int) bool {
	if d.err == nil && len(d.buf) < n {
		d.err = fmt.Errorf("metad: short message, need %d bytes, have %d", n, len(d.buf))
	}
	return d.err != nil
}

func (d *Decoder) U8() byte {
	if d.fail(1) {
		return 0
	}
	v := d.buf[0]
	d.buf = d.buf[1:]
	return v
}

func (d *Decoder) U32() uint32 {
	if d.fail(4) {
		return 0
	}
	v := binary.BigEndian.Uint32(d.buf)
	d.buf = d.buf[4:]
	return v
}

func (d *Decoder) U64() uint64 {
	if d.fail(8) {
		return 0
	}
	v := binary.BigEndian.Uint64(d.buf)
	d.buf = d.buf[8:]
	return v
}

func (d *Decoder) Bytes() []byte {
	n := int(d.U32())
	if d.fail(n) {
		return nil
	}
	v := d.buf[:n:n]
	d.buf = d.buf[n:]
	return v
}

func (d *Decoder) Valid() bool { return d.err == nil }

func (d *Decoder) Err() error {
	if d.err == nil && len(d.buf) != 0 {
		return fmt.Errorf("metad: %d trailing bytes", len(d.buf))
	}
	return d.err
}

func ReadFrame(r *bufio.Reader) (byte, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > MaxFrame {
		return 0, nil, fmt.Errorf("metad: bad frame length %d", n)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return 0, nil, err
	}
	return body[0], body[1:], nil
}

func EntryTarget(entry, head, tail []byte) ([]byte, error) {
	if len(entry) < 9 {
		return nil, fmt.Errorf("metad: entry of %d bytes", len(entry))
	}
	k := make([]byte, 0, len(head)+8+len(tail))
	k = append(k, head...)
	k = binary.LittleEndian.AppendUint64(k, binary.BigEndian.Uint64(entry[1:9]))
	return append(k, tail...), nil
}

func EncodeCommit(c *Commit) []byte {
	e := NewEncoder(OpCommit)
	e.U64(c.Snapshot)
	e.U32(uint32(len(c.Keys)))
	for _, k := range c.Keys {
		e.Bytes(k)
	}
	e.U32(uint32(len(c.Ranges)))
	for _, r := range c.Ranges {
		e.Bytes(r.Begin)
		e.Bytes(r.End)
	}
	e.U32(uint32(len(c.Writes)))
	for _, w := range c.Writes {
		e.U8(w.Op)
		e.Bytes(w.Key)
		if w.Op == WriteSet {
			e.Bytes(w.Value)
		}
	}
	return e.Frame()
}

func DecodeCommit(b []byte) (*Commit, error) {
	d := NewDecoder(b)
	c := &Commit{Snapshot: d.U64()}
	n := d.U32()
	for i := uint32(0); i < n && d.err == nil; i++ {
		c.Keys = append(c.Keys, d.Bytes())
	}
	n = d.U32()
	for i := uint32(0); i < n && d.err == nil; i++ {
		c.Ranges = append(c.Ranges, Range{d.Bytes(), d.Bytes()})
	}
	n = d.U32()
	for i := uint32(0); i < n && d.err == nil; i++ {
		w := Write{Op: d.U8(), Key: d.Bytes()}
		switch w.Op {
		case WriteSet:
			w.Value = d.Bytes()
		case WriteDelete:
		default:
			return nil, fmt.Errorf("metad: unknown write op %d", w.Op)
		}
		c.Writes = append(c.Writes, w)
	}
	return c, d.Err()
}
