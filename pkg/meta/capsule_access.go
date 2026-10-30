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
	"sync"
	"syscall"
	"time"
)

type CapsulePolicy struct {
	Root  Ino   `json:"root"`
	Depth int   `json:"depth"`
	Since int64 `json:"since"`
}

type UnitAccess struct {
	Root   Ino
	Last   int64
	Hits   int64
	Loaded int64
}

type accessTracker struct {
	sync.Mutex
	inodes map[Ino]struct{}
	units  map[Ino]Ino
}

const (
	accessFlushTicks = 60
	accessUnitCache  = 100000
	accessScanUnits  = 100
	maxCapsuleDepth  = 1000
)

func (m *baseMeta) touchAccess(ino Ino) {
	if m.capsulePolicy.Load() == nil || ino == 0 {
		return
	}
	m.access.Lock()
	if m.access.inodes == nil {
		m.access.inodes = make(map[Ino]struct{})
	}
	m.access.inodes[ino] = struct{}{}
	m.access.Unlock()
}

func (m *baseMeta) unitOfPolicy(ctx Context, p *CapsulePolicy, ino Ino) (Ino, bool) {
	m.access.Lock()
	if u, ok := m.access.units[ino]; ok {
		m.access.Unlock()
		return u, u != 0
	}
	m.access.Unlock()
	chain := []Ino{ino}
	for cur := ino; cur != p.Root; {
		if cur <= RootInode || len(chain) > maxCapsuleDepth {
			return 0, false
		}
		parent, st := m.rawParent(ctx, cur)
		if st != 0 || parent == 0 {
			return 0, false
		}
		chain = append(chain, parent)
		cur = parent
	}
	var unit Ino
	if i := len(chain) - 1 - p.Depth; i >= 0 {
		unit = chain[i]
	}
	m.access.Lock()
	if m.access.units == nil || len(m.access.units) > accessUnitCache {
		m.access.units = make(map[Ino]Ino)
	}
	for j, c := range chain {
		if j >= len(chain)-1-p.Depth {
			break
		}
		m.access.units[c] = unit
	}
	m.access.Unlock()
	return unit, unit != 0
}

func (m *baseMeta) rawParent(ctx Context, ino Ino) (Ino, syscall.Errno) {
	m.parentMu.Lock()
	parent, ok := m.dirParents[ino]
	m.parentMu.Unlock()
	if ok {
		return parent, 0
	}
	var attr Attr
	st := m.en.doGetAttr(ctx, ino, &attr)
	return attr.Parent, st
}

func (m *baseMeta) flushAccess(ctx Context) {
	c, ok := m.en.(capsuleEngine)
	if !ok {
		return
	}
	p := m.capsulePolicy.Load()
	if p == nil {
		return
	}
	m.access.Lock()
	inodes := m.access.inodes
	m.access.inodes = nil
	m.access.Unlock()
	units := make(map[Ino]struct{})
	for ino := range inodes {
		if u, ok := m.unitOfPolicy(ctx, p, ino); ok {
			units[u] = struct{}{}
		}
	}
	if len(units) == 0 {
		return
	}
	if len(units) > accessScanUnits {
		logger.Debugf("%d units touched in one epoch, not counted as access", len(units))
		return
	}
	roots := make([]Ino, 0, len(units))
	for u := range units {
		roots = append(roots, u)
	}
	if err := c.doRecordAccess(ctx, roots, time.Now().Unix()); err != nil {
		logger.Warnf("Record access of %d units: %s", len(roots), err)
	}
}

func (m *baseMeta) refreshPolicy(ctx Context) {
	c, ok := m.en.(capsuleEngine)
	if !ok {
		return
	}
	p, err := c.doLoadPolicy(ctx)
	if err != nil {
		logger.Warnf("Load capsule policy: %s", err)
		return
	}
	m.capsulePolicy.Store(p)
}

func (m *baseMeta) SetCapsulePolicy(ctx Context, p *CapsulePolicy) error {
	c, err := m.capsuleEngine()
	if err != nil {
		return err
	}
	if p != nil && p.Since == 0 {
		p.Since = time.Now().Unix()
	}
	if err = c.doSetPolicy(ctx, p); err != nil {
		return err
	}
	m.capsulePolicy.Store(p)
	return nil
}

func (m *baseMeta) GetCapsulePolicy(ctx Context) (*CapsulePolicy, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	return c.doLoadPolicy(ctx)
}

func (m *baseMeta) UnitAccess(ctx Context, roots []Ino) (map[Ino]*UnitAccess, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	return c.doLoadAccess(ctx, roots)
}
