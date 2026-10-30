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
	"errors"
	"fmt"
	"math"
	"sort"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

type AutoEvictOptions struct {
	Idle        time.Duration
	MinResident time.Duration
	HalfLife    time.Duration
	MemoryLimit int64
	High        float64
	Low         float64
	MaxUnits    int
	MaxInodes   int64
}

type AutoEvictReport struct {
	Units        int
	Cold         int
	Candidates   int
	Evicted      []Ino
	Skipped      map[Ino]string
	MemoryBefore int64
	MemoryAfter  int64
	Pressure     bool
}

type unitScore struct {
	root  Ino
	idle  int64
	score float64
}

func (m *baseMeta) policyUnits(ctx Context, p *CapsulePolicy, s *capsuleSet) (hot []Ino, cold int, err error) {
	level := []Ino{p.Root}
	for d := 0; d < p.Depth; d++ {
		var next []Ino
		for _, dir := range level {
			if s != nil {
				if _, ok := s.unitOf(dir); ok {
					continue
				}
			}
			var entries []*Entry
			if st := m.en.doReaddir(ctx, dir, 0, &entries, -1); st != 0 {
				return nil, 0, fmt.Errorf("list inode %d: %w", dir, st)
			}
			for _, e := range entries {
				if e.Attr.Typ == TypeDirectory {
					next = append(next, e.Inode)
				}
			}
		}
		level = next
	}
	for _, u := range level {
		if s != nil {
			if root, ok := s.unitOf(u); ok {
				if root == u {
					cold++
				}
				continue
			}
		}
		hot = append(hot, u)
	}
	return hot, cold, nil
}

func decayedHits(a *UnitAccess, now int64, halfLife time.Duration) float64 {
	if a.Hits <= 0 {
		return 0
	}
	age := float64(max(now-a.Last, 0))
	return float64(a.Hits) * math.Exp2(-age/halfLife.Seconds())
}

func (m *baseMeta) AutoEvict(ctx Context, store object.ObjectStorage, opt AutoEvictOptions) (*AutoEvictReport, error) {
	c, err := m.capsuleEngine()
	if err != nil {
		return nil, err
	}
	if opt.HalfLife <= 0 || opt.MaxUnits <= 0 || (opt.MemoryLimit > 0 && !(opt.Low > 0 && opt.Low < opt.High)) {
		return nil, fmt.Errorf("invalid auto-evict options: %w", syscall.EINVAL)
	}
	p, err := c.doLoadPolicy(ctx)
	if err != nil {
		return nil, err
	}
	if p == nil {
		return nil, fmt.Errorf("no capsule policy is set: %w", syscall.ENOENT)
	}
	m.refreshCapsules(ctx)
	hot, cold, err := m.policyUnits(ctx, p, m.capsules.Load())
	if err != nil {
		return nil, err
	}
	rep := &AutoEvictReport{Units: len(hot) + cold, Cold: cold, Skipped: make(map[Ino]string)}
	acc, err := c.doLoadAccess(ctx, hot)
	if err != nil {
		return nil, err
	}
	if opt.MemoryLimit > 0 {
		if rep.MemoryBefore, err = c.doMemoryUsed(ctx); err != nil {
			return nil, err
		}
		rep.MemoryAfter = rep.MemoryBefore
		rep.Pressure = float64(rep.MemoryBefore) > opt.High*float64(opt.MemoryLimit)
	}
	now := time.Now().Unix()
	var idle, rest []unitScore
	for _, u := range hot {
		a := acc[u]
		if a == nil {
			a = &UnitAccess{Root: u}
		}
		last := max(a.Last, p.Since)
		if last == 0 || now-a.Loaded < int64(opt.MinResident.Seconds()) {
			continue
		}
		us := unitScore{u, now - last, decayedHits(a, now, opt.HalfLife)}
		if opt.Idle > 0 && us.idle >= int64(opt.Idle.Seconds()) {
			idle = append(idle, us)
		} else if us.idle >= int64(opt.MinResident.Seconds()) {
			rest = append(rest, us)
		}
	}
	sort.Slice(idle, func(i, j int) bool { return idle[i].idle > idle[j].idle })
	sort.Slice(rest, func(i, j int) bool {
		if rest[i].score != rest[j].score {
			return rest[i].score < rest[j].score
		}
		return rest[i].idle > rest[j].idle
	})
	queue := idle
	if rep.Pressure {
		queue = append(queue, rest...)
	}
	rep.Candidates = len(queue)
	for _, us := range queue {
		if len(rep.Evicted) >= opt.MaxUnits {
			break
		}
		pressing := opt.MemoryLimit > 0 && float64(rep.MemoryAfter) > opt.Low*float64(opt.MemoryLimit)
		if !rep.Pressure || !pressing {
			if us.idle < int64(opt.Idle.Seconds()) || opt.Idle <= 0 {
				break
			}
		}
		if _, err := m.EvictMetadata(ctx, us.root, store, opt.MaxInodes); err != nil {
			if errors.Is(err, syscall.EBUSY) {
				return rep, err
			}
			rep.Skipped[us.root] = err.Error()
			logger.Infof("Skip evicting inode %d: %s", us.root, err)
			continue
		}
		rep.Evicted = append(rep.Evicted, us.root)
		if opt.MemoryLimit > 0 {
			if used, err := c.doMemoryUsed(ctx); err == nil {
				rep.MemoryAfter = used
			}
		}
	}
	return rep, nil
}
