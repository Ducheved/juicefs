/*
 * JuiceFS, Copyright 2024 Juicedata, Inc.
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
	"fmt"
	"math"
	"slices"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
)

// stat of dir
type dirStat struct {
	length int64
	space  int64
	inodes int64
}

const (
	DirQuotaType = iota
	UserQuotaType
	GroupQuotaType
	AllQuotaType
)

type Quota struct {
	MaxSpace, MaxInodes   int64
	UsedSpace, UsedInodes int64
	newSpace, newInodes   int64
	touched               int64
	flushed               int64
	unresolved            bool
}

type quotaScope struct {
	allDirs   bool
	dirs      []uint64
	userGroup bool
	users     []uint64
	groups    []uint64
}

type dirParent struct {
	parent   Ino
	verified int64
	touched  int64
}

type chainQuota struct {
	inode      Ino
	quota      *Quota
	unresolved bool
}

const (
	quotaBatchSize = 500
	quotaCacheIdle = 2 * time.Minute
	quotaCacheSize = 100000
	maxQuotaChain  = 1000

	parentRefreshBatch   = 10000
	parentRefreshWorkers = 16
)

type iQuota struct {
	qtype uint32
	qkey  uint64 // ino/uid/gid
	quota *Quota
}

// Returns true if it will exceed the quota limit
func (q *Quota) check(space, inodes int64) bool {
	if space > 0 {
		max := atomic.LoadInt64(&q.MaxSpace)
		if max > 0 && atomic.LoadInt64(&q.UsedSpace)+atomic.LoadInt64(&q.newSpace)+space > max {
			return true
		}
	}
	if inodes > 0 {
		max := atomic.LoadInt64(&q.MaxInodes)
		if max > 0 && atomic.LoadInt64(&q.UsedInodes)+atomic.LoadInt64(&q.newInodes)+inodes > max {
			return true
		}
	}
	return false
}

func (q *Quota) update(space, inodes int64) {
	atomic.AddInt64(&q.newSpace, space)
	atomic.AddInt64(&q.newInodes, inodes)
}

func (q *Quota) snap() Quota {
	return Quota{
		MaxSpace:   atomic.LoadInt64(&q.MaxSpace),
		MaxInodes:  atomic.LoadInt64(&q.MaxInodes),
		UsedSpace:  atomic.LoadInt64(&q.UsedSpace),
		UsedInodes: atomic.LoadInt64(&q.UsedInodes),
		newSpace:   atomic.LoadInt64(&q.newSpace),
		newInodes:  atomic.LoadInt64(&q.newInodes),
	}
}

// not thread safe
func (q *Quota) sanitize() {
	if q.UsedSpace < 0 {
		q.UsedSpace = 0
	}
	if q.MaxSpace > 0 && q.MaxSpace < q.UsedSpace {
		q.MaxSpace = q.UsedSpace
	}
	if q.UsedInodes < 0 {
		q.UsedInodes = 0
	}
	if q.MaxInodes > 0 && q.MaxInodes < q.UsedInodes {
		q.MaxInodes = q.UsedInodes
	}
}

func (m *baseMeta) parallelSyncDirStat(ctx Context, inos map[Ino]bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := range inos {
		wg.Add(1)
		go func(ino Ino) {
			defer wg.Done()
			_, st := m.en.doSyncDirStat(ctx, ino)
			if st != 0 && st != syscall.ENOENT {
				logger.Warnf("sync dir stat for %d: %s", ino, st)
			}
		}(i)
	}
	return &wg
}

func (m *baseMeta) groupBatch(batch map[Ino]dirStat, size int) [][]Ino {
	var inos []Ino
	for ino := range batch {
		inos = append(inos, ino)
	}
	sort.Slice(inos, func(i, j int) bool {
		return inos[i] < inos[j]
	})
	var batches [][]Ino
	for i := 0; i < len(inos); i += size {
		end := i + size
		if end > len(inos) {
			end = len(inos)
		}
		batches = append(batches, inos[i:end])
	}
	return batches
}

func (m *baseMeta) calcDirStat(ctx Context, ino Ino) (*dirStat, syscall.Errno) {
	var entries []*Entry
	if eno := m.readdirHot(ctx, ino, 1, &entries, -1); eno != 0 {
		return nil, eno
	}

	stat := new(dirStat)
	for _, e := range entries {
		if ctx.Canceled() {
			return nil, syscall.EINTR
		}
		stat.inodes += 1
		var l uint64
		if e.Attr.Typ == TypeFile {
			l = e.Attr.Length
		}
		stat.length += int64(l)
		stat.space += align4K(l)
	}
	return stat, 0
}

func (m *baseMeta) GetDirStat(ctx Context, inode Ino) (stat *dirStat, st syscall.Errno) {
	stat, st = m.en.doGetDirStat(ctx, m.checkRoot(inode), !m.conf.ReadOnly)
	if st != 0 {
		return
	}
	if stat == nil {
		stat, st = m.calcDirStat(ctx, inode)
	} else {
		m.dirStatsLock.RLock()
		pending := m.dirStats[inode]
		m.dirStatsLock.RUnlock()
		stat.length += pending.length
		stat.space += pending.space
		stat.inodes += pending.inodes
	}
	return
}

func (m *baseMeta) updateDirStat(ctx Context, ino Ino, length, space, inodes int64) {
	if !m.getFormat().DirStats {
		return
	}
	m.dirStatsLock.Lock()
	defer m.dirStatsLock.Unlock()
	stat := m.dirStats[ino]
	stat.length += length
	stat.inodes += inodes
	stat.space += space
	if stat == (dirStat{}) {
		delete(m.dirStats, ino)
	} else {
		m.dirStats[ino] = stat
	}
}

func (m *baseMeta) updateParentStat(ctx Context, inode, parent Ino, length, space int64) {
	if length == 0 && space == 0 {
		return
	}
	m.en.updateStats(space, 0)
	if !m.getFormat().DirStats {
		return
	}
	if parent > 0 {
		m.updateDirStat(ctx, parent, length, space, 0)
		m.updateDirQuota(ctx, parent, space, 0)
	} else {
		go func() {
			for p, v := range m.en.doGetParents(ctx, inode) {
				m.updateDirStat(ctx, p, length*int64(v), space*int64(v), 0)
				m.updateDirQuota(ctx, p, space*int64(v), 0)
			}
		}()
	}
}

func (m *baseMeta) flushDirStat(ctx Context) {
	defer m.sessWG.Done()
	period := 1 * time.Second
	if m.conf.DirStatFlushPeriod != 0 {
		period = m.conf.DirStatFlushPeriod
	}

	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.doFlushDirStat()
		}
	}
}

func (m *baseMeta) doFlushDirStat() {
	if !m.getFormat().DirStats {
		return
	}
	m.dirStatsLock.Lock()
	if len(m.dirStats) == 0 {
		m.dirStatsLock.Unlock()
		return
	}
	stats := m.dirStats
	m.dirStats = make(map[Ino]dirStat)
	m.dirStatsLock.Unlock()
	err := m.en.doUpdateDirStat(Background(), stats)
	if err != nil {
		logger.Errorf("update dir stat failed: %v", err)
	}
}

func (m *baseMeta) flushStats(ctx Context) {
	defer m.sessWG.Done()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.doFlushStats()
		}
	}
}

func (m *baseMeta) doFlushStats() {
	m.fsStatsLock.Lock()
	m.en.doFlushStats()
	m.fsStatsLock.Unlock()
}

func (m *baseMeta) syncVolumeStat(ctx Context, used, inodes int64) error {
	return m.en.doSyncVolumeStat(ctx, used, inodes)
}

func (m *baseMeta) checkQuota(ctx Context, space, inodes int64, uid, gid uint32, parents ...Ino) syscall.Errno {
	if space <= 0 && inodes <= 0 {
		return 0
	}
	if st := m.checkUserGroupQuota(ctx, uint64(uid), uint64(gid), space, inodes); st != 0 {
		return st
	}

	format := m.getFormat()
	if space > 0 && format.Capacity > 0 && atomic.LoadInt64(&m.usedSpace)+atomic.LoadInt64(&m.newSpace)+space > int64(format.Capacity) {
		return syscall.ENOSPC
	}
	if inodes > 0 && format.Inodes > 0 && atomic.LoadInt64(&m.usedInodes)+atomic.LoadInt64(&m.newInodes)+inodes > int64(format.Inodes) {
		return syscall.ENOSPC
	}
	if !format.DirStats {
		return 0
	}
	for _, ino := range parents {
		if st := m.checkDirQuota(ctx, ino, space, inodes); st != 0 {
			return st
		}
	}
	return 0
}

func (m *baseMeta) loadQuotas() {
	format := m.getFormat()
	if !format.DirStats && !format.UserGroupQuota {
		return
	}
	if format.DirStats {
		if exist, err := m.en.doHasDirQuotas(Background()); err != nil {
			logger.Warnf("Check dir quotas: %s", err)
		} else {
			m.noDirQuota.Store(!exist)
		}
	}
	if m.refreshQuotas() && format.DirStats && !m.noDirQuota.Load() {
		m.refreshDirParents(Background())
	}
}

func (m *baseMeta) refreshQuotas() bool {
	m.quotasFlushLock.Lock()
	positive, negative := m.cachedDirQuotas()
	scope := quotaScope{dirs: append(append([]uint64{}, positive...), negative...)}
	if m.getFormat().UserGroupQuota {
		scope.users = m.cachedUserGroupQuotas(UserQuotaType)
		scope.groups = m.cachedUserGroupQuotas(GroupQuotaType)
	}
	m.quotasFlushLock.Unlock()
	for i := 0; ; i++ {
		gen := m.quotaFlushGen.Load()
		dirQuotas, userQuotas, groupQuotas, err := m.en.doLoadQuotas(Background(), scope)
		if err != nil {
			logger.Warnf("Load quotas: %s", err)
			return false
		}
		m.quotasFlushLock.Lock()
		m.quotaMu.Lock()
		var stale quotaScope
		stale.dirs = m.syncDirQuotas(positive, negative, dirQuotas, gen)
		stale.users = syncUserGroupQuotas(m.userQuotas, scope.users, userQuotas, gen)
		stale.groups = syncUserGroupQuotas(m.groupQuotas, scope.groups, groupQuotas, gen)
		m.quotaMu.Unlock()
		m.quotasFlushLock.Unlock()
		if i == 2 || len(stale.dirs)+len(stale.users)+len(stale.groups) == 0 {
			return true
		}
		scope, positive, negative = stale, stale.dirs, nil
	}
}

func syncQuota(existing, loaded *Quota, gen int64) bool {
	atomic.SwapInt64(&existing.MaxSpace, loaded.MaxSpace)
	atomic.SwapInt64(&existing.MaxInodes, loaded.MaxInodes)
	if atomic.LoadInt64(&existing.flushed) > gen {
		return false
	}
	atomic.SwapInt64(&existing.UsedSpace, loaded.UsedSpace)
	atomic.SwapInt64(&existing.UsedInodes, loaded.UsedInodes)
	return true
}

func (m *baseMeta) ugQuotas(qtype uint32) map[uint64]*Quota {
	if qtype == GroupQuotaType {
		return m.groupQuotas
	}
	return m.userQuotas
}

func (m *baseMeta) cachedUserGroupQuotas(qtype uint32) []uint64 {
	idle := time.Now().Add(-quotaCacheIdle).UnixNano()
	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()
	quotas := m.ugQuotas(qtype)
	touched := make([]int64, 0, len(quotas))
	for _, q := range quotas {
		touched = append(touched, atomic.LoadInt64(&q.touched))
	}
	threshold := evictThreshold(touched, idle)
	ids := make([]uint64, 0, len(quotas))
	for id, q := range quotas {
		if atomic.LoadInt64(&q.touched) < threshold && atomic.LoadInt64(&q.newSpace) == 0 && atomic.LoadInt64(&q.newInodes) == 0 {
			delete(quotas, id)
		} else {
			ids = append(ids, id)
		}
	}
	return ids
}

func syncUserGroupQuotas(existing map[uint64]*Quota, ids []uint64, loaded map[uint64]*Quota, gen int64) (stale []uint64) {
	for _, id := range ids {
		q := existing[id]
		if q == nil {
			continue
		}
		l := loaded[id]
		if l == nil {
			l = &Quota{MaxSpace: -1, MaxInodes: -1}
		}
		if syncQuota(q, l, gen) {
			q.unresolved = false
		} else {
			stale = append(stale, id)
		}
	}
	return
}

func (m *baseMeta) missingUserGroupQuotas(scope quotaScope) (missing quotaScope) {
	m.quotaMu.RLock()
	defer m.quotaMu.RUnlock()
	for _, id := range scope.users {
		if q := m.userQuotas[id]; q == nil || q.unresolved {
			missing.users = append(missing.users, id)
		}
	}
	for _, id := range scope.groups {
		if q := m.groupQuotas[id]; q == nil || q.unresolved {
			missing.groups = append(missing.groups, id)
		}
	}
	return
}

func (m *baseMeta) fetchUserGroupQuotas(scope quotaScope) {
	var err error
	for i := 0; i < 3; i++ {
		gen := m.quotaFlushGen.Load()
		var users, groups map[uint64]*Quota
		if _, users, groups, err = m.en.doLoadQuotas(Background(), scope); err != nil {
			time.Sleep(time.Duration(10<<i) * time.Millisecond)
			continue
		}
		if i == 2 {
			gen = math.MaxInt64
		}
		if scope = m.applyUserGroupQuotas(scope, users, groups, gen); len(scope.users)+len(scope.groups) == 0 {
			return
		}
	}
	if err == nil {
		return
	}
	logger.Warnf("Load quotas of users %v groups %v: %s", scope.users, scope.groups, err)
	now := time.Now().UnixNano()
	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()
	for qtype, ids := range map[uint32][]uint64{UserQuotaType: scope.users, GroupQuotaType: scope.groups} {
		quotas := m.ugQuotas(qtype)
		for _, id := range ids {
			if quotas[id] == nil {
				quotas[id] = &Quota{MaxSpace: -1, MaxInodes: -1, touched: now, unresolved: true}
			}
		}
	}
}

func (m *baseMeta) applyUserGroupQuotas(scope quotaScope, users, groups map[uint64]*Quota, gen int64) (stale quotaScope) {
	now := time.Now().UnixNano()
	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()
	for _, t := range []struct {
		qtype  uint32
		ids    []uint64
		loaded map[uint64]*Quota
		stale  *[]uint64
	}{{UserQuotaType, scope.users, users, &stale.users}, {GroupQuotaType, scope.groups, groups, &stale.groups}} {
		quotas := m.ugQuotas(t.qtype)
		for _, id := range t.ids {
			l := t.loaded[id]
			if l == nil {
				l = &Quota{MaxSpace: -1, MaxInodes: -1}
			}
			q := quotas[id]
			if q == nil {
				l.touched = now
				quotas[id] = l
				continue
			}
			atomic.StoreInt64(&q.touched, now)
			if !q.unresolved {
				continue
			}
			if syncQuota(q, l, gen) {
				q.unresolved = false
			} else {
				*t.stale = append(*t.stale, id)
			}
		}
	}
	return
}

func evictThreshold(touched []int64, idle int64) int64 {
	if len(touched) <= quotaCacheSize {
		return idle
	}
	sort.Slice(touched, func(i, j int) bool { return touched[i] < touched[j] })
	return max(idle, touched[len(touched)-quotaCacheSize])
}

func (m *baseMeta) cachedDirQuotas() (positive, negative []uint64) {
	idle := time.Now().Add(-quotaCacheIdle).UnixNano()
	m.quotaMu.RLock()
	touched := make([]int64, 0, len(m.dirQuotas)+len(m.noDirQuotas))
	for _, q := range m.dirQuotas {
		touched = append(touched, atomic.LoadInt64(&q.touched))
	}
	for _, t := range m.noDirQuotas {
		touched = append(touched, atomic.LoadInt64(t))
	}
	m.quotaMu.RUnlock()
	threshold := evictThreshold(touched, idle)
	none := m.noDirQuota.Load()

	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()
	for ino, q := range m.dirQuotas {
		if atomic.LoadInt64(&q.touched) < threshold && atomic.LoadInt64(&q.newSpace) == 0 && atomic.LoadInt64(&q.newInodes) == 0 {
			delete(m.dirQuotas, ino)
		} else {
			positive = append(positive, ino)
		}
	}
	for ino, t := range m.noDirQuotas {
		if none || atomic.LoadInt64(t) < threshold {
			delete(m.noDirQuotas, ino)
		} else {
			negative = append(negative, ino)
		}
	}
	return
}

func (m *baseMeta) syncDirQuotas(positive, negative []uint64, loaded map[uint64]*Quota, gen int64) (stale []uint64) {
	for _, ino := range positive {
		existing := m.dirQuotas[ino]
		if existing == nil {
			continue
		}
		if q, found := loaded[ino]; found {
			if !syncQuota(existing, q, gen) {
				stale = append(stale, ino)
			}
			existing.unresolved = false
			continue
		}
		if !existing.unresolved {
			logger.Infof("Quota for inode %d is deleted", ino)
		}
		delete(m.dirQuotas, ino)
		if !m.noDirQuota.Load() {
			t := atomic.LoadInt64(&existing.touched)
			m.noDirQuotas[ino] = &t
		}
	}
	for _, ino := range negative {
		q, found := loaded[ino]
		if !found || m.dirQuotas[ino] != nil {
			continue
		}
		if t := m.noDirQuotas[ino]; t != nil {
			q.touched = atomic.LoadInt64(t)
			delete(m.noDirQuotas, ino)
		} else {
			q.touched = time.Now().UnixNano()
		}
		logger.Infof("Quota for inode %d is loaded", ino)
		m.dirQuotas[ino] = q
	}
	return
}

func (m *baseMeta) fetchDirQuotas(inodes []uint64) {
	var loaded map[uint64]*Quota
	var err error
	for i := 0; i < 3; i++ {
		if loaded, _, _, err = m.en.doLoadQuotas(Background(), quotaScope{dirs: inodes}); err == nil {
			break
		}
		time.Sleep(time.Duration(10<<i) * time.Millisecond)
	}
	if err != nil {
		logger.Warnf("Load quotas of inodes %v: %s", inodes, err)
	}
	now := time.Now().UnixNano()
	m.quotaMu.Lock()
	defer m.quotaMu.Unlock()
	for _, ino := range inodes {
		if m.dirQuotas[ino] != nil || m.noDirQuotas[ino] != nil {
			continue
		}
		if err != nil {
			m.dirQuotas[ino] = &Quota{MaxSpace: -1, MaxInodes: -1, touched: now, unresolved: true}
		} else if q := loaded[ino]; q != nil {
			q.touched = now
			m.dirQuotas[ino] = q
		} else {
			t := now
			m.noDirQuotas[ino] = &t
		}
	}
}

func (m *baseMeta) setDirParent(inode, parent Ino) {
	now := time.Now().UnixNano()
	m.parentMu.Lock()
	m.dirParents[inode] = dirParent{parent: parent, verified: now, touched: m.dirParents[inode].touched}
	m.parentMu.Unlock()
}

func (m *baseMeta) getDirParent(ctx Context, inode Ino) (Ino, syscall.Errno) {
	now := time.Now().UnixNano()
	m.parentMu.Lock()
	p, ok := m.dirParents[inode]
	if ok {
		p.touched = now
		m.dirParents[inode] = p
	}
	m.parentMu.Unlock()
	if ok {
		return p.parent, 0
	}
	logger.Debugf("Get directory parent of inode %d: cache miss", inode)
	var attr Attr
	st := m.GetAttr(ctx, inode, &attr)
	if st == 0 {
		m.parentMu.Lock()
		if p, ok := m.dirParents[inode]; ok {
			p.touched = now
			m.dirParents[inode] = p
		}
		m.parentMu.Unlock()
	}
	return attr.Parent, st
}

func (m *baseMeta) refreshDirParents(ctx Context) {
	now := time.Now()
	idle := now.Add(-quotaCacheIdle).UnixNano()
	stale := now.Add(-m.conf.Heartbeat / 2).UnixNano()
	m.parentMu.Lock()
	touched := make([]int64, 0, len(m.dirParents))
	for _, p := range m.dirParents {
		touched = append(touched, max(p.touched, p.verified))
	}
	m.parentMu.Unlock()
	threshold := evictThreshold(touched, idle)

	type candidate struct {
		inode    Ino
		verified int64
	}
	var candidates []candidate
	m.parentMu.Lock()
	for ino, p := range m.dirParents {
		if max(p.touched, p.verified) < threshold {
			delete(m.dirParents, ino)
		} else if p.touched >= idle && p.verified < stale {
			candidates = append(candidates, candidate{ino, p.verified})
		}
	}
	m.parentMu.Unlock()
	if len(candidates) > parentRefreshBatch {
		sort.Slice(candidates, func(i, j int) bool { return candidates[i].verified < candidates[j].verified })
		candidates = candidates[:parentRefreshBatch]
	}

	work := make(chan candidate)
	var wg sync.WaitGroup
	for i := 0; i < parentRefreshWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range work {
				var attr Attr
				st := m.en.doGetAttr(ctx, c.inode, &attr)
				if st != 0 && st != syscall.ENOENT {
					logger.Warnf("Refresh directory parent of inode %d: %s", c.inode, st)
					continue
				}
				m.parentMu.Lock()
				if p, ok := m.dirParents[c.inode]; ok && p.verified == c.verified {
					if st == 0 && attr.Typ == TypeDirectory && !attr.Parent.IsTrash() {
						p.parent, p.verified = attr.Parent, time.Now().UnixNano()
						m.dirParents[c.inode] = p
					} else {
						delete(m.dirParents, c.inode)
					}
				}
				m.parentMu.Unlock()
			}
		}()
	}
	for _, c := range candidates {
		work <- c
	}
	close(work)
	wg.Wait()
}

func (m *baseMeta) quotaChain(ctx Context, inode Ino) []chainQuota {
	if !m.getFormat().DirStats || m.noDirQuota.Load() {
		return nil
	}
	var chain []Ino
	for inode > 0 {
		if slices.Contains(chain, inode) || len(chain) >= maxQuotaChain {
			logger.Warnf("Loop in directory parents of inode %d: %v", chain[0], chain)
			m.parentMu.Lock()
			for _, ino := range chain {
				delete(m.dirParents, ino)
			}
			m.parentMu.Unlock()
			break
		}
		chain = append(chain, inode)
		if inode <= RootInode {
			break
		}
		last := inode
		var st syscall.Errno
		if inode, st = m.getDirParent(ctx, inode); st != 0 {
			logger.Warnf("Get directory parent of inode %d: %s", last, st)
			break
		}
	}
	collect := func() ([]chainQuota, []uint64) {
		now := time.Now().UnixNano()
		var quotas []chainQuota
		var unknown []uint64
		m.quotaMu.RLock()
		defer m.quotaMu.RUnlock()
		for _, ino := range chain {
			if q := m.dirQuotas[uint64(ino)]; q != nil {
				atomic.StoreInt64(&q.touched, now)
				quotas = append(quotas, chainQuota{ino, q, q.unresolved})
			} else if t := m.noDirQuotas[uint64(ino)]; t != nil {
				atomic.StoreInt64(t, now)
			} else {
				unknown = append(unknown, uint64(ino))
			}
		}
		return quotas, unknown
	}
	quotas, unknown := collect()
	if len(unknown) > 0 {
		m.fetchDirQuotas(unknown)
		quotas, _ = collect()
	}
	return quotas
}

// get inode of the first parent (or myself) with quota
func (m *baseMeta) getQuotaParent(ctx Context, inode Ino) (Ino, *Quota) {
	for _, c := range m.quotaChain(ctx, inode) {
		return c.inode, c.quota
	}
	return 0, nil
}

func (m *baseMeta) checkDirQuota(ctx Context, inode Ino, space, inodes int64) syscall.Errno {
	for _, c := range m.quotaChain(ctx, inode) {
		if c.unresolved {
			if space > 0 || inodes > 0 {
				return syscall.EIO
			}
		} else if c.quota.check(space, inodes) {
			return syscall.EDQUOT
		}
	}
	return 0
}

func (m *baseMeta) checkUserGroupQuotas(scope quotaScope, space, inodes int64) syscall.Errno {
	if !m.getFormat().UserGroupQuota || (space <= 0 && inodes <= 0) {
		return 0
	}
	if missing := m.missingUserGroupQuotas(scope); len(missing.users)+len(missing.groups) > 0 {
		m.fetchUserGroupQuotas(missing)
	}
	now := time.Now().UnixNano()
	m.quotaMu.RLock()
	defer m.quotaMu.RUnlock()
	for qtype, ids := range map[uint32][]uint64{UserQuotaType: scope.users, GroupQuotaType: scope.groups} {
		for _, id := range ids {
			q := m.ugQuotas(qtype)[id]
			if q == nil || q.unresolved {
				return syscall.EIO
			}
			atomic.StoreInt64(&q.touched, now)
			if q.check(space, inodes) {
				return syscall.EDQUOT
			}
		}
	}
	return 0
}

func (m *baseMeta) checkUserGroupQuota(ctx Context, uid, gid uint64, space, inodes int64) syscall.Errno {
	return m.checkUserGroupQuotas(quotaScope{users: []uint64{uid}, groups: []uint64{gid}}, space, inodes)
}

func (m *baseMeta) checkGroupQuota(ctx Context, gid uint64, space, inodes int64) syscall.Errno {
	return m.checkUserGroupQuotas(quotaScope{groups: []uint64{gid}}, space, inodes)
}

func (m *baseMeta) updateDirQuota(ctx Context, inode Ino, space, inodes int64) {
	for _, c := range m.quotaChain(ctx, inode) {
		c.quota.update(space, inodes)
	}
}

func (m *baseMeta) updateUserGroupStat(ctx Context, uid, gid uint32, space, inodes int64) {
	if !m.getFormat().UserGroupQuota {
		return
	}
	if space == 0 && inodes == 0 {
		return
	}
	now := time.Now().UnixNano()
	m.quotaMu.Lock()
	for _, t := range []struct {
		quotas map[uint64]*Quota
		id     uint64
	}{{m.userQuotas, uint64(uid)}, {m.groupQuotas, uint64(gid)}} {
		if q := t.quotas[t.id]; q != nil {
			q.update(space, inodes)
			atomic.StoreInt64(&q.touched, now)
		} else {
			t.quotas[t.id] = &Quota{
				MaxSpace:   -1,
				MaxInodes:  -1,
				newSpace:   space,
				newInodes:  inodes,
				touched:    now,
				unresolved: true,
			}
		}
	}
	m.quotaMu.Unlock()
}

func (m *baseMeta) flushQuotas(ctx Context) {
	defer m.sessWG.Done()
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.doFlushQuotas()
		}
	}
}

func (m *baseMeta) collectQuotas(qtype uint32, quotas map[uint64]*Quota) []*iQuota {
	var result []*iQuota
	for key, q := range quotas {
		if q.unresolved && qtype == DirQuotaType {
			continue
		}
		newSpace := atomic.LoadInt64(&q.newSpace)
		newInodes := atomic.LoadInt64(&q.newInodes)
		if newSpace != 0 || newInodes != 0 {
			result = append(result, &iQuota{
				qtype: qtype,
				qkey:  key,
				quota: &Quota{newSpace: newSpace, newInodes: newInodes},
			})
		}
	}
	return result
}

func (m *baseMeta) updateQuota(q *Quota, newSpace, newInodes int64) {
	atomic.AddInt64(&q.newSpace, -newSpace)
	atomic.AddInt64(&q.UsedSpace, newSpace)
	atomic.AddInt64(&q.newInodes, -newInodes)
	atomic.AddInt64(&q.UsedInodes, newInodes)
}

func (m *baseMeta) doFlushQuotas() {
	if !m.getFormat().DirStats && !m.getFormat().UserGroupQuota {
		return
	}
	m.quotasFlushLock.Lock()
	defer m.quotasFlushLock.Unlock()

	var allQuotas []*iQuota
	m.quotaMu.RLock()
	allQuotas = append(allQuotas, m.collectQuotas(DirQuotaType, m.dirQuotas)...)
	allQuotas = append(allQuotas, m.collectQuotas(UserQuotaType, m.userQuotas)...)
	allQuotas = append(allQuotas, m.collectQuotas(GroupQuotaType, m.groupQuotas)...)
	m.quotaMu.RUnlock()

	if len(allQuotas) == 0 {
		return
	}
	if err := m.en.doFlushQuotas(Background(), allQuotas); err != nil {
		logger.Warnf("Flush quotas: %s", err)
		return
	}
	m.quotaMu.Lock()
	gen := m.quotaFlushGen.Add(1)
	for _, snap := range allQuotas {
		var q *Quota
		switch snap.qtype {
		case DirQuotaType:
			q = m.dirQuotas[snap.qkey]
		case UserQuotaType:
			q = m.userQuotas[snap.qkey]
		case GroupQuotaType:
			q = m.groupQuotas[snap.qkey]
		}
		if q != nil {
			m.updateQuota(q, snap.quota.newSpace, snap.quota.newInodes)
			atomic.StoreInt64(&q.flushed, gen)
		}
	}
	m.quotaMu.Unlock()

}

func (m *baseMeta) HandleQuota(ctx Context, cmd uint8, qkey string, qtype uint32, quotas map[string]*Quota, strict, repair bool, create bool) error {
	var inode Ino
	var dpath string
	var key uint64

	if qtype == DirQuotaType && cmd != QuotaList {
		dpath = qkey
		if st := m.resolve(ctx, dpath, &inode, create); st != 0 {
			return fmt.Errorf("resolve dir %s: %w", dpath, st)
		}
		if inode.IsTrash() {
			return errors.New("no quota for any trash directory")
		}
		key = uint64(inode)
	} else if (qtype == UserQuotaType || qtype == GroupQuotaType) && cmd != QuotaCheck {
		id, err := strconv.ParseUint(qkey, 10, 64)
		if err != nil {
			return fmt.Errorf("parse quota key %q: %s: %w", qkey, err, syscall.EINVAL)
		}
		key = id
	}

	switch cmd {
	case QuotaSet:
		return m.handleQuotaSet(ctx, qtype, key, dpath, quotas, strict)
	case QuotaGet:
		return m.handleQuotaGet(ctx, qtype, key, dpath, quotas)
	case QuotaDel:
		return m.en.doDelQuota(ctx, qtype, key)
	case QuotaList:
		return m.handleQuotaList(ctx, qtype, key, quotas)
	case QuotaCheck:
		return m.handleQuotaCheck(ctx, qtype, key, dpath, strict, repair, quotas)
	default:
		return fmt.Errorf("invalid quota command: %d: %w", cmd, syscall.EINVAL)
	}
}

func (m *baseMeta) handleQuotaSet(ctx Context, qtype uint32, key uint64, dpath string, quotas map[string]*Quota, strict bool) error {
	format := m.getFormat().Clone()
	var quota *Quota
	var scan bool = false
	switch qtype {
	case DirQuotaType:
		if !format.DirStats {
			format.DirStats = true
			if err := m.en.doInit(format, false); err != nil {
				return fmt.Errorf("enable dir stats: %s (%w)", err, syscall.EIO)
			}
		}
		quota = quotas[dpath]
	case UserQuotaType:
		if !format.UserGroupQuota {
			format.UserGroupQuota = true
			scan = true
			if err := m.en.doInit(format, false); err != nil {
				return fmt.Errorf("enable user group quota: %s (%w)", err, syscall.EIO)
			}
		}
		quota = quotas[fmt.Sprintf("%d", key)]
	case GroupQuotaType:
		if !format.UserGroupQuota {
			format.UserGroupQuota = true
			scan = true
			if err := m.en.doInit(format, false); err != nil {
				return fmt.Errorf("enable user group quota: %s (%w)", err, syscall.EIO)
			}
		}
		quota = quotas[fmt.Sprintf("%d", key)]
	}
	if quota == nil {
		return nil
	}

	created, err := m.en.doSetQuota(ctx, qtype, uint64(key), &Quota{
		MaxSpace:   quota.MaxSpace,
		MaxInodes:  quota.MaxInodes,
		UsedSpace:  -1,
		UsedInodes: -1,
	})
	if err != nil {
		return err
	}
	if qtype == DirQuotaType {
		m.noDirQuota.Store(false)
		m.quotaMu.Lock()
		delete(m.noDirQuotas, uint64(key))
		m.quotaMu.Unlock()
	}
	if created && qtype == DirQuotaType {
		return m.calcDirQuotaUsage(ctx, Ino(key), dpath, strict)
	} else if scan && (qtype == UserQuotaType || qtype == GroupQuotaType) {
		return m.ScanUserGroupUsage(ctx)
	} else {
		return nil
	}
}

func (m *baseMeta) calcDirQuotaUsage(ctx Context, ino Ino, dpath string, strict bool) error {
	wrapErr := func(e error) error {
		return errors.Wrapf(e, "set quota usage for file(%s), please repair it later", dpath)
	}

	var sum Summary
	if st := m.GetSummary(ctx, ino, &sum, true, strict); st != 0 {
		return wrapErr(st)
	}

	_, err := m.en.doSetQuota(ctx, DirQuotaType, uint64(ino), &Quota{
		UsedSpace:  int64(sum.Size) - align4K(0),
		UsedInodes: int64(sum.Dirs+sum.Files) - 1,
		MaxSpace:   -1,
		MaxInodes:  -1,
	})
	if err != nil {
		return wrapErr(err)
	}
	return nil
}

func (m *baseMeta) ScanUserGroupUsage(ctx Context) error {
	userUsage, groupUsage, err := m.scanGlobalUserGroupUsage(ctx)
	if err != nil {
		return fmt.Errorf("scan global user group usage: %v", err)
	}

	var userQuotasSnapshot map[uint64]*Quota
	var groupQuotasSnapshot map[uint64]*Quota

	m.quotaMu.Lock()
	// Reset user and group quotas
	m.userQuotas = make(map[uint64]*Quota)
	m.groupQuotas = make(map[uint64]*Quota)
	for uid, usage := range userUsage {
		m.userQuotas[uid] = &Quota{
			MaxSpace:   -1,
			MaxInodes:  -1,
			UsedSpace:  int64(usage.Size),
			UsedInodes: int64(usage.Files),
		}
	}
	for gid, usage := range groupUsage {
		m.groupQuotas[gid] = &Quota{
			MaxSpace:   -1,
			MaxInodes:  -1,
			UsedSpace:  int64(usage.Size),
			UsedInodes: int64(usage.Files),
		}
	}

	userQuotasSnapshot = make(map[uint64]*Quota)
	for uid, quota := range m.userQuotas {
		userQuotasSnapshot[uid] = &Quota{
			MaxSpace:   atomic.LoadInt64(&quota.MaxSpace),
			MaxInodes:  atomic.LoadInt64(&quota.MaxInodes),
			UsedSpace:  atomic.LoadInt64(&quota.UsedSpace),
			UsedInodes: atomic.LoadInt64(&quota.UsedInodes),
		}
	}

	groupQuotasSnapshot = make(map[uint64]*Quota)
	for gid, quota := range m.groupQuotas {
		groupQuotasSnapshot[gid] = &Quota{
			MaxSpace:   atomic.LoadInt64(&quota.MaxSpace),
			MaxInodes:  atomic.LoadInt64(&quota.MaxInodes),
			UsedSpace:  atomic.LoadInt64(&quota.UsedSpace),
			UsedInodes: atomic.LoadInt64(&quota.UsedInodes),
		}
	}
	m.quotaMu.Unlock()

	for uid, quota := range userQuotasSnapshot {
		_, err := m.en.doSetQuota(ctx, UserQuotaType, uid, quota)
		if err != nil {
			logger.Warnf("Failed to save user quota for uid %d: %v", uid, err)
		}
	}

	for gid, quota := range groupQuotasSnapshot {
		_, err := m.en.doSetQuota(ctx, GroupQuotaType, gid, quota)
		if err != nil {
			logger.Warnf("Failed to save group quota for gid %d: %v", gid, err)
		}
	}

	return nil
}

func (m *baseMeta) scanGlobalUserGroupUsage(ctx Context) (map[uint64]*Summary, map[uint64]*Summary, error) {
	userUsage := make(map[uint64]*Summary)
	groupUsage := make(map[uint64]*Summary)

	processedFiles := make(map[Ino]bool)
	visitedDirs := make(map[Ino]bool)

	dirQueue := []Ino{RootInode}
	if m.getFormat().TrashDays > 0 {
		var trashAttr Attr
		if st := m.en.doGetAttr(ctx, TrashInode, &trashAttr); st == 0 {
			dirQueue = append(dirQueue, TrashInode)
		}
	}

	for len(dirQueue) > 0 {
		currentDir := dirQueue[0]
		dirQueue = dirQueue[1:]

		var entries []*Entry
		visitedDirs[currentDir] = true
		err := m.readdirHot(ctx, currentDir, 1, &entries, -1)
		if err != 0 {
			logger.Warnf("readdir %d: %s", currentDir, err)
			continue
		}

		for _, e := range entries {
			if string(e.Name) == "." || string(e.Name) == ".." {
				continue
			}

			uid, gid := uint64(e.Attr.Uid), uint64(e.Attr.Gid)

			if userUsage[uid] == nil {
				userUsage[uid] = &Summary{}
			}
			if groupUsage[gid] == nil {
				groupUsage[gid] = &Summary{}
			}

			var space int64
			var inodes int64
			if e.Attr.Typ == TypeFile {
				if e.Attr.Nlink > 1 {
					if processedFiles[e.Inode] {
						space = 0
						inodes = 0
					} else {
						space = align4K(e.Attr.Length)
						inodes = 1
						processedFiles[e.Inode] = true
					}
				} else {
					space = align4K(e.Attr.Length)
					inodes = 1
				}
			} else if e.Attr.Typ == TypeDirectory {
				space = align4K(0)
				inodes = 1
				userUsage[uid].Dirs++
				groupUsage[gid].Dirs++
				if !visitedDirs[e.Inode] {
					dirQueue = append(dirQueue, e.Inode)
				}
			}

			userUsage[uid].Size += uint64(space)
			userUsage[uid].Files += uint64(inodes)
			groupUsage[gid].Size += uint64(space)
			groupUsage[gid].Files += uint64(inodes)

		}
	}

	if err := m.en.doScanSustainedInodes(ctx, func(uid, gid uint32, length uint64) error {
		u, g := uint64(uid), uint64(gid)
		if userUsage[u] == nil {
			userUsage[u] = &Summary{}
		}
		if groupUsage[g] == nil {
			groupUsage[g] = &Summary{}
		}
		space := align4K(length)
		userUsage[u].Size += uint64(space)
		userUsage[u].Files++
		groupUsage[g].Size += uint64(space)
		groupUsage[g].Files++
		return nil
	}); err != nil {
		logger.Warnf("scan sustained inodes for user/group quota: %v", err)
	}

	return userUsage, groupUsage, nil
}

func (m *baseMeta) handleQuotaGet(ctx Context, qtype uint32, key uint64, dpath string, quotas map[string]*Quota) error {
	q, err := m.en.doGetQuota(ctx, qtype, key)
	if err != nil {
		return err
	}
	if q == nil {
		return nil
	}
	switch qtype {
	case DirQuotaType:
		quotas[dpath] = q
	case UserQuotaType:
		quotas[fmt.Sprintf("%d", key)] = q
	case GroupQuotaType:
		quotas[fmt.Sprintf("%d", key)] = q
	}
	return nil
}

func (m *baseMeta) handleQuotaList(ctx Context, qtype uint32, key uint64, quotas map[string]*Quota) error {
	dirQuotas, userQuotas, groupQuotas, err := m.en.doLoadQuotas(ctx, quotaScope{allDirs: true, userGroup: true})
	if err != nil {
		return err
	}

	match := func(targetType uint32, k uint64, v *Quota) bool {
		if v.MaxInodes == -1 && v.MaxSpace == -1 {
			return false
		}
		if qtype == AllQuotaType {
			return true
		}
		return qtype == targetType && k == key
	}

	for ino, quota := range dirQuotas {
		if !match(DirQuotaType, ino, quota) {
			continue
		}
		if ps := m.GetPaths(ctx, Ino(ino)); len(ps) > 0 {
			quotas[ps[0]] = quota
		} else {
			quotas[fmt.Sprintf("inode:%d", ino)] = quota
		}
	}
	for uid, quota := range userQuotas {
		if match(UserQuotaType, uid, quota) {
			quotas[fmt.Sprintf("uid:%d", uid)] = quota
		}
	}
	for gid, quota := range groupQuotas {
		if match(GroupQuotaType, gid, quota) {
			quotas[fmt.Sprintf("gid:%d", gid)] = quota
		}
	}
	return nil
}

func (m *baseMeta) handleQuotaCheck(ctx Context, qtype uint32, key uint64, dpath string, strict, repair bool, quotas map[string]*Quota) error {
	if qtype == UserQuotaType || qtype == GroupQuotaType {
		return m.checkUGUsage(ctx, repair, quotas)
	}
	return m.checkDirUsage(ctx, qtype, key, dpath, strict, repair, quotas)
}

func (m *baseMeta) checkDirUsage(ctx Context, qtype uint32, key uint64, dpath string, strict, repair bool, quotas map[string]*Quota) error {
	q, err := m.en.doGetQuota(ctx, qtype, key)
	if err != nil {
		return err
	}
	if q == nil {
		return fmt.Errorf("no quota for inode %d path %s", key, dpath)
	}

	var sum Summary
	if st := m.GetSummary(ctx, Ino(key), &sum, true, strict); st != 0 {
		return st
	}

	usedInodes := int64(sum.Dirs+sum.Files) - 1
	usedSpace := int64(sum.Size) - align4K(0) // quota ignore root dir

	if q.UsedInodes == usedInodes && q.UsedSpace == usedSpace {
		logger.Infof("quota of %s is consistent", dpath)
		quotas[dpath] = q
		return nil
	}

	logger.Warnf(
		"%s: quota(%s, %s) != summary(%s, %s)", dpath,
		humanize.Comma(q.UsedInodes), humanize.IBytes(uint64(q.UsedSpace)),
		humanize.Comma(usedInodes), humanize.IBytes(uint64(usedSpace)),
	)

	if repair {
		q.UsedInodes = usedInodes
		q.UsedSpace = usedSpace
		quotas[dpath] = q
		logger.Info("repairing...")
		_, err = m.en.doSetQuota(ctx, qtype, key, &Quota{
			MaxInodes:  -1,
			MaxSpace:   -1,
			UsedInodes: q.UsedInodes,
			UsedSpace:  q.UsedSpace,
		})
		return err
	}

	return fmt.Errorf("quota of %s is inconsistent, please repair it with --repair flag", dpath)
}

func (m *baseMeta) compareUGUsage(usageMap map[uint64]*Summary, quotaMap map[uint64]*Quota, qtype uint32, retQuotas map[string]*Quota) bool {
	var hasErr bool
	idType := "uid"
	if qtype == GroupQuotaType {
		idType = "gid"
	}
	for id, usage := range usageMap {
		usedSpace := int64(usage.Size)
		usedInodes := int64(usage.Files)
		q, ok := quotaMap[id]
		if !ok {
			logger.Warnf("%s:%d: quota not found, actual usage(%s, %s)",
				idType, id, humanize.Comma(usedInodes), humanize.IBytes(uint64(usedSpace)))
			hasErr = true
			continue
		}
		if q.UsedInodes != usedInodes || q.UsedSpace != usedSpace {
			logger.Warnf("%s:%d: usage(%s, %s) != actual usage(%s, %s)",
				idType, id,
				humanize.Comma(q.UsedInodes), humanize.IBytes(uint64(q.UsedSpace)),
				humanize.Comma(usedInodes), humanize.IBytes(uint64(usedSpace)))
			hasErr = true
			continue
		}
		retQuotas[fmt.Sprintf("%d", id)] = q
	}
	for id, q := range quotaMap {
		if _, ok := usageMap[id]; ok {
			continue
		}
		if q.UsedInodes != 0 || q.UsedSpace != 0 {
			logger.Warnf("%s:%d: usage missing, but quota usage is (%s, %s)",
				idType, id, humanize.Comma(q.UsedInodes), humanize.IBytes(uint64(q.UsedSpace)))
			hasErr = true
		}
	}
	return hasErr
}

func (m *baseMeta) repairUsage(ctx Context, usageMap map[uint64]*Summary, quotaMap map[uint64]*Quota, qtype uint32) error {
	for id, usage := range usageMap {
		quota := &Quota{
			MaxSpace:   -1,
			MaxInodes:  -1,
			UsedSpace:  int64(usage.Size),
			UsedInodes: int64(usage.Files),
		}
		if q, ok := quotaMap[id]; ok {
			quota.MaxSpace = q.MaxSpace
			quota.MaxInodes = q.MaxInodes
		}
		if _, err := m.en.doSetQuota(ctx, qtype, id, quota); err != nil {
			return err
		}
	}
	return nil
}

func (m *baseMeta) repairUgUsage(ctx Context, qtype uint32, usageMap map[uint64]*Summary, quotaMap map[uint64]*Quota) error {
	idType := "uid"
	if qtype == GroupQuotaType {
		idType = "gid"
	}
	if err := m.en.cleanUgUsage(ctx, qtype); err != nil {
		return fmt.Errorf("clean %s quotas: %w", idType, err)
	}
	if err := m.repairUsage(ctx, usageMap, quotaMap, qtype); err != nil {
		return fmt.Errorf("set %s quota: %w", idType, err)
	}
	return nil
}

func (m *baseMeta) checkUGUsage(ctx Context, repair bool, quotas map[string]*Quota) error {
	userUsage, groupUsage, err := m.scanGlobalUserGroupUsage(ctx)
	if err != nil {
		return fmt.Errorf("scan global user group usage: %w", err)
	}

	_, userQuotas, groupQuotas, err := m.en.doLoadQuotas(ctx, quotaScope{userGroup: true})
	if err != nil {
		return fmt.Errorf("load user/group quotas: %w", err)
	}
	hasErr := m.compareUGUsage(userUsage, userQuotas, UserQuotaType, quotas)
	hasErr = m.compareUGUsage(groupUsage, groupQuotas, GroupQuotaType, quotas) || hasErr

	if !repair {
		if hasErr {
			return fmt.Errorf("user/group quota is inconsistent, please repair it with --repair flag")
		}
		return nil
	}

	logger.Infof("Begin to repair user/group quota.")
	if err = m.repairUgUsage(ctx, UserQuotaType, userUsage, userQuotas); err != nil {
		return err
	}
	if err = m.repairUgUsage(ctx, GroupQuotaType, groupUsage, groupQuotas); err != nil {
		return err
	}

	return nil
}

func (m *baseMeta) updateQuotaMetrics() {
	m.quotaMu.RLock()
	dirQuotasSnapshot := make(map[uint64]Quota)
	for inode, quota := range m.dirQuotas {
		dirQuotasSnapshot[inode] = quota.snap()
	}

	userQuotasSnapshot := make(map[uint64]Quota)
	for uid, quota := range m.userQuotas {
		userQuotasSnapshot[uid] = quota.snap()
	}

	groupQuotasSnapshot := make(map[uint64]Quota)
	for gid, quota := range m.groupQuotas {
		groupQuotasSnapshot[gid] = quota.snap()
	}
	m.quotaMu.RUnlock()
	m.updateDirQuotaMetrics(dirQuotasSnapshot)
	m.updateUserQuotaMetrics(userQuotasSnapshot)
	m.updateGroupQuotaMetrics(groupQuotasSnapshot)
}

func (m *baseMeta) updateDirQuotaMetrics(dirQuotas map[uint64]Quota) {
	m.quotaMetricMu.Lock()
	defer m.quotaMetricMu.Unlock()
	for inode, quota := range dirQuotas {
		inodeStr := strconv.FormatUint(inode, 10)
		m.dirQuotaMaxSpaceG.WithLabelValues(inodeStr).Set(float64(quota.MaxSpace))
		m.dirQuotaMaxInodesG.WithLabelValues(inodeStr).Set(float64(quota.MaxInodes))
		m.dirQuotaUsedSpaceG.WithLabelValues(inodeStr).Set(float64(quota.UsedSpace))
		m.dirQuotaUsedInodesG.WithLabelValues(inodeStr).Set(float64(quota.UsedInodes))
		m.dirQuotaMetricKeys[inode] = true
	}
}

func (m *baseMeta) updateUserQuotaMetrics(userQuotas map[uint64]Quota) {
	m.quotaMetricMu.Lock()
	defer m.quotaMetricMu.Unlock()
	for uid, quota := range userQuotas {
		uidStr := strconv.FormatUint(uid, 10)
		m.userQuotaMaxSpaceG.WithLabelValues(uidStr).Set(float64(quota.MaxSpace))
		m.userQuotaMaxInodesG.WithLabelValues(uidStr).Set(float64(quota.MaxInodes))
		m.userQuotaUsedSpaceG.WithLabelValues(uidStr).Set(float64(quota.UsedSpace))
		m.userQuotaUsedInodesG.WithLabelValues(uidStr).Set(float64(quota.UsedInodes))
		m.userQuotaMetricKeys[uid] = true
	}
}

func (m *baseMeta) updateGroupQuotaMetrics(groupQuotas map[uint64]Quota) {
	m.quotaMetricMu.Lock()
	defer m.quotaMetricMu.Unlock()
	for gid, quota := range groupQuotas {
		gidStr := strconv.FormatUint(gid, 10)
		m.groupQuotaMaxSpaceG.WithLabelValues(gidStr).Set(float64(quota.MaxSpace))
		m.groupQuotaMaxInodesG.WithLabelValues(gidStr).Set(float64(quota.MaxInodes))
		m.groupQuotaUsedSpaceG.WithLabelValues(gidStr).Set(float64(quota.UsedSpace))
		m.groupQuotaUsedInodesG.WithLabelValues(gidStr).Set(float64(quota.UsedInodes))
		m.groupQuotaMetricKeys[gid] = true
	}
}

func (m *baseMeta) cleanupQuotaMetrics() {
	m.quotaMu.RLock()
	dirQuotas := make(map[uint64]Quota, len(m.dirQuotas))
	for inode, q := range m.dirQuotas {
		dirQuotas[inode] = q.snap()
	}
	userQuotas := make(map[uint64]Quota, len(m.userQuotas))
	for uid, q := range m.userQuotas {
		userQuotas[uid] = q.snap()
	}
	groupQuotas := make(map[uint64]Quota, len(m.groupQuotas))
	for gid, q := range m.groupQuotas {
		groupQuotas[gid] = q.snap()
	}
	m.quotaMu.RUnlock()

	m.quotaMetricMu.Lock()
	defer m.quotaMetricMu.Unlock()

	// directory quotas
	for inode := range m.dirQuotaMetricKeys {
		q, ok := dirQuotas[inode]
		if !ok || (q.MaxSpace <= 0 && q.MaxInodes <= 0) {
			inodeStr := strconv.FormatUint(inode, 10)
			m.dirQuotaMaxSpaceG.DeleteLabelValues(inodeStr)
			m.dirQuotaMaxInodesG.DeleteLabelValues(inodeStr)
			m.dirQuotaUsedSpaceG.DeleteLabelValues(inodeStr)
			m.dirQuotaUsedInodesG.DeleteLabelValues(inodeStr)
			delete(m.dirQuotaMetricKeys, inode)
		}
	}

	// user quotas
	for uid := range m.userQuotaMetricKeys {
		q, ok := userQuotas[uid]
		if !ok || (q.MaxSpace <= 0 && q.MaxInodes <= 0) {
			uidStr := strconv.FormatUint(uid, 10)
			m.userQuotaMaxSpaceG.DeleteLabelValues(uidStr)
			m.userQuotaMaxInodesG.DeleteLabelValues(uidStr)
			m.userQuotaUsedSpaceG.DeleteLabelValues(uidStr)
			m.userQuotaUsedInodesG.DeleteLabelValues(uidStr)
			delete(m.userQuotaMetricKeys, uid)
		}
	}

	// group quotas
	for gid := range m.groupQuotaMetricKeys {
		q, ok := groupQuotas[gid]
		if !ok || (q.MaxSpace <= 0 && q.MaxInodes <= 0) {
			gidStr := strconv.FormatUint(gid, 10)
			m.groupQuotaMaxSpaceG.DeleteLabelValues(gidStr)
			m.groupQuotaMaxInodesG.DeleteLabelValues(gidStr)
			m.groupQuotaUsedSpaceG.DeleteLabelValues(gidStr)
			m.groupQuotaUsedInodesG.DeleteLabelValues(gidStr)
			delete(m.groupQuotaMetricKeys, gid)
		}
	}
}
