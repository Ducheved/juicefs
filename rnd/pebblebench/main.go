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

package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/rnd/corpus"
)

type layout struct {
	users, dirs, files int
}

func (l layout) perUser() uint64 { return uint64(1 + l.dirs*(1+l.files)) }

func (l layout) userIno(u int) meta.Ino { return meta.Ino(2 + uint64(u)*l.perUser()) }

func (l layout) dirIno(u, d int) meta.Ino {
	return l.userIno(u) + meta.Ino(1+d*(1+l.files))
}

func (l layout) fileIno(u, d, k int) meta.Ino { return l.dirIno(u, d) + meta.Ino(1+k) }

func (l layout) seq(u, d, k int) int { return u*l.dirs*l.files + d*l.files + k }

func ino(b []byte, i meta.Ino) []byte {
	return binary.LittleEndian.AppendUint64(b, uint64(i))
}

func inodeKey(i meta.Ino) []byte { return append(ino([]byte{'A'}, i), 'I') }

func entryKey(parent meta.Ino, name string) []byte {
	return append(append(ino([]byte{'A'}, parent), 'D'), name...)
}

func chunkKey(i meta.Ino) []byte {
	return binary.BigEndian.AppendUint32(append(ino([]byte{'A'}, i), 'C'), 0)
}

func dirStatKey(i meta.Ino) []byte { return ino([]byte{'U'}, i) }

func entry(typ uint8, i meta.Ino) []byte {
	return binary.BigEndian.AppendUint64([]byte{typ}, uint64(i))
}

func slice(id uint64, size uint32) []byte {
	b := binary.BigEndian.AppendUint32(nil, 0)
	b = binary.BigEndian.AppendUint64(b, id)
	b = binary.BigEndian.AppendUint32(b, size)
	b = binary.BigEndian.AppendUint32(b, 0)
	return binary.BigEndian.AppendUint32(b, size)
}

func attr(typ uint8, mode uint16, u int, length uint64, parent meta.Ino, ts int64) []byte {
	a := meta.Attr{Typ: typ, Mode: mode, Uid: uint32(1000 + u), Gid: uint32(1000 + u), Atime: ts, Mtime: ts, Ctime: ts,
		Atimensec: uint32(ts % 1e9), Mtimensec: uint32(ts % 1e9), Ctimensec: uint32(ts % 1e9), Nlink: 1, Length: length, Parent: parent}
	if typ == meta.TypeDirectory {
		a.Nlink = 2
	}
	return a.Marshal()
}

func cpuSeconds() float64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
}

func pct(d []time.Duration, p float64) int64 {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return d[int(float64(len(d)-1)*p)].Microseconds()
}

func emit(v map[string]any) {
	_ = json.NewEncoder(os.Stdout).Encode(v)
}

func load(db *pebble.DB, l layout, threads int) {
	start, c0 := time.Now(), cpuSeconds()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	users := make(chan int, l.users)
	for u := 0; u < l.users; u++ {
		users <- u
	}
	close(users)
	var done atomic.Int64
	var wg sync.WaitGroup
	for t := 0; t < threads; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range users {
				b := db.NewBatch()
				uino := l.userIno(u)
				b.Set(entryKey(meta.RootInode, fmt.Sprintf("u%06d", u)), entry(meta.TypeDirectory, uino), nil)
				b.Set(inodeKey(uino), attr(meta.TypeDirectory, 0755, u, 4096, meta.RootInode, base), nil)
				for d := 0; d < l.dirs; d++ {
					dino := l.dirIno(u, d)
					b.Set(entryKey(uino, corpus.DirName(true, u, d)), entry(meta.TypeDirectory, dino), nil)
					b.Set(inodeKey(dino), attr(meta.TypeDirectory, 0755, u, 4096, uino, base+int64(d)), nil)
					var space int64
					for k := 0; k < l.files; k++ {
						seq := l.seq(u, d, k)
						fino := l.fileIno(u, d, k)
						size := corpus.FileSize(true, seq)
						b.Set(entryKey(dino, corpus.FileName(true, seq, k)), entry(meta.TypeFile, fino), nil)
						b.Set(inodeKey(fino), attr(meta.TypeFile, 0644, u, uint64(size), dino, base+int64(seq)), nil)
						b.Set(chunkKey(fino), slice(uint64(seq+1), size), nil)
						space += int64((size + 4095) / 4096 * 4096)
					}
					st := binary.BigEndian.AppendUint64(nil, uint64(space))
					st = binary.BigEndian.AppendUint64(st, uint64(space))
					st = binary.BigEndian.AppendUint64(st, uint64(l.files))
					b.Set(dirStatKey(dino), st, nil)
				}
				if err := b.Commit(pebble.NoSync); err != nil {
					log.Fatalf("commit: %s", err)
				}
				_ = b.Close()
				if n := done.Add(1); n%100 == 0 {
					log.Printf("loaded %d users, %.0f files/s", n, float64(n)*float64(l.dirs*l.files)/time.Since(start).Seconds())
				}
			}
		}()
	}
	wg.Wait()
	if err := db.Flush(); err != nil {
		log.Fatalf("flush: %s", err)
	}
	emit(map[string]any{"phase": "load", "files": l.users * l.dirs * l.files, "seconds": time.Since(start).Seconds(), "cpu_s": cpuSeconds() - c0})
}

func settle(db *pebble.DB) {
	start := time.Now()
	for {
		m := db.Metrics()
		if m.Compact.NumInProgress == 0 && m.Compact.EstimatedDebt == 0 {
			break
		}
		time.Sleep(5 * time.Second)
	}
	m := db.Metrics()
	emit(map[string]any{"phase": "settle", "seconds": time.Since(start).Seconds(), "disk_bytes": m.DiskSpaceUsage(),
		"block_cache_bytes": m.BlockCache.Size, "memtable_bytes": m.MemTable.Size})
	fmt.Fprintln(os.Stderr, m.String())
}

func lookup(db *pebble.DB, parent meta.Ino, name string) error {
	v, closer, err := db.Get(entryKey(parent, name))
	if err != nil {
		return fmt.Errorf("entry %d/%s: %w", parent, name, err)
	}
	i := meta.Ino(binary.BigEndian.Uint64(v[1:]))
	_ = closer.Close()
	_, closer, err = db.Get(inodeKey(i))
	if err != nil {
		return fmt.Errorf("inode %d: %w", i, err)
	}
	return closer.Close()
}

func read(db *pebble.DB, l layout, users, readers int, secs float64, label string) {
	deadline := time.Now().Add(time.Duration(secs * float64(time.Second)))
	var mu sync.Mutex
	var all []time.Duration
	var wg sync.WaitGroup
	c0 := cpuSeconds()
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var local []time.Duration
			for time.Now().Before(deadline) {
				u, d, k := rng.Intn(users), rng.Intn(l.dirs), rng.Intn(l.files)
				t := time.Now()
				if err := lookup(db, l.dirIno(u, d), corpus.FileName(true, l.seq(u, d, k), k)); err != nil {
					log.Fatalf("lookup: %s", err)
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(int64(r + 1))
	}
	wg.Wait()
	cpu := cpuSeconds() - c0
	m := db.Metrics()
	emit(map[string]any{"phase": "read", "label": label, "readers": readers, "ops_per_s": float64(len(all)) / secs,
		"p50_us": pct(all, 0.5), "p99_us": pct(all, 0.99), "cpu_us_per_op": cpu * 1e6 / float64(len(all)),
		"block_cache_hit_ratio": float64(m.BlockCache.Hits) / float64(m.BlockCache.Hits+m.BlockCache.Misses)})
}

func write(db *pebble.DB, l layout, writers int, secs float64) {
	deadline := time.Now().Add(time.Duration(secs * float64(time.Second)))
	var mu sync.Mutex
	var all []time.Duration
	var wg sync.WaitGroup
	var next atomic.Uint64
	next.Store(1 << 40)
	c0 := cpuSeconds()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + w)))
			var local []time.Duration
			for i := 0; time.Now().Before(deadline); i++ {
				u, d := rng.Intn(l.users), rng.Intn(l.dirs)
				parent := l.dirIno(u, d)
				name := "w" + strconv.Itoa(w) + "-" + strconv.Itoa(i) + "-" + strconv.FormatInt(time.Now().UnixNano(), 36)
				t := time.Now()
				pv, closer, err := db.Get(inodeKey(parent))
				if err != nil {
					log.Fatalf("parent: %s", err)
				}
				pattr := append([]byte(nil), pv...)
				_ = closer.Close()
				if _, closer, err := db.Get(entryKey(parent, name)); err == nil {
					_ = closer.Close()
					log.Fatalf("exists: %s", name)
				} else if err != pebble.ErrNotFound {
					log.Fatalf("entry: %s", err)
				}
				fino := meta.Ino(next.Add(1))
				now := time.Now().Unix()
				b := db.NewBatch()
				_ = b.Set(entryKey(parent, name), entry(meta.TypeFile, fino), nil)
				_ = b.Set(inodeKey(fino), attr(meta.TypeFile, 0644, u, 0, parent, now), nil)
				_ = b.Set(inodeKey(parent), pattr, nil)
				if err := b.Commit(pebble.Sync); err != nil {
					log.Fatalf("commit: %s", err)
				}
				_ = b.Close()
				local = append(local, time.Since(t))
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	cpu := cpuSeconds() - c0
	m := db.Metrics()
	emit(map[string]any{"phase": "write", "writers": writers, "txn_per_s": float64(len(all)) / secs,
		"p50_us": pct(all, 0.5), "p99_us": pct(all, 0.99), "cpu_us_per_txn": cpu * 1e6 / float64(len(all)),
		"wal_bytes": m.WAL.BytesWritten})
}

func main() {
	dir := flag.String("dir", "/srv/stand/stand/pebble", "data dir")
	users := flag.Int("users", 3900, "top level dirs")
	dirs := flag.Int("dirs", 100, "dirs per user")
	files := flag.Int("files", 100, "files per dir")
	cacheMB := flag.Int64("cache-mb", 1024, "block cache")
	memtableMB := flag.Uint64("memtable-mb", 64, "memtable size")
	phases := flag.String("phases", "load,settle,read,write", "phases")
	threads := flag.Int("threads", 2, "load threads")
	readers := flag.Int("readers", 8, "read goroutines")
	writers := flag.String("writers", "1,4,16,64,256", "writer counts")
	secs := flag.Float64("seconds", 20, "seconds per step")
	flag.Parse()

	cache := pebble.NewCache(*cacheMB << 20)
	defer cache.Unref()
	opts := &pebble.Options{Cache: cache, MemTableSize: *memtableMB << 20, MemTableStopWritesThreshold: 4}
	opts.EnsureDefaults()
	for i := range opts.Levels {
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
	}
	db, err := pebble.Open(*dir, opts)
	if err != nil {
		log.Fatalf("open: %s", err)
	}
	defer db.Close()
	l := layout{*users, *dirs, *files}
	for _, p := range strings.Split(*phases, ",") {
		switch p {
		case "load":
			load(db, l, *threads)
		case "settle":
			settle(db)
		case "read":
			read(db, l, 1, *readers, *secs, "hot10k")
			read(db, l, l.users, *readers, *secs, "all")
		case "write":
			for _, w := range strings.Split(*writers, ",") {
				n, err := strconv.Atoi(w)
				if err != nil {
					log.Fatalf("writers %q: %s", w, err)
				}
				write(db, l, n, *secs)
			}
		default:
			log.Fatalf("unknown phase %s", p)
		}
	}
}
