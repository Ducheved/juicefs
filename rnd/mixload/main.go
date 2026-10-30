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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/rnd/corpus"
)

type stats struct {
	Ops   int     `json:"ops"`
	Rate  float64 `json:"ops_per_s"`
	P50us float64 `json:"p50_us"`
	P99us float64 `json:"p99_us"`
}

func summarize(d []time.Duration, secs float64) stats {
	if len(d) == 0 {
		return stats{}
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	at := func(p float64) float64 { return float64(d[int(float64(len(d)-1)*p)].Microseconds()) }
	return stats{Ops: len(d), Rate: float64(len(d)) / secs, P50us: at(0.5), P99us: at(0.99)}
}

func main() {
	metaURL := flag.String("meta", "redis://127.0.0.1:6379/1", "meta url")
	users := flag.Int("users", 2, "users created by nsgen")
	dirs := flag.Int("dirs", 4, "dirs per user")
	files := flag.Int("files", 500, "files per dir")
	random := flag.Bool("random", false, "names from rnd/corpus as created by nsgen -random")
	readers := flag.Int("readers", 32, "lookup goroutines")
	writers := flag.Int("writers", 8, "create+unlink goroutines")
	seconds := flag.Int("seconds", 20, "duration")
	label := flag.String("label", "", "label")
	flag.Parse()

	m := meta.NewClient(*metaURL, meta.DefaultConf())
	if _, err := m.Load(true); err != nil {
		log.Fatalf("load: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		log.Fatalf("session: %s", err)
	}
	defer m.CloseSession()
	ctx := meta.Background()

	dirIno := make([][]meta.Ino, *users)
	for u := 0; u < *users; u++ {
		var uino meta.Ino
		var attr meta.Attr
		if st := m.Lookup(ctx, meta.RootInode, fmt.Sprintf("u%06d", u), &uino, &attr, false); st != 0 {
			log.Fatalf("lookup user %d: %s", u, st)
		}
		dirIno[u] = make([]meta.Ino, *dirs)
		for d := 0; d < *dirs; d++ {
			if st := m.Lookup(ctx, uino, corpus.DirName(*random, u, d), &dirIno[u][d], &attr, false); st != 0 {
				log.Fatalf("lookup dir %d/%d: %s", u, d, st)
			}
		}
	}

	deadline := time.Now().Add(time.Duration(*seconds) * time.Second)
	var mu sync.Mutex
	var wg sync.WaitGroup
	var lookups, creates []time.Duration
	for r := 0; r < *readers; r++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			var local []time.Duration
			for time.Now().Before(deadline) {
				u, d, k := rng.Intn(*users), rng.Intn(*dirs), rng.Intn(*files)
				name := corpus.FileName(*random, u**dirs**files+d**files+k, k)
				var ino meta.Ino
				var attr meta.Attr
				t := time.Now()
				if st := m.Lookup(ctx, dirIno[u][d], name, &ino, &attr, false); st != 0 {
					log.Fatalf("lookup %s: %s", name, st)
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			lookups = append(lookups, local...)
			mu.Unlock()
		}(int64(r))
	}
	for w := 0; w < *writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(1000 + w)))
			var local []time.Duration
			for i := 0; time.Now().Before(deadline); i++ {
				dir := dirIno[rng.Intn(*users)][rng.Intn(*dirs)]
				name := fmt.Sprintf("mix-%d-%d", w, i)
				var ino meta.Ino
				var attr meta.Attr
				t := time.Now()
				if st := m.Create(ctx, dir, name, 0644, 0, 0, &ino, &attr); st != 0 {
					log.Fatalf("create: %s", st)
				}
				local = append(local, time.Since(t))
				m.Close(ctx, ino)
				if st := m.Unlink(ctx, dir, name); st != 0 {
					log.Fatalf("unlink: %s", st)
				}
			}
			mu.Lock()
			creates = append(creates, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	secs := float64(*seconds)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"label": *label, "lookup": summarize(lookups, secs), "create": summarize(creates, secs)})
}
