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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

func main() {
	url := flag.String("meta", "", "meta url")
	clients := flag.Int("clients", 1000, "meta clients, each with its own session")
	active := flag.Float64("active", 0.2, "fraction of clients doing create/stat/unlink")
	rate := flag.Float64("rate", 10, "operations per second per active client")
	quotas := flag.Int("quotas", 100, "directory quotas")
	heartbeat := flag.Duration("heartbeat", 12*time.Second, "session heartbeat")
	duration := flag.Duration("duration", 2*time.Minute, "measured load after all sessions are up")
	label := flag.String("label", "", "label")
	flag.Parse()

	conf := meta.DefaultConf()
	conf.Heartbeat = *heartbeat
	conf.NoBGJob = true
	ctx := meta.Background()
	t0 := time.Now()
	ms := make([]meta.Meta, *clients)
	var wg sync.WaitGroup
	var setupErr atomic.Value
	sem := make(chan struct{}, 32)
	for i := range ms {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			m := meta.NewClient(*url, conf)
			if _, err := m.Load(true); err != nil {
				setupErr.Store(fmt.Errorf("load %d: %s", i, err))
				return
			}
			if err := m.NewSession(true); err != nil {
				setupErr.Store(fmt.Errorf("session %d: %s", i, err))
				return
			}
			ms[i] = m
		}(i)
	}
	wg.Wait()
	if err := setupErr.Load(); err != nil {
		log.Fatal(err)
	}
	sessions := time.Since(t0)

	t1 := time.Now()
	var fleet meta.Ino
	var fattr meta.Attr
	if st := ms[0].Mkdir(ctx, meta.RootInode, "fleet", 0755, 0, 0, &fleet, &fattr); st != 0 && st != syscall.EEXIST {
		log.Fatalf("mkdir fleet: %s", st)
	}
	if *quotas == 0 {
		if st := ms[0].Mkdir(ctx, fleet, "noquota", 0755, 0, 0, new(meta.Ino), &fattr); st != 0 && st != syscall.EEXIST {
			log.Fatalf("mkdir noquota: %s", st)
		}
	}
	for q := 0; q < *quotas; q++ {
		p := fmt.Sprintf("/fleet/q%d", q)
		if err := ms[0].HandleQuota(ctx, meta.QuotaSet, p, meta.DirQuotaType, map[string]*meta.Quota{p: {MaxSpace: 1 << 40, MaxInodes: 1 << 30}}, false, false, true); err != nil {
			log.Fatalf("quota %s: %s", p, err)
		}
	}
	nActive := int(float64(*clients) * *active)
	dirs := make([]meta.Ino, nActive)
	for i := range dirs {
		var q meta.Ino
		var attr meta.Attr
		if st := ms[i].Lookup(ctx, meta.RootInode, "fleet", &q, &attr, false); st != 0 {
			log.Fatalf("lookup fleet: %s", st)
		}
		qname := "noquota"
		if *quotas > 0 {
			qname = fmt.Sprintf("q%d", i%*quotas)
		}
		if st := ms[i].Lookup(ctx, q, qname, &q, &attr, false); st != 0 {
			log.Fatalf("lookup %s: %s", qname, st)
		}
		if st := ms[i].Mkdir(ctx, q, fmt.Sprintf("c%d", i), 0755, 0, 0, &dirs[i], &attr); st != 0 {
			log.Fatalf("mkdir c%d: %s", i, st)
		}
	}
	prepare := time.Since(t1)

	var mu sync.Mutex
	var lat []time.Duration
	var errs atomic.Int64
	var ops atomic.Int64
	stop := time.Now().Add(*duration)
	start := time.Now()
	for i := 0; i < nActive; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := ms[i]
			rng := rand.New(rand.NewSource(int64(i)))
			var local []time.Duration
			gap := time.Duration(float64(time.Second) / *rate)
			time.Sleep(time.Duration(rng.Int63n(int64(gap))))
			for n := 0; time.Now().Before(stop); n++ {
				next := time.Now().Add(gap)
				name := fmt.Sprintf("f%d", n)
				var ino, got meta.Ino
				var attr meta.Attr
				s := time.Now()
				st := m.Create(ctx, dirs[i], name, 0644, 0, 0, &ino, &attr)
				if st == 0 {
					m.Close(ctx, ino)
					st = m.Lookup(ctx, dirs[i], name, &got, &attr, true)
				}
				if st == 0 {
					st = m.Unlink(ctx, dirs[i], name)
				}
				if st != 0 {
					errs.Add(1)
				} else {
					local = append(local, time.Since(s))
					ops.Add(1)
				}
				time.Sleep(time.Until(next))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)].Microseconds()
	}
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"label":         *label,
		"clients":       *clients,
		"active":        nActive,
		"quotas":        *quotas,
		"heartbeat_s":   heartbeat.Seconds(),
		"sessions_up_s": sessions.Seconds(),
		"prepare_s":     prepare.Seconds(),
		"cycles":        ops.Load(),
		"cycles_per_s":  float64(ops.Load()) / elapsed.Seconds(),
		"errors":        errs.Load(),
		"cycle_p50_us":  pct(0.5),
		"cycle_p99_us":  pct(0.99),
		"cycle_max_us":  pct(1),
		"measured_s":    elapsed.Seconds(),
	})
	for _, m := range ms {
		_ = m.CloseSession()
	}
	if errs.Load() > 0 {
		os.Exit(1)
	}
}
