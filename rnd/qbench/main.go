package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/redis/go-redis/v9"
)

type stats struct {
	Calls    map[string]int64 `json:"calls"`
	TotalCmd int64            `json:"total_cmd"`
	NetIn    int64            `json:"net_in"`
	NetOut   int64            `json:"net_out"`
	CPUUsec  int64            `json:"cpu_usec"`
}

func info(ctx context.Context, rdb *redis.Client) stats {
	s, err := rdb.Info(ctx, "commandstats", "stats", "cpu").Result()
	if err != nil {
		log.Fatalf("info: %s", err)
	}
	st := stats{Calls: map[string]int64{}}
	for _, line := range strings.Split(s, "\r\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		switch {
		case strings.HasPrefix(kv[0], "cmdstat_"):
			for _, f := range strings.Split(kv[1], ",") {
				if strings.HasPrefix(f, "calls=") {
					v, _ := strconv.ParseInt(strings.TrimPrefix(f, "calls="), 10, 64)
					st.Calls[strings.TrimPrefix(kv[0], "cmdstat_")] = v
					st.TotalCmd += v
				}
			}
		case kv[0] == "total_net_input_bytes":
			st.NetIn, _ = strconv.ParseInt(kv[1], 10, 64)
		case kv[0] == "total_net_output_bytes":
			st.NetOut, _ = strconv.ParseInt(kv[1], 10, 64)
		case kv[0] == "used_cpu_sys" || kv[0] == "used_cpu_user":
			f, _ := strconv.ParseFloat(kv[1], 64)
			st.CPUUsec += int64(f * 1e6)
		}
	}
	return st
}

func diff(a, b stats) stats {
	d := stats{Calls: map[string]int64{}, TotalCmd: b.TotalCmd - a.TotalCmd, NetIn: b.NetIn - a.NetIn, NetOut: b.NetOut - a.NetOut, CPUUsec: b.CPUUsec - a.CPUUsec}
	for k, v := range b.Calls {
		if v-a.Calls[k] > 0 {
			d.Calls[k] = v - a.Calls[k]
		}
	}
	return d
}

func heap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

func main() {
	metaURL := flag.String("meta", "redis://127.0.0.1:6380/2", "meta url")
	build := flag.String("build", "", "label")
	quotas := flag.Int("quotas", 1000, "number of dir quotas")
	active := flag.Int("active", 10, "active quota dirs touched by the client")
	periods := flag.Int("periods", 5, "heartbeat periods to observe")
	setup := flag.Bool("setup", true, "create quotas")
	flag.Parse()

	u, _ := url.Parse(*metaURL)
	db, _ := strconv.Atoi(strings.TrimPrefix(u.Path, "/"))
	rdb := redis.NewClient(&redis.Options{Addr: u.Host, DB: db})
	bg := context.Background()
	ctx := meta.Background()

	if *setup {
		m := meta.NewClient(*metaURL, meta.DefaultConf())
		if err := m.Reset(); err != nil {
			log.Fatalf("reset: %s", err)
		}
		if err := m.Init(&meta.Format{Name: "qbench", UUID: uuid.New().String(), Storage: "file", Bucket: "/tmp/qbench/", BlockSize: 4096, DirStats: true, TrashDays: 0}, true); err != nil {
			log.Fatalf("init: %s", err)
		}
		for i := 0; i < *quotas; i++ {
			var ino meta.Ino
			var attr meta.Attr
			name := fmt.Sprintf("p%07d", i)
			if st := m.Mkdir(ctx, meta.RootInode, name, 0755, 0, 0, &ino, &attr); st != 0 {
				log.Fatalf("mkdir: %s", st)
			}
			p := "/" + name
			if err := m.HandleQuota(ctx, meta.QuotaSet, p, meta.DirQuotaType, map[string]*meta.Quota{p: {MaxSpace: 1 << 40, MaxInodes: 1 << 30}}, false, false, false); err != nil {
				log.Fatalf("quota: %s", err)
			}
		}
	}

	conf := meta.DefaultConf()
	conf.Heartbeat = time.Second
	m := meta.NewClient(*metaURL, conf)
	if _, err := m.Load(true); err != nil {
		log.Fatalf("load: %s", err)
	}
	h0 := heap()
	s0 := info(bg, rdb)
	t0 := time.Now()
	if err := m.NewSession(true); err != nil {
		log.Fatalf("session: %s", err)
	}
	startup := time.Since(t0)
	s1 := info(bg, rdb)
	h1 := heap()

	dirs := make([]meta.Ino, *active)
	for i := range dirs {
		var attr meta.Attr
		if st := m.Lookup(ctx, meta.RootInode, fmt.Sprintf("p%07d", i), &dirs[i], &attr, true); st != 0 {
			log.Fatalf("lookup: %s", st)
		}
	}
	s2 := info(bg, rdb)
	var lat []time.Duration
	n := 0
	deadline := time.Now().Add(time.Duration(*periods) * conf.Heartbeat)
	for time.Now().Before(deadline) {
		for _, d := range dirs {
			var ino meta.Ino
			var attr meta.Attr
			t := time.Now()
			if st := m.Create(ctx, d, fmt.Sprintf("%s-%d-f%d", *build, time.Now().UnixNano(), n), 0644, 0, 0, &ino, &attr); st != 0 {
				log.Fatalf("create: %s", st)
			}
			lat = append(lat, time.Since(t))
			m.Close(ctx, ino)
			n++
		}
		time.Sleep(50 * time.Millisecond)
	}
	s3 := info(bg, rdb)
	h3 := heap()
	_ = m.CloseSession()

	var sum time.Duration
	var mx time.Duration
	for _, l := range lat {
		sum += l
		mx = max(mx, l)
	}
	steady := diff(s2, s3)
	res := map[string]any{
		"time":              time.Now().UTC().Format(time.RFC3339),
		"build":             *build,
		"quotas":            *quotas,
		"active":            *active,
		"periods":           *periods,
		"startup_ms":        float64(startup.Microseconds()) / 1000,
		"startup":           diff(s0, s1),
		"steady":            steady,
		"creates":           n,
		"steady_cmd_per_s":  float64(steady.TotalCmd) / float64(*periods),
		"steady_out_per_s":  float64(steady.NetOut) / float64(*periods),
		"create_avg_us":     float64(sum.Microseconds()) / float64(len(lat)),
		"create_max_us":     mx.Microseconds(),
		"heap_after_start":  int64(h1) - int64(h0),
		"heap_after_steady": int64(h3) - int64(h0),
	}
	_ = json.NewEncoder(os.Stdout).Encode(res)
	fmt.Fprintf(os.Stderr, "%s Q=%d startup=%.1fms startup_cmds=%d steady_cmd/s=%.0f steady_out/s=%.0f creates=%d avg=%.0fus heap=%d\n", *build, *quotas, float64(startup.Microseconds())/1000, diff(s0, s1).TotalCmd, float64(steady.TotalCmd)/float64(*periods), float64(steady.NetOut)/float64(*periods), n, float64(sum.Microseconds())/float64(len(lat)), int64(h3)-int64(h0))
}
