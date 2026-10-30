package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

func pct(d []time.Duration, p float64) float64 {
	if len(d) == 0 {
		return 0
	}
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	return float64(d[int(float64(len(d)-1)*p)].Microseconds())
}

func main() {
	metaURL := flag.String("meta", "redis://127.0.0.1:6380/5", "meta url")
	users := flag.Int("users", 10, "hot users u000000..")
	dirs := flag.Int("dirs", 100, "dirs per user")
	files := flag.Int("files", 100, "files per dir")
	n := flag.Int("n", 5000, "operations per kind")
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
	var stat, create []time.Duration
	for i := 0; i < *n; i++ {
		p := fmt.Sprintf("/u%06d/d%04d/f%05d", rand.Intn(*users), rand.Intn(*dirs), rand.Intn(*files))
		var ino meta.Ino
		var attr meta.Attr
		t := time.Now()
		if st := resolve(m, p, &ino, &attr); st != 0 {
			log.Fatalf("resolve %s: %s", p, st)
		}
		if st := m.GetAttr(ctx, ino, &attr); st != 0 {
			log.Fatalf("getattr %s: %s", p, st)
		}
		stat = append(stat, time.Since(t))
	}
	for i := 0; i < *n; i++ {
		var dir, ino meta.Ino
		var attr meta.Attr
		p := fmt.Sprintf("/u%06d/d%04d", rand.Intn(*users), rand.Intn(*dirs))
		if st := resolve(m, p, &dir, &attr); st != 0 {
			log.Fatalf("resolve %s: %s", p, st)
		}
		name := fmt.Sprintf("probe-%d", i)
		t := time.Now()
		if st := m.Create(ctx, dir, name, 0644, 0, 0, &ino, &attr); st != 0 {
			log.Fatalf("create: %s", st)
		}
		create = append(create, time.Since(t))
		m.Close(ctx, ino)
		if st := m.Unlink(ctx, dir, name); st != 0 {
			log.Fatalf("unlink: %s", st)
		}
	}
	res := map[string]any{"label": *label, "stat_p50_us": pct(stat, 0.5), "stat_p99_us": pct(stat, 0.99), "create_p50_us": pct(create, 0.5), "create_p99_us": pct(create, 0.99)}
	_ = json.NewEncoder(os.Stdout).Encode(res)
}

func resolve(m meta.Meta, p string, ino *meta.Ino, attr *meta.Attr) syscall.Errno {
	cur := meta.RootInode
	for _, name := range strings.Split(strings.Trim(p, "/"), "/") {
		if st := m.Lookup(meta.Background(), cur, name, &cur, attr, false); st != 0 {
			return st
		}
	}
	*ino = cur
	return 0
}
