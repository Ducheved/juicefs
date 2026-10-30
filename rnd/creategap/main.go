package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/rnd/corpus"
)

func main() {
	url := flag.String("meta", "", "")
	n := flag.Int("n", 2000, "")
	unlink := flag.Bool("unlink", true, "")
	gap := flag.String("gap", "none", "none|lookup|getattr|sleep")
	flag.Parse()
	m := meta.NewClient(*url, meta.DefaultConf())
	if _, err := m.Load(true); err != nil {
		log.Fatal(err)
	}
	if err := m.NewSession(true); err != nil {
		log.Fatal(err)
	}
	defer m.CloseSession()
	ctx := meta.Background()
	var u, dir meta.Ino
	var attr meta.Attr
	if st := m.Lookup(ctx, 1, "u000000", &u, &attr, false); st != 0 {
		log.Fatal(st)
	}
	dn := corpus.DirName(true, 0, 0)
	if st := m.Lookup(ctx, u, dn, &dir, &attr, false); st != 0 {
		log.Fatal(st)
	}
	var lat []time.Duration
	for i := 0; i < *n; i++ {
		var ino, x meta.Ino
		var a meta.Attr
		switch *gap {
		case "lookup":
			m.Lookup(ctx, u, dn, &x, &a, false)
		case "getattr":
			m.GetAttr(ctx, dir, &a)
		case "sleep":
			time.Sleep(300 * time.Microsecond)
		}
		name := fmt.Sprintf("abx-%d-%d", time.Now().UnixNano(), i)
		t := time.Now()
		if st := m.Create(ctx, dir, name, 0644, 0, 0, &ino, &a); st != 0 {
			log.Fatal(st)
		}
		lat = append(lat, time.Since(t))
		m.Close(ctx, ino)
		if !*unlink {
			continue
		}
		if st := m.Unlink(ctx, dir, name); st != 0 {
			log.Fatal(st)
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	fmt.Printf("unlink=%v gap=%s create_p50_us=%d p99_us=%d\n", *unlink, *gap, lat[len(lat)/2].Microseconds(), lat[len(lat)*99/100].Microseconds())
}
