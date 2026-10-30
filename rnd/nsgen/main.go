package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/rnd/corpus"
	"github.com/redis/go-redis/v9"
)

type shape struct {
	Users        int     `json:"users"`
	DirsPerUser  int     `json:"dirs_per_user"`
	FilesPerDir  int     `json:"files_per_dir"`
	Chunks       int     `json:"chunks"`
	Slices       int     `json:"slices"`
	XattrRatio   float64 `json:"xattr_ratio"`
	SymlinkRatio float64 `json:"symlink_ratio"`
	LinkRatio    float64 `json:"hardlink_ratio"`
	UserQuota    bool    `json:"user_quota"`
	DirQuota     bool    `json:"dir_quota"`
	Threads      int     `json:"threads"`
}

type family struct {
	Keys  int64 `json:"keys"`
	Bytes int64 `json:"bytes"`
}

type result struct {
	Time       string             `json:"time"`
	Meta       string             `json:"meta"`
	Server     string             `json:"server"`
	Shape      shape              `json:"shape"`
	Files      int64              `json:"files"`
	Dirs       int64              `json:"dirs"`
	Symlinks   int64              `json:"symlinks"`
	Hardlinks  int64              `json:"hardlinks"`
	Inodes     int64              `json:"inodes"`
	GenSeconds float64            `json:"gen_seconds"`
	Before     map[string]int64   `json:"memory_before"`
	After      map[string]int64   `json:"memory_after"`
	Families   map[string]*family `json:"families,omitempty"`
	PerFile    float64            `json:"used_memory_per_file"`
	PerInode   float64            `json:"used_memory_per_inode"`
}

var families = []struct {
	name string
	re   *regexp.Regexp
}{
	{"inode", regexp.MustCompile(`^i\d+$`)},
	{"dentry", regexp.MustCompile(`^d\d+$`)},
	{"parent", regexp.MustCompile(`^p\d+$`)},
	{"chunk", regexp.MustCompile(`^c\d+_\d+$`)},
	{"xattr", regexp.MustCompile(`^x\d+$`)},
	{"symlink", regexp.MustCompile(`^s\d+$`)},
}

func classify(key, prefix string) string {
	k := strings.TrimPrefix(key, prefix)
	for _, f := range families {
		if f.re.MatchString(k) {
			return f.name
		}
	}
	return k
}

func memInfo(ctx context.Context, rdb redis.UniversalClient) (map[string]int64, string) {
	out := make(map[string]int64)
	var s string
	var err error
	for i := 0; ; i++ {
		if s, err = rdb.Info(ctx, "memory", "server", "keyspace").Result(); err != nil {
			log.Fatalf("info: %s", err)
		}
		if !strings.Contains(s, "lazyfree_pending_objects:") || strings.Contains(s, "lazyfree_pending_objects:0\r\n") || i > 600 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	server := ""
	for _, line := range strings.Split(s, "\r\n") {
		kv := strings.SplitN(line, ":", 2)
		if len(kv) != 2 {
			continue
		}
		switch kv[0] {
		case "used_memory", "used_memory_dataset", "used_memory_rss", "used_memory_overhead", "lazyfree_pending_objects":
			v, _ := strconv.ParseInt(kv[1], 10, 64)
			out[kv[0]] = v
		case "redis_version", "valkey_version", "kvrocks_version", "server_name":
			server += kv[0] + "=" + kv[1] + " "
		}
	}
	n, _ := rdb.DBSize(ctx).Result()
	out["dbsize"] = n
	return out, strings.TrimSpace(server)
}

func scanFamilies(ctx context.Context, rdb redis.UniversalClient, prefix string) map[string]*family {
	res := make(map[string]*family)
	var cursor uint64
	for {
		keys, next, err := rdb.Scan(ctx, cursor, prefix+"*", 2000).Result()
		if err != nil {
			log.Fatalf("scan: %s", err)
		}
		if len(keys) > 0 {
			cmds, err := rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
				for _, k := range keys {
					p.MemoryUsage(ctx, k, 0)
				}
				return nil
			})
			if err != nil && err != redis.Nil {
				log.Fatalf("memory usage: %s", err)
			}
			for i, c := range cmds {
				v, _ := c.(*redis.IntCmd).Result()
				name := classify(keys[i], prefix)
				f := res[name]
				if f == nil {
					f = &family{}
					res[name] = f
				}
				f.Keys++
				f.Bytes += v
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	return res
}

func check(st syscall.Errno, what string) {
	if st != 0 {
		log.Fatalf("%s: %s", what, st)
	}
}

func main() {
	metaURL := flag.String("meta", "redis://127.0.0.1:6379/1", "meta url")
	var sh shape
	flag.IntVar(&sh.Users, "users", 10, "top level dirs")
	flag.IntVar(&sh.DirsPerUser, "dirs", 10, "dirs per user")
	flag.IntVar(&sh.FilesPerDir, "files", 100, "files per dir")
	flag.IntVar(&sh.Chunks, "chunks", 1, "chunks per file")
	flag.IntVar(&sh.Slices, "slices", 1, "slices per chunk")
	flag.Float64Var(&sh.XattrRatio, "xattr", 0, "ratio of files with xattr")
	flag.Float64Var(&sh.SymlinkRatio, "symlink", 0, "ratio of symlinks")
	flag.Float64Var(&sh.LinkRatio, "hardlink", 0, "ratio of extra hardlinks")
	flag.BoolVar(&sh.UserQuota, "user-quota", false, "quota on each user dir")
	flag.BoolVar(&sh.DirQuota, "dir-quota", false, "quota on each second level dir")
	flag.IntVar(&sh.Threads, "threads", 32, "threads")
	trash := flag.Int("trash-days", 7, "trash days")
	measure := flag.Bool("families", true, "measure memory by key family")
	reset := flag.Bool("reset", true, "reset meta before generating")
	offset := flag.Int("user-offset", 0, "first user index")
	random := flag.Bool("random", false, "realistic names and sizes from rnd/corpus")
	flag.Parse()

	conf := meta.DefaultConf()
	conf.Retries = 50
	m := meta.NewClient(*metaURL, conf)
	if *reset {
		if err := m.Reset(); err != nil {
			log.Fatalf("reset: %s", err)
		}
	}
	format := &meta.Format{
		Name:        "rnd",
		UUID:        uuid.New().String(),
		Storage:     "file",
		Bucket:      "/tmp/rnd-bucket/",
		BlockSize:   4096,
		Compression: "none",
		TrashDays:   *trash,
		DirStats:    true,
	}
	if *reset {
		if err := m.Init(format, true); err != nil {
			log.Fatalf("init: %s", err)
		}
	} else if _, err := m.Load(true); err != nil {
		log.Fatalf("load: %s", err)
	}
	if err := m.NewSession(true); err != nil {
		log.Fatalf("session: %s", err)
	}

	u, err := url.Parse(*metaURL)
	if err != nil {
		log.Fatalf("parse url: %s", err)
	}
	db := 0
	if p := strings.TrimPrefix(u.Path, "/"); p != "" {
		db, _ = strconv.Atoi(p)
	}
	rdb := redis.NewClient(&redis.Options{Addr: u.Host, DB: db})
	bg := context.Background()

	res := result{Time: time.Now().UTC().Format(time.RFC3339), Meta: *metaURL, Shape: sh}
	redisMeta := u.Scheme == "redis" || u.Scheme == "rediss"
	if redisMeta {
		res.Before, res.Server = memInfo(bg, rdb)
	}

	ctx := meta.Background()
	start := time.Now()
	var files, dirs, symlinks, links int64
	userIno := make([]meta.Ino, sh.Users)
	for i := 0; i < sh.Users; i++ {
		var attr meta.Attr
		name := fmt.Sprintf("u%06d", i+*offset)
		check(m.Mkdir(ctx, meta.RootInode, name, 0755, 0, 0, &userIno[i], &attr), "mkdir user")
		dirs++
		if sh.UserQuota {
			p := "/" + name
			if err := m.HandleQuota(ctx, meta.QuotaSet, p, meta.DirQuotaType, map[string]*meta.Quota{p: {MaxSpace: 1 << 40, MaxInodes: 1 << 30}}, false, false, false); err != nil {
				log.Fatalf("quota %s: %s", p, err)
			}
		}
	}

	type job struct{ user, dir int }
	jobs := make(chan job, 1024)
	var wg sync.WaitGroup
	var quotaMu sync.Mutex
	for t := 0; t < sh.Threads; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var prevFile meta.Ino
			for j := range jobs {
				var dino meta.Ino
				var attr meta.Attr
				dname := corpus.DirName(*random, j.user+*offset, j.dir)
				check(m.Mkdir(ctx, userIno[j.user], dname, 0755, 0, 0, &dino, &attr), "mkdir dir")
				atomic.AddInt64(&dirs, 1)
				if sh.DirQuota {
					p := fmt.Sprintf("/u%06d/%s", j.user+*offset, dname)
					quotaMu.Lock()
					err := m.HandleQuota(ctx, meta.QuotaSet, p, meta.DirQuotaType, map[string]*meta.Quota{p: {MaxSpace: 1 << 40, MaxInodes: 1 << 30}}, false, false, false)
					quotaMu.Unlock()
					if err != nil {
						log.Fatalf("quota %s: %s", p, err)
					}
				}
				for k := 0; k < sh.FilesPerDir; k++ {
					seq := j.user*sh.DirsPerUser*sh.FilesPerDir + j.dir*sh.FilesPerDir + k
					fname := corpus.FileName(*random, seq+*offset*sh.DirsPerUser*sh.FilesPerDir, k)
					if sh.SymlinkRatio > 0 && seq%int(1/sh.SymlinkRatio) == 0 {
						var sino meta.Ino
						check(m.Symlink(ctx, dino, fname, "../target/"+fname, &sino, &attr), "symlink")
						atomic.AddInt64(&symlinks, 1)
						continue
					}
					if sh.LinkRatio > 0 && prevFile != 0 && seq%int(1/sh.LinkRatio) == 0 {
						check(m.Link(ctx, prevFile, dino, fname, &attr), "link")
						atomic.AddInt64(&links, 1)
						continue
					}
					var fino meta.Ino
					check(m.Create(ctx, dino, fname, 0644, 0, 0, &fino, &attr), "create")
					for c := 0; c < sh.Chunks; c++ {
						for s := 0; s < sh.Slices; s++ {
							var sid uint64
							check(m.NewSlice(ctx, &sid), "new slice")
							size := corpus.FileSize(*random, seq+*offset*sh.DirsPerUser*sh.FilesPerDir)
							if sh.Chunks > 1 {
								size = meta.ChunkSize / uint32(sh.Slices)
							}
							check(m.Write(ctx, fino, uint32(c), uint32(s)*size, meta.Slice{Id: sid, Size: size, Len: size}, time.Now()), "write")
						}
					}
					if sh.XattrRatio > 0 && seq%int(1/sh.XattrRatio) == 0 {
						check(m.SetXattr(ctx, fino, "user.rnd", []byte("value-0123456789"), 0), "setxattr")
					}
					check(m.Close(ctx, fino), "close")
					prevFile = fino
					atomic.AddInt64(&files, 1)
				}
			}
		}()
	}
	for i := 0; i < sh.Users; i++ {
		for d := 0; d < sh.DirsPerUser; d++ {
			jobs <- job{i, d}
		}
	}
	close(jobs)
	wg.Wait()
	m.FlushSession()
	res.GenSeconds = time.Since(start).Seconds()
	res.Files, res.Dirs, res.Symlinks, res.Hardlinks = files, dirs, symlinks, links
	res.Inodes = files + dirs + symlinks
	time.Sleep(2 * time.Second)
	if err := m.CloseSession(); err != nil {
		log.Printf("close session: %s", err)
	}
	if redisMeta {
		res.After, _ = memInfo(bg, rdb)
	}
	if *measure && redisMeta {
		res.Families = scanFamilies(bg, rdb, "")
	}
	delta := float64(res.After["used_memory"] - res.Before["used_memory"])
	if res.Files > 0 {
		res.PerFile = delta / float64(res.Files)
	}
	if res.Inodes > 0 {
		res.PerInode = delta / float64(res.Inodes)
	}
	names := make([]string, 0, len(res.Families))
	for k := range res.Families {
		names = append(names, k)
	}
	sort.Strings(names)
	enc := json.NewEncoder(os.Stdout)
	_ = enc.Encode(res)
	for _, k := range names {
		f := res.Families[k]
		fmt.Fprintf(os.Stderr, "%-22s keys=%-10d bytes=%-12d per_file=%.1f\n", k, f.Keys, f.Bytes, float64(f.Bytes)/float64(max(res.Files, 1)))
	}
	fmt.Fprintf(os.Stderr, "files=%d dirs=%d inodes=%d gen=%.1fs used_memory_delta=%.0f per_file=%.1f per_inode=%.1f\n", res.Files, res.Dirs, res.Inodes, res.GenSeconds, delta, res.PerFile, res.PerInode)
}
