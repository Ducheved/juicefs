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

package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

func cmdMetadata() *cli.Command {
	return &cli.Command{
		Name:     "metadata",
		Category: "ADMIN",
		Usage:    "Move metadata of cold directories out of the metadata engine (experimental)",
		Flags: []cli.Flag{
			&cli.Int64Flag{
				Name:  "max-inodes",
				Value: 1000000,
				Usage: "refuse or skip trees with more inodes",
			},
			&cli.BoolFlag{
				Name:  "force",
				Usage: "take over an unfinished eviction or hydration of a live session",
			},
			&cli.IntFlag{
				Name:  "depth",
				Value: 1,
				Usage: "units are directories this many levels below PATH",
			},
			&cli.BoolFlag{
				Name:  "clear",
				Usage: "remove the policy and stop tracking access",
			},
			&cli.DurationFlag{
				Name:  "idle",
				Value: 7 * 24 * time.Hour,
				Usage: "evict units not used for this long (0 to evict only under memory pressure)",
			},
			&cli.DurationFlag{
				Name:  "min-resident",
				Value: time.Hour,
				Usage: "keep units loaded or used within this time",
			},
			&cli.DurationFlag{
				Name:  "half-life",
				Value: 24 * time.Hour,
				Usage: "half-life of access counts when choosing units under memory pressure",
			},
			&cli.StringFlag{
				Name:  "memory-limit",
				Usage: "memory of the metadata engine to keep usage under, e.g. 16G (disabled by default)",
			},
			&cli.Float64Flag{
				Name:  "high",
				Value: 0.9,
				Usage: "start evicting used units above this share of memory-limit",
			},
			&cli.Float64Flag{
				Name:  "low",
				Value: 0.8,
				Usage: "stop evicting used units below this share of memory-limit",
			},
			&cli.IntFlag{
				Name:  "max-units",
				Value: 10,
				Usage: "evict at most this many units per round",
			},
			&cli.DurationFlag{
				Name:  "interval",
				Value: 5 * time.Minute,
				Usage: "time between rounds",
			},
			&cli.BoolFlag{
				Name:  "once",
				Usage: "run one round and exit",
			},
		},
		Subcommands: []*cli.Command{
			{
				Name:      "evict",
				Usage:     "Move metadata of a directory tree into a capsule in object storage",
				ArgsUsage: "META-URL PATH",
				Action:    metadataEvict,
			},
			{
				Name:      "prefetch",
				Usage:     "Load metadata of an evicted directory back into the metadata engine",
				ArgsUsage: "META-URL PATH",
				Action:    metadataPrefetch,
			},
			{
				Name:      "status",
				Usage:     "List evicted directories",
				ArgsUsage: "META-URL",
				Action:    metadataStatus,
			},
			{
				Name:      "policy",
				Usage:     "Show or set which directories are units of automatic eviction",
				ArgsUsage: "META-URL [PATH]",
				Action:    metadataPolicy,
			},
			{
				Name:      "auto-evict",
				Usage:     "Evict units that are idle or least used under memory pressure",
				ArgsUsage: "META-URL",
				Action:    metadataAutoEvict,
			},
		},
	}
}

func openMetadata(ctx *cli.Context, min, max int) (meta.Meta, object.ObjectStorage) {
	setup0(ctx, min, max)
	removePassword(ctx.Args().Get(0))
	conf := meta.DefaultConf()
	conf.NoBGJob = true
	m := meta.NewClient(ctx.Args().Get(0), conf)
	format, err := m.Load(true)
	if err != nil {
		logger.Fatalf("load setting: %s", err)
	}
	blob, err := createStorage(*format)
	if err != nil {
		logger.Fatalf("object storage: %s", err)
	}
	return m, blob
}

func resolveDir(m meta.Meta, path string) meta.Ino {
	ino := meta.RootInode
	var attr meta.Attr
	for _, name := range strings.Split(path, "/") {
		if name == "" {
			continue
		}
		if st := m.Lookup(meta.Background(), ino, name, &ino, &attr, false); st != 0 {
			logger.Fatalf("resolve %s: %s", path, st)
		}
	}
	return ino
}

func printCapsule(info *meta.CapsuleInfo, took time.Duration) {
	out, _ := json.Marshal(info)
	fmt.Printf("%s took=%s\n", out, took)
}

func metadataEvict(ctx *cli.Context) error {
	m, blob := openMetadata(ctx, 2, 2)
	if err := m.NewSession(true); err != nil {
		logger.Fatalf("create session: %s", err)
	}
	defer m.CloseSession() //nolint:errcheck
	root := resolveDir(m, ctx.Args().Get(1))
	start := time.Now()
	info, err := m.EvictMetadata(meta.Background(), root, blob, ctx.Int64("max-inodes"))
	if err != nil {
		return err
	}
	printCapsule(info, time.Since(start))
	return nil
}

func metadataPrefetch(ctx *cli.Context) error {
	m, blob := openMetadata(ctx, 2, 2)
	if err := m.NewSession(true); err != nil {
		logger.Fatalf("create session: %s", err)
	}
	defer m.CloseSession() //nolint:errcheck
	root := resolveDir(m, ctx.Args().Get(1))
	start := time.Now()
	info, err := m.HydrateMetadata(meta.Background(), root, blob, ctx.Bool("force"))
	if err != nil {
		return err
	}
	printCapsule(info, time.Since(start))
	return nil
}

func metadataStatus(ctx *cli.Context) error {
	m, _ := openMetadata(ctx, 1, 1)
	infos, err := m.ListCapsules(meta.Background())
	if err != nil {
		return err
	}
	var b strings.Builder
	for _, info := range infos {
		out, _ := json.Marshal(info)
		b.Write(out)
		b.WriteByte('\n')
	}
	_, err = os.Stdout.WriteString(b.String())
	return err
}

func metadataPolicy(ctx *cli.Context) error {
	m, _ := openMetadata(ctx, 1, 2)
	if ctx.Bool("clear") {
		return m.SetCapsulePolicy(meta.Background(), nil)
	}
	if ctx.NArg() > 1 {
		if d := ctx.Int("depth"); d < 0 || d > 100 {
			logger.Fatalf("invalid depth %d", d)
		}
		p := &meta.CapsulePolicy{Root: resolveDir(m, ctx.Args().Get(1)), Depth: ctx.Int("depth")}
		if err := m.SetCapsulePolicy(meta.Background(), p); err != nil {
			return err
		}
	}
	p, err := m.GetCapsulePolicy(meta.Background())
	if err != nil {
		return err
	}
	out, _ := json.Marshal(p)
	fmt.Println(string(out))
	return nil
}

func metadataAutoEvict(ctx *cli.Context) error {
	m, blob := openMetadata(ctx, 1, 1)
	opt := meta.AutoEvictOptions{
		Idle:        ctx.Duration("idle"),
		MinResident: ctx.Duration("min-resident"),
		HalfLife:    ctx.Duration("half-life"),
		High:        ctx.Float64("high"),
		Low:         ctx.Float64("low"),
		MaxUnits:    ctx.Int("max-units"),
		MaxInodes:   ctx.Int64("max-inodes"),
	}
	if s := ctx.String("memory-limit"); s != "" {
		opt.MemoryLimit = int64(utils.ParseBytesStr("memory-limit", s, 'M'))
	}
	if err := m.NewSession(true); err != nil {
		logger.Fatalf("create session: %s", err)
	}
	defer m.CloseSession() //nolint:errcheck
	for {
		start := time.Now()
		rep, err := m.AutoEvict(meta.Background(), blob, opt)
		if rep != nil {
			out, _ := json.Marshal(rep)
			fmt.Printf("%s took=%s\n", out, time.Since(start))
		}
		if err != nil {
			if ctx.Bool("once") || !errors.Is(err, syscall.EBUSY) {
				return err
			}
			logger.Warnf("auto-evict: %s", err)
		}
		if ctx.Bool("once") {
			return nil
		}
		time.Sleep(ctx.Duration("interval"))
	}
}
