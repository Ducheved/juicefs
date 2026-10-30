package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/urfave/cli/v2"
)

func cmdMetadata() *cli.Command {
	return &cli.Command{
		Name:     "metadata",
		Category: "ADMIN",
		Usage:    "Move metadata of cold directories out of the metadata engine (experimental)",
		Subcommands: []*cli.Command{
			{
				Name:      "evict",
				Usage:     "Move metadata of a directory tree into a capsule in object storage",
				ArgsUsage: "META-URL PATH",
				Action:    metadataEvict,
				Flags: []cli.Flag{
					&cli.Int64Flag{
						Name:  "max-inodes",
						Value: 1000000,
						Usage: "refuse trees with more inodes",
					},
				},
			},
			{
				Name:      "prefetch",
				Usage:     "Load metadata of an evicted directory back into the metadata engine",
				ArgsUsage: "META-URL PATH",
				Action:    metadataPrefetch,
				Flags: []cli.Flag{
					&cli.BoolFlag{
						Name:  "force",
						Usage: "take over an unfinished eviction or hydration of a live session",
					},
				},
			},
			{
				Name:      "status",
				Usage:     "List evicted directories",
				ArgsUsage: "META-URL",
				Action:    metadataStatus,
			},
		},
	}
}

func openMetadata(ctx *cli.Context, args int) (meta.Meta, object.ObjectStorage) {
	setup(ctx, args)
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
	m, blob := openMetadata(ctx, 2)
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
	m, blob := openMetadata(ctx, 2)
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
	m, _ := openMetadata(ctx, 1)
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
