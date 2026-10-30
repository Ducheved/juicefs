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
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/metad/server"
	"github.com/urfave/cli/v2"
)

func cmdMetad() *cli.Command {
	return &cli.Command{
		Name:      "metad",
		Action:    metad,
		Category:  "SERVICE",
		Usage:     "Run a metadata server for metad:// volumes",
		ArgsUsage: "DIR",
		Description: `
Examples:
$ juicefs metad --listen 127.0.0.1:9520 /var/lib/metad
$ juicefs format metad://127.0.0.1:9520/myjfs myjfs`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "listen",
				Value: "127.0.0.1:9520",
				Usage: "address to listen on",
			},
			&cli.Int64Flag{
				Name:  "cache-size",
				Value: 768,
				Usage: "block cache size in MiB",
			},
			&cli.Uint64Flag{
				Name:  "memtable-size",
				Value: 64,
				Usage: "memtable size in MiB",
			},
			&cli.DurationFlag{
				Name:  "snapshot-ttl",
				Value: 5 * time.Second,
				Usage: "how long an idle read snapshot is kept for a transaction",
			},
			&cli.IntFlag{
				Name:  "max-batch",
				Value: 4096,
				Usage: "maximum transactions committed with one sync",
			},
		},
	}
}

func metad(c *cli.Context) error {
	setup(c, 1)
	s, err := server.Open(server.Options{
		Dir:         c.Args().Get(0),
		CacheMB:     c.Int64("cache-size"),
		MemtableMB:  c.Uint64("memtable-size"),
		SnapshotTTL: c.Duration("snapshot-ttl"),
		MaxBatch:    c.Int("max-batch"),
	})
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", c.String("listen"))
	if err != nil {
		_ = s.Close()
		return err
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-signals
		logger.Infof("metad: shutting down")
		_ = s.Close()
	}()
	logger.Infof("metad: serving %s on %s", c.Args().Get(0), ln.Addr())
	return s.Serve(ln)
}
