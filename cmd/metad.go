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
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
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
$ juicefs format metad://127.0.0.1:9520/myjfs myjfs

# three replicas, one per host
$ juicefs metad --id 1 --listen 10.0.0.1:9520 --peers 1=10.0.0.1:9520,2=10.0.0.2:9520,3=10.0.0.3:9520 /var/lib/metad
$ juicefs format metad://10.0.0.1:9520,10.0.0.2:9520,10.0.0.3:9520/myjfs myjfs`,
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
				Usage: "maximum transactions in one log entry",
			},
			&cli.Uint64Flag{
				Name:  "id",
				Value: 1,
				Usage: "raft id of this node",
			},
			&cli.StringFlag{
				Name:  "peers",
				Usage: "all replicas as id=host:port,... (default: this node alone at --listen)",
			},
			&cli.DurationFlag{
				Name:  "tick",
				Value: 100 * time.Millisecond,
				Usage: "raft tick; heartbeat every tick, election after 10 ticks",
			},
			&cli.Uint64Flag{
				Name:  "log-retain",
				Value: 100000,
				Usage: "log entries kept behind the applied index for lagging replicas",
			},
			&cli.DurationFlag{
				Name:  "dedup-window",
				Value: 2 * time.Minute,
				Usage: "how long transaction outcomes are kept for clients that lost a reply",
			},
		},
	}
}

func parsePeers(s string, id uint64, listen string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	if s == "" {
		peers[id] = listen
		return peers, nil
	}
	for _, p := range strings.Split(s, ",") {
		k, addr, ok := strings.Cut(p, "=")
		n, err := strconv.ParseUint(k, 10, 64)
		if !ok || err != nil || n == 0 || addr == "" {
			return nil, fmt.Errorf("invalid peer %q", p)
		}
		if _, dup := peers[n]; dup {
			return nil, fmt.Errorf("duplicate peer id %d", n)
		}
		peers[n] = addr
	}
	return peers, nil
}

func metad(c *cli.Context) error {
	setup(c, 1)
	peers, err := parsePeers(c.String("peers"), c.Uint64("id"), c.String("listen"))
	if err != nil {
		return err
	}
	s, err := server.Open(server.Options{
		Dir:         c.Args().Get(0),
		CacheMB:     c.Int64("cache-size"),
		MemtableMB:  c.Uint64("memtable-size"),
		SnapshotTTL: c.Duration("snapshot-ttl"),
		MaxBatch:    c.Int("max-batch"),
		ID:          c.Uint64("id"),
		Peers:       peers,
		Tick:        c.Duration("tick"),
		LogRetain:   c.Uint64("log-retain"),
		DedupWindow: c.Duration("dedup-window"),
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
