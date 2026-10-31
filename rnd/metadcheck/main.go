package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/metad/proto"
)

type input struct {
	Key   int
	Write bool
	Value string
}

type event struct {
	Op     porcupine.Operation
	Failed bool
}

var registerModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[int][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(input).Key
			byKey[k] = append(byKey[k], op)
		}
		var parts [][]porcupine.Operation
		for _, ops := range byKey {
			parts = append(parts, ops)
		}
		return parts
	},
	Init: func() interface{} { return "init" },
	Step: func(state, in, out interface{}) (bool, interface{}) {
		i := in.(input)
		if i.Write {
			return true, i.Value
		}
		return out.(string) == state.(string), state
	},
	Equal: func(a, b interface{}) bool { return a.(string) == b.(string) },
	DescribeOperation: func(in, out interface{}) string {
		i := in.(input)
		if i.Write {
			return fmt.Sprintf("put(r%d, %s)", i.Key, i.Value)
		}
		return fmt.Sprintf("get(r%d) -> %v", i.Key, out)
	},
}

func findLeader(addrs []string) {
	for i, a := range addrs {
		c, err := net.DialTimeout("tcp", a, 500*time.Millisecond)
		if err != nil {
			continue
		}
		_ = c.SetDeadline(time.Now().Add(time.Second))
		_, err = c.Write(proto.NewEncoder(proto.OpPing).Frame())
		var status byte
		if err == nil {
			status, _, err = proto.ReadFrame(bufio.NewReader(c))
		}
		_ = c.Close()
		if err == nil && status == proto.StatusOK {
			fmt.Println(i)
			return
		}
	}
	fmt.Println(-1)
}

func main() {
	url := flag.String("meta", "", "metad url")
	workers := flag.Int("workers", 16, "concurrent clients")
	keys := flag.Int("keys", 8, "registers")
	clients := flag.Int("clients", 1, "independent meta clients, each with its own leader view")
	duration := flag.Duration("duration", time.Minute, "workload duration")
	out := flag.String("out", "", "directory for the history visualisation on failure")
	leader := flag.Bool("find-leader", false, "print the index of the ready leader among the addresses")
	flag.Parse()
	if *leader {
		hosts, _, _ := strings.Cut(*url, "/")
		findLeader(strings.Split(hosts, ","))
		return
	}

	ms := make([]meta.Meta, *clients)
	for i := range ms {
		ms[i] = meta.NewClient("metad://"+*url, meta.DefaultConf())
		if _, err := ms[i].Load(true); err != nil {
			log.Fatalf("load: %s", err)
		}
		if err := ms[i].NewSession(true); err != nil {
			log.Fatalf("session: %s", err)
		}
	}
	m := ms[0]
	ctx := meta.Background()
	var attr meta.Attr
	var dir, setDir meta.Ino
	if st := m.Mkdir(ctx, meta.RootInode, "chk", 0755, 0, 0, &dir, &attr); st != 0 {
		log.Fatalf("mkdir chk: %s", st)
	}
	if st := m.Mkdir(ctx, dir, "set", 0755, 0, 0, &setDir, &attr); st != 0 {
		log.Fatalf("mkdir set: %s", st)
	}
	regs := make([]meta.Ino, *keys)
	for k := range regs {
		if st := m.Create(ctx, dir, fmt.Sprintf("r%d", k), 0644, 0, 0, &regs[k], &attr); st != 0 {
			log.Fatalf("create r%d: %s", k, st)
		}
		if st := m.SetXattr(ctx, regs[k], "user.v", []byte("init"), 0); st != 0 {
			log.Fatalf("init r%d: %s", k, st)
		}
	}

	start := time.Now()
	var mu sync.Mutex
	var events []event
	var okTimes []int64
	var lat []time.Duration
	created := map[string]bool{}
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			m := ms[w%len(ms)]
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; time.Since(start) < *duration; i++ {
				k := rng.Intn(*keys)
				call := time.Since(start).Nanoseconds()
				t0 := time.Now()
				switch p := rng.Intn(10); {
				case p < 5:
					v := fmt.Sprintf("w%d-%d", w, i)
					st := m.SetXattr(ctx, regs[k], "user.v", []byte(v), 0)
					ret := time.Since(start).Nanoseconds()
					mu.Lock()
					if st == 0 {
						events = append(events, event{Op: porcupine.Operation{ClientId: w, Input: input{k, true, v}, Call: call, Output: "", Return: ret}})
						okTimes = append(okTimes, ret)
						lat = append(lat, time.Since(t0))
					} else {
						events = append(events, event{Op: porcupine.Operation{ClientId: w, Input: input{k, true, v}, Call: call, Output: "", Return: math.MaxInt64}, Failed: true})
					}
					mu.Unlock()
				case p < 9:
					var buf []byte
					st := m.GetXattr(ctx, regs[k], "user.v", &buf)
					ret := time.Since(start).Nanoseconds()
					mu.Lock()
					if st == 0 {
						events = append(events, event{Op: porcupine.Operation{ClientId: w, Input: input{k, false, ""}, Call: call, Output: string(buf), Return: ret}})
						okTimes = append(okTimes, ret)
						lat = append(lat, time.Since(t0))
					}
					mu.Unlock()
				default:
					name := fmt.Sprintf("c%d-%d", w, i)
					var ino meta.Ino
					var a meta.Attr
					st := m.Create(ctx, setDir, name, 0644, 0, 0, &ino, &a)
					if st == 0 {
						m.Close(ctx, ino)
					}
					ret := time.Since(start).Nanoseconds()
					mu.Lock()
					created[name] = st == 0
					if st == 0 {
						okTimes = append(okTimes, ret)
						lat = append(lat, time.Since(t0))
					}
					mu.Unlock()
				}
			}
		}(w)
	}
	wg.Wait()

	for k := range regs {
		var buf []byte
		call := time.Since(start).Nanoseconds()
		if st := m.GetXattr(ctx, regs[k], "user.v", &buf); st != 0 {
			log.Fatalf("final read r%d: %s", k, st)
		}
		events = append(events, event{Op: porcupine.Operation{ClientId: *workers, Input: input{k, false, ""}, Call: call, Output: string(buf), Return: time.Since(start).Nanoseconds()}})
	}
	var entries []*meta.Entry
	if st := m.Readdir(ctx, setDir, 0, &entries); st != 0 {
		log.Fatalf("readdir set: %s", st)
	}
	present := map[string]bool{}
	for _, e := range entries {
		present[string(e.Name)] = true
	}
	var ackedCreates, unknownCreates, lostCreates, unknownPresent int
	for name, acked := range created {
		switch {
		case acked && !present[name]:
			lostCreates++
		case acked:
			ackedCreates++
		default:
			unknownCreates++
			if present[name] {
				unknownPresent++
			}
		}
	}

	ops := make([]porcupine.Operation, len(events))
	var writes, failedWrites, reads int
	for i, e := range events {
		ops[i] = e.Op
		switch {
		case e.Failed:
			failedWrites++
		case e.Op.Input.(input).Write:
			writes++
		default:
			reads++
		}
	}
	sort.Slice(okTimes, func(i, j int) bool { return okTimes[i] < okTimes[j] })
	var gaps []float64
	for i := 1; i < len(okTimes); i++ {
		if g := float64(okTimes[i]-okTimes[i-1]) / 1e9; g > 0.2 {
			gaps = append(gaps, math.Round(float64(okTimes[i-1])/1e7)/100, math.Round(g*1000)/1000)
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	pct := func(p float64) int64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*p)].Microseconds()
	}
	res := map[string]any{
		"writes_acked":     writes,
		"writes_failed":    failedWrites,
		"reads":            reads,
		"creates_acked":    ackedCreates,
		"creates_lost":     lostCreates,
		"creates_failed":   unknownCreates,
		"failed_but_exist": unknownPresent,
		"op_p50_us":        pct(0.5),
		"op_p99_us":        pct(0.99),
		"op_max_us":        pct(1),
		"gaps_at_s_len_s":  gaps,
		"duration_s":       duration.Seconds(),
	}
	_ = json.NewEncoder(os.Stdout).Encode(res)
	result, info := porcupine.CheckOperationsVerbose(registerModel, ops, 10*time.Minute)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"linearizable": string(result), "ops_checked": len(ops)})
	if result != porcupine.Ok && *out != "" {
		if f, err := os.Create(*out + "/linearizability.html"); err == nil {
			_ = porcupine.Visualize(registerModel, info, f)
			_ = f.Close()
		}
	}
	if result != porcupine.Ok || lostCreates > 0 {
		os.Exit(1)
	}
}
