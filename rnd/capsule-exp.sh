#!/bin/bash
S=${S:-/tmp/rnd}
R=$S/results/capsule
mkdir -p $R
M=redis://127.0.0.1:6380/4
J=$S/juicefs-capsule
mem() { redis-cli -p 6380 info memory | grep -E "^used_memory:" | cut -d: -f2 | tr -d '\r'; }
keys() { redis-cli -p 6380 -n 4 dbsize; }
echo "== gen $(date -u +%T)"
$S/nsgen -meta $M -users 100 -dirs 100 -files 100 -user-quota -families=false > $R/gen.json 2> $R/gen.err; tail -1 $R/gen.err
echo "before used_memory=$(mem) keys=$(keys)"
for u in 3 4; do $J dump $M --subdir /u00000$u $R/dump-before-$u.json --log-level error >/dev/null 2>&1; done
start=$(date +%s.%N)
for i in $(seq 0 89); do
  u=$(printf "u%06d" $i)
  $J metadata evict $M /$u --log-level error >> $R/evict.log 2>&1 || { echo "evict $u failed"; tail -3 $R/evict.log; exit 1; }
done
end=$(date +%s.%N)
echo "evicted 90 units in $(echo "$end - $start" | bc)s"
echo "cold used_memory=$(mem) keys=$(keys)"
redis-cli -p 6380 -n 4 memory usage capsule2 ; redis-cli -p 6380 -n 4 memory usage capsules
du -sh /tmp/rnd-bucket/meta/capsules | tail -1
for u in 3 4 5 6 7; do
  peak=0
  ( while true; do v=$(mem); echo $v; sleep 0.05; done ) > $R/peak-$u.txt &
  P=$!
  $J metadata prefetch $M /u00000$u --log-level error >> $R/hydrate.log 2>&1 || echo "prefetch $u failed"
  kill $P
  echo "hydrated u00000$u peak_used_memory=$(sort -n $R/peak-$u.txt | tail -1) used_memory=$(mem)"
done
for u in 3 4; do
  $J dump $M --subdir /u00000$u $R/dump-after-$u.json --log-level error >/dev/null 2>&1
  python3 - $R/dump-before-$u.json $R/dump-after-$u.json <<'PY'
import json,sys
a=json.load(open(sys.argv[1])); b=json.load(open(sys.argv[2]))
for d in (a,b):
    d.pop("Setting",None); d.pop("Counters",None)
print("dump identical" if a==b else "dump DIFFERS", sys.argv[1].split('/')[-1])
PY
done
$J metadata status $M --log-level error | wc -l
echo "== done $(date -u +%T)"
