#!/bin/bash
S=${S:-/tmp/rnd}
R=$S/results/growth
mkdir -p $R
M=redis://127.0.0.1:6380/5
J=$S/juicefs-capsule
mem() { redis-cli -p 6380 info memory | grep -E "^used_memory:" | cut -d: -f2 | tr -d '\r'; }
redis-cli -p 6380 -n 1 flushdb sync >/dev/null; redis-cli -p 6380 -n 4 flushdb sync >/dev/null; redis-cli -p 6380 -n 2 flushdb sync >/dev/null
for r in $(seq 0 9); do
  off=$((r*100))
  if [ $r -eq 0 ]; then reset=true; else reset=false; fi
  $S/nsgen2 -meta $M -reset=$reset -user-offset $off -users 100 -dirs 100 -files 100 -user-quota -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed"; tail -3 $R/gen-$r.err; exit 1; }
  first=$off; [ $r -eq 0 ] && first=10
  t0=$(date +%s.%N)
  for i in $(seq $first $((off+99))); do
    $J metadata evict $M /$(printf "u%06d" $i) --log-level error >> $R/evict-$r.log 2>&1 || { echo "evict $i failed"; tail -3 $R/evict-$r.log; exit 1; }
  done
  t1=$(date +%s.%N)
  sleep 2
  cold=$(( (r+1)*100 - 10 ))
  echo "round=$r cold_units=$cold cold_files=$((cold*10000)) used_memory=$(mem) keys=$(redis-cli -p 6380 -n 5 dbsize) evict_s=$(echo "$t1 - $t0" | bc) probe=$($S/probe -meta $M -label r$r 2>/dev/null)"
done
du -sh /tmp/rnd-bucket/rnd/meta/capsules | tail -1
echo "== done $(date -u +%T)"
