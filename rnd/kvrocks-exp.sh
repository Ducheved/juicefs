#!/bin/bash
S=${S:-/tmp/rnd}
R=$S/results/kvrocks
mkdir -p $R
M=redis://127.0.0.1:6666/3
rss() { ps -o rss= -p $(pgrep -x kvrocks | head -1) | tr -d ' '; }
redis-cli -p 6666 -n 3 flushdb >/dev/null
for r in 0 1 2 3; do
  off=$((r*100)); reset=false; [ $r -eq 0 ] && reset=true
  t0=$(date +%s)
  $S/nsgen2 -meta $M -reset=$reset -user-offset $off -users 100 -dirs 100 -files 100 -user-quota -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed"; tail -3 $R/gen-$r.err; exit 1; }
  t1=$(date +%s)
  redis-cli -p 6666 compact >/dev/null 2>&1
  sleep 5
  echo "round=$r files=$(( (r+1)*1000000 )) gen_s=$((t1-t0)) kvrocks_rss_kb=$(rss) disk=$(du -sh $S/stand/kvrocks | cut -f1) probe=$($S/probe -meta $M -label k$r 2>/dev/null)"
done
echo "== done $(date -u +%T)"
