#!/bin/bash
S=/tmp/claude-0/-home-user/40fd7f72-6e5a-5b1e-910b-f8925f83d035/scratchpad
R=$S/results/pg
mkdir -p $R
M="postgres://postgres:pg@127.0.0.1:5432/jfs?sslmode=disable"
pss() { t=0; for p in $(pgrep -x postgres); do v=$(awk '/^Pss:/{print $2}' /proc/$p/smaps_rollup 2>/dev/null); t=$((t+${v:-0})); done; echo $t; }
dbsize() { su postgres -c "psql -At -d jfs -c \"select pg_size_pretty(pg_database_size('jfs'))\""; }
for r in 0 1 2 3; do
  off=$((r*100)); reset=false; [ $r -eq 0 ] && reset=true
  t0=$(date +%s)
  $S/nsgen2 -meta "$M" -reset=$reset -user-offset $off -users 100 -dirs 100 -files 100 -user-quota -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed"; tail -3 $R/gen-$r.err; exit 1; }
  t1=$(date +%s)
  sleep 5
  echo "round=$r files=$(( (r+1)*1000000 )) gen_s=$((t1-t0)) pg_pss_kb=$(pss) db=$(dbsize) probe=$($S/probe -meta "$M" -label p$r 2>/dev/null)"
done
echo "== done $(date -u +%T)"
