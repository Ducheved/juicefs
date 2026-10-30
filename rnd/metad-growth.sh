#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/metad-growth
DATA=/srv/stand/stand/metad-growth
CG=/sys/fs/cgroup/metad-growth
MEM=${MEM:-2G}
ROUNDS=${ROUNDS:-4}
M="metad://127.0.0.1:9520/jfs?conns=64"
mkdir -p $R
rm -rf /srv/stand/stand/metad-growth
mkdir -p $DATA
metad_start $DATA 9520 $CG $MEM
echo "metad mem=$MEM cache_mb=${METAD_CACHE_MB:-768} engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS pid=$METAD_PID"
probe() {
  local users=$1 label=$2 c0 c1
  c0=$(cg_cpu_us $CG)
  load $S/bin/probe3 -meta "$M" -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
  c1=$(cg_cpu_us $CG)
  echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1 - c0) / 6000 ))"
}
for r in $(seq 0 $((ROUNDS - 1))); do
  free_ok || break
  reset=false; [ $r -eq 0 ] && reset=true
  t0=$(date +%s); c0=$(cg_cpu_us $CG)
  load $S/bin/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err)"; metad_stop $CG; exit 1; }
  t1=$(date +%s); c1=$(cg_cpu_us $CG)
  files=$(( (r + 1) * 1000000 ))
  d=$(du -sb $DATA | awk '{print $1}')
  echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000 )) dir_bytes=$d b_per_file=$((d / files)) cg_now_mb=$(cg_mb $CG memory.current) cg_peak_mb=$(cg_mb $CG memory.peak) rss_mb=$(( $(ps -o rss= -p $METAD_PID) / 1024 )) oom_kills=$(cg_oom $CG)"
  probe 1 "r$r-hot10k"
  probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(cg_oom $CG)"
metad_stop $CG
