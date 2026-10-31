#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/metad-${LABEL:-growth}
DATA=/srv/stand/stand/metad-growth
CG=/sys/fs/cgroup/metad-growth
MEM=${MEM:-2G}
ROUNDS=${ROUNDS:-4}
REPLICAS=${REPLICAS:-1}
mkdir -p $R
rm -rf /srv/stand/stand/metad-growth /srv/stand/stand/metad-growth-1 /srv/stand/stand/metad-growth-2 /srv/stand/stand/metad-growth-3
if [ $REPLICAS -eq 3 ]; then
  metad3_start $DATA 9520 $CG $MEM
  M="metad://$METAD_URL/jfs?conns=64"
  DIRS_GLOB="$DATA-1 $DATA-2 $DATA-3"
  stop() { metad3_stop $CG; }
  rss() { ps -o rss= -p $(echo $METAD_PIDS | tr " " ,) | awk '{s += $1} END {print int(s / 1024)}'; }
else
  mkdir -p $DATA
  metad_start $DATA 9520 $CG $MEM
  M="metad://127.0.0.1:9520/jfs?conns=64"
  DIRS_GLOB=$DATA
  stop() { metad_stop $CG; }
  rss() { echo $(( $(ps -o rss= -p $METAD_PID) / 1024 )); }
fi
echo "metad replicas=$REPLICAS mem=$MEM cache_mb=${METAD_CACHE_MB:-default} engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS"
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
  load $S/bin/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err)"; stop; exit 1; }
  t1=$(date +%s); c1=$(cg_cpu_us $CG)
  files=$(( (r + 1) * 1000000 ))
  d=$(du -scb $DIRS_GLOB | tail -1 | awk '{print $1}')
  echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000 )) dir_bytes=$d b_per_file=$((d / files)) cg_now_mb=$(cg_mb $CG memory.current) cg_peak_mb=$(cg_mb $CG memory.peak) rss_mb=$(rss) anon_mb=$(( $(awk '/^anon /{print $2}' $CG/memory.stat) / 1048576 )) file_mb=$(( $(awk '/^file /{print $2}' $CG/memory.stat) / 1048576 )) oom_kills=$(cg_oom $CG)"
  probe 1 "r$r-hot10k"
  probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(cg_oom $CG)"
stop
