#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
LABEL=${LABEL:-ab}
R=$S/results/metad-$LABEL
DATA=${DATA:-/srv/stand/stand/metad-growth}
CG=/sys/fs/cgroup/metad-ab
MEM=${MEM:-2G}
PAIRS=${PAIRS:-3}
CLIENTS=${CLIENTS:-"probe3-p1 probe3"}
M="metad://127.0.0.1:9521/jfs?conns=64"
mkdir -p $R
metad_start $DATA 9521 $CG $MEM
echo "metad ab data=$DATA mem=$MEM clients=$CLIENTS pairs=$PAIRS pid=$METAD_PID"
for p in $(seq 1 $PAIRS); do
  for c in $CLIENTS; do
    for users in 1 400; do
      label=$c-u$users-$p
      c0=$(cg_cpu_us $CG)
      load $S/bin/$c -meta "$M" -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
      c1=$(cg_cpu_us $CG)
      echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1 - c0) / 6000 ))"
    done
  done
done
echo "== done $(date +%T) oom_kills=$(cg_oom $CG)"
metad_stop $CG
