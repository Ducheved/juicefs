#!/bin/bash
set -uo pipefail
. $(dirname $0)/cg2.sh
DATA=${DATA:-/srv/stand/stand/metad-growth}
CG=/sys/fs/cgroup/metad-creategap
PAIRS=${PAIRS:-2}
M="metad://127.0.0.1:9523/jfs?conns=64"
metad_start $DATA 9523 $CG ${MEM:-2G}
echo "metad creategap data=$DATA pairs=$PAIRS pid=$METAD_PID"
for p in $(seq 1 $PAIRS); do
  for u in true false; do
    for gap in none lookup getattr sleep; do
      load $S/bin/creategap -meta "$M" -unlink=$u -gap $gap 2>/dev/null
    done
  done
done
metad_stop $CG
