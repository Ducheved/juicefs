#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
MEM=${MEM:-2G}
CACHE_MB=${CACHE_MB:-768}
USERS=${USERS:-3900}
PHASES=${PHASES:-load,settle,read,write}
LABEL=${LABEL:-$PHASES}
R=$S/results/pebble-bench
CG=/sys/fs/cgroup/pebble-bench
mkdir -p $R
case $PHASES in load*) rm -rf /srv/stand/stand/pebble-bench ;; esac
cg_make $CG $MEM
echo "mem=$MEM cache_mb=$CACHE_MB users=$USERS files=$((USERS * 10000)) cpus=$ENGINE_CPUS phases=$PHASES"
GOMAXPROCS=2 GOMEMLIMIT=1GiB cg_exec $CG taskset -c $ENGINE_CPUS $S/bin/pebblebench -dir /srv/stand/stand/pebble-bench -users $USERS -cache-mb $CACHE_MB -threads 2 -phases $PHASES > $R/$LABEL.json 2> $R/$LABEL.err || echo "rc=$?"
cat $R/$LABEL.json
echo "peak_mb=$(cg_mb $CG memory.peak) oom_kills=$(cg_oom $CG) cpu_s=$(( $(cg_cpu_us $CG) / 1000000 )) dir_bytes=$(du -sb /srv/stand/stand/pebble-bench | awk '{print $1}')"
rmdir $CG
