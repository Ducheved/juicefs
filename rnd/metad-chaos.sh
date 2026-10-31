#!/bin/bash
set -uo pipefail
. $(dirname $0)/cg2.sh
LABEL=${LABEL:-chaos}
R=$S/results/metad-$LABEL
CG=/sys/fs/cgroup/metad-chaos
DURATION=${DURATION:-300}
PERIOD=${PERIOD:-10}
WORKERS=${WORKERS:-16}
KEYS=${KEYS:-256}
PORTS=(9531 9532 9533)
ADDRS=127.0.0.1:9531,127.0.0.1:9532,127.0.0.1:9533
PEERS=1=127.0.0.1:9531,2=127.0.0.1:9532,3=127.0.0.1:9533
URL="$ADDRS/chk?conns=8"
PIDS=(0 0 0)
mkdir -p $R
cg_make $CG ${MEM:-2G}
log() { echo "$(date +%T.%3N) $*" | tee -a $R/chaos.log; }
start() {
  local i=$1
  bash -c "echo \$\$ > $CG/cgroup.procs; GOMAXPROCS=1 exec taskset -c $ENGINE_CPUS $S/bin/juicefs-metad metad --id $((i + 1)) --listen 127.0.0.1:${PORTS[$i]} --peers $PEERS --cache-size 256 --memtable-size 32 /srv/stand/stand/chaos-$((i + 1))" >> $R/node-$((i + 1)).log 2>&1 &
  PIDS[$i]=$!
  log "start node $((i + 1)) pid ${PIDS[$i]}"
}
kill9() {
  local i=$1
  kill -9 ${PIDS[$i]} 2>/dev/null
  wait ${PIDS[$i]} 2>/dev/null
  log "kill -9 node $((i + 1)) pid ${PIDS[$i]}"
  PIDS[$i]=0
}
leader() { $S/bin/metadcheck -meta "$ADDRS" -find-leader; }
for i in 0 1 2; do rm -rf /srv/stand/stand/chaos-$((i + 1)) /srv/stand/stand/chaos-$((i + 1)).install-*; done
rm -rf /srv/stand/stand/chaos-data
: > $R/chaos.log
for i in 0 1 2; do start $i; done
sleep 3
load $S/bin/juicefs-metad format --storage file --bucket /srv/stand/stand/chaos-data/ "metad://$URL" chk > $R/format.log 2>&1 || {
  log "format failed"; tail -3 $R/format.log
  for i in 0 1 2; do kill ${PIDS[$i]}; wait ${PIDS[$i]} 2>/dev/null; done
  rmdir $CG; exit 1
}
load $S/bin/metadcheck -meta "$URL" -workers $WORKERS -keys $KEYS -duration ${DURATION}s -out $R > $R/check.json 2> $R/check.err &
CHECK=$!
log "checker pid $CHECK duration ${DURATION}s workers $WORKERS keys $KEYS"
end=$(( $(date +%s) + DURATION - PERIOD ))
round=0
while [ $(date +%s) -lt $end ]; do
  sleep $PERIOD
  round=$((round + 1))
  l=$(leader)
  case $((round % 4)) in
    1|3) v=$l; [ "$v" -lt 0 ] && v=$((RANDOM % 3)); log "round $round: leader is node $((l + 1)), killing it"; kill9 $v; sleep $((2 + RANDOM % 3)); start $v ;;
    2) v=$(( (l + 1 + RANDOM % 2) % 3 )); log "round $round: leader is node $((l + 1)), killing follower node $((v + 1))"; kill9 $v; sleep $((2 + RANDOM % 3)); start $v ;;
    0) a=$(( (l + 1) % 3 )); b=$(( (l + 2) % 3 )); log "round $round: leader is node $((l + 1)), killing both followers"; kill9 $a; kill9 $b; sleep 5; start $a; start $b ;;
  esac
done
wait $CHECK
rc=$?
log "checker exit $rc"
cat $R/check.json
for i in 0 1 2; do [ ${PIDS[$i]} -ne 0 ] && kill ${PIDS[$i]} && wait ${PIDS[$i]} 2>/dev/null; done
log "cgroup peak $(cg_mb $CG memory.peak) MB, oom_kills $(cg_oom $CG)"
rmdir $CG
du -sh /srv/stand/stand/chaos-* | tee -a $R/chaos.log
