#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/kvrocks-growth
DATA=$S/stand/kvrocks-growth
MEM=${MEM:-192m}
CACHE=${CACHE:-64}
ROUNDS=${ROUNDS:-8}
IMAGE=${IMAGE:-apache/kvrocks:2.17.0}
M=redis://127.0.0.1:6668/1
mkdir -p $R $DATA && chmod 777 $DATA
docker rm -f kvrocks-growth >/dev/null 2>&1 || true
docker run -d --name kvrocks-growth --network host --memory $MEM --memory-swap $MEM --cpuset-cpus $ENGINE_CPUS -v $DATA:/data $IMAGE \
  --bind 127.0.0.1 --port 6668 --dir /data --redis-databases 16 --rocksdb.write_options.sync yes \
  --rocksdb.block_cache_size $CACHE --rocksdb.write_buffer_size 8 --rocksdb.max_write_buffer_number 2 --workers 4 >/dev/null
for i in $(seq 1 120); do redis-cli -p 6668 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
redis-cli -p 6668 ping | grep -q PONG || { echo "kvrocks did not start"; docker logs --tail 5 kvrocks-growth; exit 1; }
CG=$(container_cg kvrocks-growth)
pid=$(docker inspect -f '{{.State.Pid}}' kvrocks-growth)
echo "image=$IMAGE mem=$MEM cache_mb=$CACHE engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS"
sst() { find $DATA -name '*.sst' -printf '%s\n' | awk '{s+=$1} END{print s+0}'; }
wal() { find $DATA -name '*.log' -path '*db*' -printf '%s\n' | awk '{s+=$1} END{print s+0}'; }
settle() { for i in $(seq 1 360); do p=$(redis-cli -p 6668 info rocksdb | awk -F: '/^(num_running_compactions|num_running_flushes|compaction_pending)/{s+=$2} END{print s+0}'); [ "$p" = "0" ] && return 0; sleep 5; done; echo "compactions did not settle"; return 1; }
probe() {
  local users=$1 label=$2 c0 c1
  c0=$(cg_cpu_us $CG)
  load $S/bin/probe3 -meta $M -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
  c1=$(cg_cpu_us $CG)
  echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1 - c0) / 6000 ))"
}
for r in $(seq 0 $((ROUNDS - 1))); do
  free_ok || break
  reset=false; [ $r -eq 0 ] && reset=true
  t0=$(date +%s); c0=$(cg_cpu_us $CG)
  load $S/bin/nsgen3 -meta $M -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err
  t1=$(date +%s); c1=$(cg_cpu_us $CG)
  settle
  files=$(( (r + 1) * 1000000 ))
  s=$(sst); w=$(wal)
  echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000 )) sst_bytes=$s sst_b_per_file=$((s / files)) wal_bytes=$w cg_now_mb=$(cg_mb $CG memory.current) cg_peak_mb=$(cg_mb $CG memory.peak) rss_mb=$(( $(ps -o rss= -p $pid) / 1024 )) oom_kills=$(cg_oom $CG)"
  probe 1 "r$r-hot10k"
  probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(cg_oom $CG)"
