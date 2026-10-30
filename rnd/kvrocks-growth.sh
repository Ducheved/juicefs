#!/bin/bash
set -euo pipefail
S=${S:-/tmp/rnd}
R=$S/results/kvrocks-growth
DATA=$S/stand/kvrocks-growth
MEM=${MEM:-192m}
CACHE=${CACHE:-64}
ROUNDS=${ROUNDS:-8}
MINFREE_KB=${MINFREE_KB:-1500000}
M=redis://127.0.0.1:6668/1
mkdir -p $R $DATA && chmod 777 $DATA
docker rm -f kvrocks-growth >/dev/null 2>&1 || true
docker run -d --name kvrocks-growth --network host --memory $MEM --memory-swap $MEM -v $DATA:/data apache/kvrocks:latest \
  --bind 127.0.0.1 --port 6668 --dir /data --redis-databases 16 --rocksdb.write_options.sync yes \
  --rocksdb.block_cache_size $CACHE --rocksdb.write_buffer_size 8 --rocksdb.max_write_buffer_number 2 --workers 4 >/dev/null
for i in $(seq 1 120); do redis-cli -p 6668 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
redis-cli -p 6668 ping | grep -q PONG || { echo "kvrocks did not start"; docker logs --tail 5 kvrocks-growth; exit 1; }
CG=/sys/fs/cgroup/memory/docker/$(docker inspect -f '{{.Id}}' kvrocks-growth)
pid=$(docker inspect -f '{{.State.Pid}}' kvrocks-growth)
cpu() { awk '{print $14+$15}' /proc/$pid/stat; }
sst() { find $DATA -name '*.sst' -printf '%s\n' | awk '{s+=$1} END{print s+0}'; }
wal() { find $DATA -name '*.log' -path '*db*' -printf '%s\n' | awk '{s+=$1} END{print s+0}'; }
settle() { for i in $(seq 1 360); do p=$(redis-cli -p 6668 info rocksdb | awk -F: '/^(num_running_compactions|num_running_flushes|compaction_pending)/{s+=$2} END{print s+0}'); [ "$p" = "0" ] && return 0; sleep 5; done; echo "compactions did not settle"; return 1; }
probe() {
  local users=$1 label=$2 c0 c1
  c0=$(cpu)
  $S/probe3 -meta $M -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
  c1=$(cpu)
  echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1-c0)*10000/6000 ))"
}
for r in $(seq 0 $((ROUNDS-1))); do
  free=$(df --output=avail -k $S | tail -1)
  if [ "$free" -lt "$MINFREE_KB" ]; then echo "stop: free ${free} KB"; break; fi
  reset=false; [ $r -eq 0 ] && reset=true
  t0=$(date +%s); c0=$(cpu)
  $S/nsgen3 -meta $M -random -reset=$reset -user-offset $((r*100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err
  t1=$(date +%s); c1=$(cpu)
  settle
  files=$(( (r+1)*1000000 ))
  s=$(sst); w=$(wal)
  echo "round=$r files=$files gen_s=$((t1-t0)) gen_server_cpu_s=$(( (c1-c0)/100 )) sst_bytes=$s sst_b_per_file=$((s/files)) wal_bytes=$w cg_now_mb=$(( $(cat $CG/memory.usage_in_bytes)/1048576 )) cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes)/1048576 )) rss_mb=$(( $(ps -o rss= -p $pid)/1024 )) oom=$(cat $CG/memory.failcnt)"
  probe 1 "r$r-hot10k"
  probe $(( (r+1)*100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom=$(cat $CG/memory.failcnt)"
