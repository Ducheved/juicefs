#!/bin/bash
S=/tmp/claude-0/-home-user/40fd7f72-6e5a-5b1e-910b-f8925f83d035/scratchpad
R=$S/results/kvrocks-cap
mkdir -p $R
M=redis://127.0.0.1:6666/3
MEM=${MEM:-256m}
CACHE=${CACHE:-96}
NAME=kvrocks-cap
docker rm -f $NAME >/dev/null 2>&1
sync; echo 3 > /proc/sys/vm/drop_caches 2>/dev/null
docker run -d --name $NAME --network host --memory $MEM --memory-swap $MEM -v $S/stand/kvrocks:/data apache/kvrocks:latest \
  --bind 127.0.0.1 --port 6666 --dir /data --redis-databases 16 --rocksdb.write_options.sync yes \
  --rocksdb.block_cache_size $CACHE --rocksdb.write_buffer_size 8 --rocksdb.max_write_buffer_number 2 --workers 4 >/dev/null
for i in $(seq 1 120); do redis-cli -p 6666 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
CG=/sys/fs/cgroup/memory/docker/$(docker inspect -f '{{.Id}}' $NAME)
pid=$(docker inspect -f '{{.State.Pid}}' $NAME)
mem() { echo "cg_now_mb=$(( $(cat $CG/memory.usage_in_bytes) / 1048576 )) cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes) / 1048576 )) rss_mb=$(( $(ps -o rss= -p $pid) / 1024 )) read_mb=$(( $(awk '/^read_bytes/{print $2}' /proc/$pid/io) / 1048576 ))"; }
echo "start mem=$MEM cache=${CACHE}MB $(mem) keys=$(redis-cli -p 6666 -n 3 dbsize)"
for u in 1 10 100 400; do
  $S/probe -meta $M -users $u -n 5000 -label "ws$u" > $R/probe-$MEM-$u.json 2> $R/probe-$MEM-$u.err
  echo "users=$u ws_files=$((u*10000)) $(cat $R/probe-$MEM-$u.json) $(mem)"
done
echo "== done $(date -u +%T) oom=$(cat $CG/memory.failcnt)"
