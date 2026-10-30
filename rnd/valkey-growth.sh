#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/valkey-growth
DATA=$S/stand/valkey-growth
MEM=${MEM:-4g}
MAXMEMORY=${MAXMEMORY:-2gb}
ROUNDS=${ROUNDS:-4}
IMAGE=${IMAGE:-valkey/valkey:9.1.1@sha256:3acc0687f2a2e1091fae6450d7842dd658c941338cf0a873ddd9e14b9e4ea4dd}
M=redis://127.0.0.1:6380/1
mkdir -p $R $DATA && chmod 777 $DATA
docker rm -f valkey-growth >/dev/null 2>&1 || true
docker run -d --name valkey-growth --network host --memory $MEM --memory-swap $MEM --cpuset-cpus $ENGINE_CPUS -v $DATA:/data $IMAGE \
  valkey-server --bind 127.0.0.1 --port 6380 --dir /data --appendonly yes --appendfsync always --save "" \
  --maxmemory $MAXMEMORY --maxmemory-policy noeviction >/dev/null
for i in $(seq 1 60); do redis-cli -p 6380 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
redis-cli -p 6380 ping | grep -q PONG || { echo "valkey did not start"; docker logs --tail 5 valkey-growth; exit 1; }
CG=$(container_cg valkey-growth)
echo "image=$IMAGE mem=$MEM maxmemory=$MAXMEMORY engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS"
info() { redis-cli -p 6380 info $1 | awk -F: -v k=$2 '$1==k{gsub("\r","",$2); print $2}'; }
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
  load $S/bin/nsgen3 -meta $M -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err) used_memory=$(info memory used_memory)"; exit 1; }
  t1=$(date +%s); c1=$(cg_cpu_us $CG)
  files=$(( (r + 1) * 1000000 ))
  u=$(info memory used_memory)
  echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000 )) used_memory=$u b_per_file=$((u / files)) rss=$(info memory used_memory_rss) keys=$(redis-cli -p 6380 -n 1 dbsize) aof_bytes=$(du -sb $DATA | awk '{print $1}') cg_now_mb=$(cg_mb $CG memory.current) cg_peak_mb=$(cg_mb $CG memory.peak) oom_kills=$(cg_oom $CG)"
  probe 1 "r$r-hot10k"
  probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(cg_oom $CG)"
