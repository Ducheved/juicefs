#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/write-ceiling
WRITERS=${WRITERS:-"1 4 16 64 256"}
SECONDS_PER_STEP=${SECONDS_PER_STEP:-30}
mkdir -p $R
start_valkey() {
  docker rm -f -v wc-engine >/dev/null 2>&1 || true
  docker run -d --name wc-engine --network host --memory 4g --memory-swap 4g --cpuset-cpus $ENGINE_CPUS \
    valkey/valkey:9.1.1@sha256:3acc0687f2a2e1091fae6450d7842dd658c941338cf0a873ddd9e14b9e4ea4dd \
    valkey-server --bind 127.0.0.1 --port 6390 --dir /data --appendonly yes --appendfsync always --save "" \
    --maxmemory 2gb --maxmemory-policy noeviction >/dev/null
}
start_kvrocks() {
  docker rm -f -v wc-engine >/dev/null 2>&1 || true
  docker run -d --name wc-engine --network host --memory 512m --memory-swap 512m --cpuset-cpus $ENGINE_CPUS apache/kvrocks:2.17.0 \
    --bind 127.0.0.1 --port 6390 --dir /tmp/kvrocks --redis-databases 16 --rocksdb.write_options.sync yes \
    --rocksdb.block_cache_size 256 --workers 8 >/dev/null
}
for engine in valkey kvrocks; do
  start_$engine
  for i in $(seq 1 120); do redis-cli -p 6390 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
  redis-cli -p 6390 ping | grep -q PONG || { echo "$engine did not start"; docker logs --tail 5 wc-engine; exit 1; }
  CG=$(container_cg wc-engine)
  M="redis://127.0.0.1:6390/1?pool_size=512"
  load $S/bin/nsgen3 -meta "$M" -random -users 2 -dirs 4 -files 500 -trash-days 0 -families=false > $R/$engine-gen.json 2> $R/$engine-gen.err
  echo "engine=$engine engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS"
  for w in $WRITERS; do
    c0=$(cg_cpu_us $CG)
    load $S/bin/mixload -meta "$M" -random -users 2 -dirs 4 -files 500 -readers 0 -writers $w -seconds $SECONDS_PER_STEP -label $engine-w$w > $R/$engine-w$w.json 2> $R/$engine-w$w.err
    c1=$(cg_cpu_us $CG)
    cycles=$(python3 -c "import json,sys; print(json.load(open('$R/$engine-w$w.json'))['create']['ops'])")
    echo "writers=$w $(cat $R/$engine-w$w.json) engine_cores=$(python3 -c "print(round(($c1-$c0)/1e6/$SECONDS_PER_STEP,2))") engine_cpu_us_per_cycle=$(( (c1 - c0) / (cycles > 0 ? cycles : 1) ))"
  done
  docker rm -f -v wc-engine >/dev/null
done
echo "== done $(date -u +%T)"
