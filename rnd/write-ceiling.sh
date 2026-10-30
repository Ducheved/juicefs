#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
LABEL=${LABEL:-spread}
ENGINES=${ENGINES:-"valkey kvrocks pg"}
USERS=${USERS:-8}
DIRS=${DIRS:-64}
FILES=${FILES:-20}
PROCS=${PROCS:-4}
WRITERS=${WRITERS:-"4 16 64 256"}
SECONDS_PER_STEP=${SECONDS_PER_STEP:-30}
R=$S/results/write-ceiling-$LABEL
LCG=/sys/fs/cgroup/wc-load
mkdir -p $R
wait_redis() {
  for i in $(seq 1 120); do redis-cli -p 6390 ping 2>/dev/null | grep -q PONG && break; sleep 1; done
  redis-cli -p 6390 ping | grep -q PONG || { echo "engine did not start"; docker logs --tail 5 wc-engine; exit 1; }
  M="redis://127.0.0.1:6390/1?pool_size=512"
}
start_valkey() {
  docker run -d --name wc-engine --network host --memory 4g --memory-swap 4g --cpuset-cpus $ENGINE_CPUS \
    valkey/valkey:9.1.1@sha256:3acc0687f2a2e1091fae6450d7842dd658c941338cf0a873ddd9e14b9e4ea4dd \
    valkey-server --bind 127.0.0.1 --port 6390 --dir /data --appendonly yes --appendfsync always --save "" \
    --maxmemory 2gb --maxmemory-policy noeviction >/dev/null
  wait_redis
}
start_kvrocks() {
  docker run -d --name wc-engine --network host --memory 512m --memory-swap 512m --cpuset-cpus $ENGINE_CPUS apache/kvrocks:2.17.0 \
    --bind 127.0.0.1 --port 6390 --dir /tmp/kvrocks --redis-databases 16 --rocksdb.write_options.sync yes \
    --rocksdb.block_cache_size 256 --workers 8 >/dev/null
  wait_redis
}
start_pg() {
  rm -rf /srv/stand/stand/wc-pg && mkdir -p /srv/stand/stand/wc-pg && chown 26 /srv/stand/stand/wc-pg && chmod 700 /srv/stand/stand/wc-pg
  docker run -d --name wc-engine --network host --memory 512m --memory-swap 512m --cpuset-cpus $ENGINE_CPUS --shm-size 256m --user 26 \
    -v /srv/stand/stand/wc-pg:/pgdata --entrypoint bash \
    ghcr.io/cloudnative-pg/postgresql:18.4-standard-trixie@sha256:ebf3919504d7523a63e8e2fee9b051211de714644b36469275558c624afd50ec -c \
    "initdb -U postgres --auth=trust -D /pgdata >/dev/null && exec postgres -D /pgdata -c listen_addresses=127.0.0.1 -c port=5433 -c shared_buffers=128MB -c max_connections=600" >/dev/null
  for i in $(seq 1 60); do docker exec wc-engine psql -U postgres -p 5433 -At -c "select 1" >/dev/null 2>&1 && break; sleep 1; done
  docker exec wc-engine psql -U postgres -p 5433 -At -c "create database jfs" >/dev/null
  M="postgres://postgres@127.0.0.1:5433/jfs?sslmode=disable"
}
start_metad() {
  rm -rf /srv/stand/stand/wc-metad
  mkdir -p /srv/stand/stand/wc-metad
  ENGINE_CG=/sys/fs/cgroup/wc-metad
  metad_start /srv/stand/stand/wc-metad 6390 $ENGINE_CG 2G
  M="metad://127.0.0.1:6390/jfs?conns=64"
}
echo "label=$LABEL users=$USERS dirs=$DIRS files=$FILES procs=$PROCS engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS"
for engine in $ENGINES; do
  docker rm -f -v wc-engine >/dev/null 2>&1 || true
  ENGINE_CG=
  start_$engine
  CG=${ENGINE_CG:-$(container_cg wc-engine)}
  load $S/bin/nsgen3 -meta "$M" -random -users $USERS -dirs $DIRS -files $FILES -trash-days 0 -families=false > $R/$engine-gen.json 2> $R/$engine-gen.err
  for w in $WRITERS; do
    procs=$(( w < PROCS ? w : PROCS ))
    cg_make $LCG max
    c0=$(cg_cpu_us $CG)
    for p in $(seq 1 $procs); do
      cg_exec $LCG taskset -c $LOAD_CPUS $S/bin/mixload -meta "$M" -random -users $USERS -dirs $DIRS -files $FILES -readers 0 -writers $((w / procs)) -seconds $SECONDS_PER_STEP -label $engine-w$w-p$p > $R/$engine-w$w-p$p.json 2> $R/$engine-w$w-p$p.err &
    done
    wait
    c1=$(cg_cpu_us $CG)
    lc=$(cg_cpu_us $LCG)
    rmdir $LCG
    python3 - $R $engine $w $procs $SECONDS_PER_STEP $c0 $c1 $lc <<'EOF'
import json, sys
r, engine, w, procs, secs, c0, c1, lc = sys.argv[1:]
procs, secs = int(procs), float(secs)
res = [json.load(open(f"{r}/{engine}-w{w}-p{p}.json"))["create"] for p in range(1, procs + 1)]
ops = sum(x["ops"] for x in res)
print(f"engine={engine} writers={w} procs={procs} cycles_per_s={ops / secs:.0f} p50_us_max={max(x['p50_us'] for x in res):.0f} "
      f"p99_us_max={max(x['p99_us'] for x in res):.0f} engine_cores={(int(c1) - int(c0)) / 1e6 / secs:.2f} "
      f"loader_cores={int(lc) / 1e6 / secs:.2f} engine_cpu_us_per_cycle={(int(c1) - int(c0)) / max(ops, 1):.0f}")
EOF
  done
  if [ -n "$ENGINE_CG" ]; then metad_stop $ENGINE_CG; else docker rm -f -v wc-engine >/dev/null; fi
done
echo "== done $(date -u +%T)"
