#!/bin/bash
set -euo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/pg-growth
DATA=$S/stand/pg-growth
MEM=${MEM:-192m}
BUFFERS=${BUFFERS:-64MB}
ROUNDS=${ROUNDS:-4}
IMAGE=${IMAGE:-ghcr.io/cloudnative-pg/postgresql:18.4-standard-trixie@sha256:ebf3919504d7523a63e8e2fee9b051211de714644b36469275558c624afd50ec}
M="postgres://postgres@127.0.0.1:5432/jfs?sslmode=disable"
mkdir -p $R $DATA && chown 26 $DATA && chmod 700 $DATA
docker rm -f pg-growth >/dev/null 2>&1 || true
docker run -d --name pg-growth --network host --memory $MEM --memory-swap $MEM --cpuset-cpus $ENGINE_CPUS --shm-size 256m --user 26 \
  -v $DATA:/pgdata --entrypoint bash $IMAGE -c \
  "test -f /pgdata/PG_VERSION || initdb -U postgres --auth=trust -D /pgdata >/dev/null; exec postgres -D /pgdata -c listen_addresses=127.0.0.1 -c port=5432 -c shared_buffers=$BUFFERS -c max_connections=200" >/dev/null
psql_() { docker exec pg-growth psql -U postgres -At -d ${2:-postgres} -c "$1"; }
for i in $(seq 1 60); do psql_ "select 1" >/dev/null 2>&1 && break; sleep 1; done
psql_ "select 1" >/dev/null || { echo "postgres did not start"; docker logs --tail 5 pg-growth; exit 1; }
psql_ "drop database if exists jfs" >/dev/null
psql_ "create database jfs" >/dev/null
CG=$(container_cg pg-growth)
echo "image=$IMAGE mem=$MEM engine_cpus=$ENGINE_CPUS load_cpus=$LOAD_CPUS shared_buffers=$(psql_ 'show shared_buffers') fsync=$(psql_ 'show fsync') synchronous_commit=$(psql_ 'show synchronous_commit') io_method=$(psql_ 'show io_method') version=$(psql_ 'show server_version')"
dbsize() { psql_ "select pg_database_size('jfs')"; }
walsize() { du -sb $DATA/pg_wal | awk '{print $1}'; }
probe() {
	local users=$1 label=$2 c0 c1
	c0=$(cg_cpu_us $CG)
	load $S/bin/probe3 -meta "$M" -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
	c1=$(cg_cpu_us $CG)
	echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1 - c0) / 6000 ))"
}
for r in $(seq 0 $((ROUNDS - 1))); do
	free_ok || break
	reset=false
	[ $r -eq 0 ] && reset=true
	t0=$(date +%s)
	c0=$(cg_cpu_us $CG)
	load $S/bin/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err) oom_kills=$(cg_oom $CG)"; exit 1; }
	t1=$(date +%s)
	c1=$(cg_cpu_us $CG)
	psql_ "CHECKPOINT" >/dev/null
	files=$(( (r + 1) * 1000000 ))
	d=$(dbsize)
	echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000 )) db_bytes=$d db_b_per_file=$((d / files)) wal_bytes=$(walsize) cg_now_mb=$(cg_mb $CG memory.current) cg_peak_mb=$(cg_mb $CG memory.peak) oom_kills=$(cg_oom $CG)"
	probe 1 "r$r-hot10k"
	probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(cg_oom $CG)"
