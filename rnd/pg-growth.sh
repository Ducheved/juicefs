#!/bin/bash
set -euo pipefail
S=${S:-/tmp/rnd}
R=$S/results/pg-growth
MEM=${MEM:-256M}
BUFFERS=${BUFFERS:-64MB}
ROUNDS=${ROUNDS:-4}
MINFREE_KB=${MINFREE_KB:-1500000}
M="postgres://postgres:pg@127.0.0.1:5432/jfs?sslmode=disable"
CG=/sys/fs/cgroup/memory/pg-growth
CA=/sys/fs/cgroup/cpuacct/pg-growth
mkdir -p $R
psql_() { su postgres -c "psql -At -d ${2:-postgres} -c \"$1\""; }
psql_ "ALTER SYSTEM SET shared_buffers = '$BUFFERS'" >/dev/null
psql_ "ALTER SYSTEM SET max_connections = 200" >/dev/null
service postgresql restart >/dev/null
for i in $(seq 1 30); do psql_ "select 1" >/dev/null 2>&1 && break; sleep 1; done
echo "shared_buffers=$(psql_ 'show shared_buffers') fsync=$(psql_ 'show fsync') synchronous_commit=$(psql_ 'show synchronous_commit') version=$(psql_ 'show server_version')"
mkdir -p $CG $CA
echo $MEM > $CG/memory.limit_in_bytes
echo 0 > $CG/memory.max_usage_in_bytes
for p in $(pgrep -x postgres); do echo $p > $CG/cgroup.procs; echo $p > $CA/cgroup.procs; done
echo 0 > $CA/cpuacct.usage
cpu() { cat $CA/cpuacct.usage; }
datadir=$(psql_ "show data_directory")
dbsize() { psql_ "select pg_database_size('jfs')"; }
walsize() { du -sb $datadir/pg_wal | awk '{print $1}'; }
oomkills() { awk '/^oom_kill /{print $2}' $CG/memory.oom_control; }
probe() {
	local users=$1 label=$2 c0 c1
	c0=$(cpu)
	$S/probe3 -meta "$M" -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err
	c1=$(cpu)
	echo "$label $(cat $R/$label.json) server_cpu_us_per_op=$(( (c1 - c0) / 1000 / 6000 ))"
}
for r in $(seq 0 $((ROUNDS - 1))); do
	free=$(df --output=avail -k $datadir | tail -1)
	if [ "$free" -lt "$MINFREE_KB" ]; then echo "stop: free ${free} KB"; break; fi
	reset=false
	[ $r -eq 0 ] && reset=true
	t0=$(date +%s)
	c0=$(cpu)
	$S/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err)"; echo "oom_kills=$(oomkills)"; exit 1; }
	t1=$(date +%s)
	c1=$(cpu)
	psql_ "CHECKPOINT" >/dev/null
	files=$(( (r + 1) * 1000000 ))
	d=$(dbsize)
	echo "round=$r files=$files gen_s=$((t1 - t0)) gen_server_cpu_s=$(( (c1 - c0) / 1000000000 )) db_bytes=$d db_b_per_file=$((d / files)) wal_bytes=$(walsize) cg_now_mb=$(( $(cat $CG/memory.usage_in_bytes) / 1048576 )) cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes) / 1048576 )) oom_kills=$(oomkills)"
	probe 1 "r$r-hot10k"
	probe $(( (r + 1) * 100 )) "r$r-all"
done
echo "== done $(date -u +%T) oom_kills=$(oomkills)"
