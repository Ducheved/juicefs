#!/bin/bash
set -uo pipefail
S=${S:-/tmp/rnd}
R=$S/results/badger-growth
DATA=$S/stand/badger-growth
MEM=${MEM:-192M}
ROUNDS=${ROUNDS:-4}
MINFREE_KB=${MINFREE_KB:-1500000}
M="badger://$DATA?sync=true"
CG=/sys/fs/cgroup/memory/badger-growth
CA=/sys/fs/cgroup/cpuacct/badger-growth
mkdir -p $R $DATA $CG $CA
echo $MEM > $CG/memory.limit_in_bytes
echo 0 > $CG/memory.max_usage_in_bytes
echo 0 > $CA/cpuacct.usage
oomkills() { awk '/^oom_kill /{print $2}' $CG/memory.oom_control; }
capped() { bash -c "echo \$\$ > $CG/cgroup.procs; echo \$\$ > $CA/cgroup.procs; exec \"\$@\"" capped "$@"; }
cpu() { cat $CA/cpuacct.usage; }
probe() {
	local users=$1 label=$2 c0 c1
	c0=$(cpu)
	capped $S/probe3 -meta "$M" -random -users $users -dirs 100 -files 100 -n 3000 -label $label > $R/$label.json 2> $R/$label.err || { echo "$label failed: $(tail -1 $R/$label.err) oom_kills=$(oomkills)"; return 1; }
	c1=$(cpu)
	echo "$label $(cat $R/$label.json) process_cpu_us_per_op=$(( (c1 - c0) / 1000 / 6000 ))"
}
for r in $(seq 0 $((ROUNDS - 1))); do
	free=$(df --output=avail -k $DATA | tail -1)
	if [ "$free" -lt "$MINFREE_KB" ]; then echo "stop: free ${free} KB"; break; fi
	reset=false
	[ $r -eq 0 ] && reset=true
	echo 0 > $CG/memory.max_usage_in_bytes
	t0=$(date +%s)
	c0=$(cpu)
	capped $S/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false > $R/gen-$r.json 2> $R/gen-$r.err || { echo "gen $r failed: $(tail -1 $R/gen-$r.err) oom_kills=$(oomkills) cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes) / 1048576 ))"; exit 1; }
	t1=$(date +%s)
	c1=$(cpu)
	files=$(( (r + 1) * 1000000 ))
	d=$(du -sb $DATA | awk '{print $1}')
	echo "round=$r files=$files gen_s=$((t1 - t0)) gen_process_cpu_s=$(( (c1 - c0) / 1000000000 )) dir_bytes=$d b_per_file=$((d / files)) sst_bytes=$(find $DATA -name '*.sst' -printf '%s\n' | awk '{s+=$1} END{print s+0}') vlog_bytes=$(find $DATA -name '*.vlog' -printf '%s\n' | awk '{s+=$1} END{print s+0}') gen_cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes) / 1048576 )) oom_kills=$(oomkills)"
	echo 0 > $CG/memory.max_usage_in_bytes
	probe 1 "r$r-hot10k" || exit 1
	probe $(( (r + 1) * 100 )) "r$r-all" || exit 1
	echo "r$r probe_cg_max_mb=$(( $(cat $CG/memory.max_usage_in_bytes) / 1048576 ))"
done
echo "== done $(date -u +%T) oom_kills=$(oomkills)"
