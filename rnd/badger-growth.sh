#!/bin/bash
set -uo pipefail
. $(dirname $0)/cg2.sh
R=$S/results/badger-growth
DATA=$S/stand/badger-growth
MEM=${MEM:-192M}
ROUNDS=${ROUNDS:-4}
CPUS=${CPUS:-$ENGINE_CPUS,$LOAD_CPUS}
M="badger://$DATA?sync=true"
mkdir -p $R $DATA
echo "mem=$MEM cpus=$CPUS"
capped() {
	local label=$1 cg=/sys/fs/cgroup/badger-$1 rc c0
	shift
	cg_make $cg $MEM
	taskset -c $CPUS bash -c "echo \$\$ > $cg/cgroup.procs; exec \"\$@\"" capped "$@" > $R/$label.json 2> $R/$label.err
	rc=$?
	echo "$label rc=$rc peak_mb=$(cg_mb $cg memory.peak) oom_kills=$(cg_oom $cg) process_cpu_us=$(cg_cpu_us $cg)"
	rmdir $cg
	return $rc
}
for r in $(seq 0 $((ROUNDS - 1))); do
	free_ok || break
	reset=false
	[ $r -eq 0 ] && reset=true
	t0=$(date +%s)
	capped gen-$r $S/bin/nsgen3 -meta "$M" -random -reset=$reset -user-offset $((r * 100)) -users 100 -dirs 100 -files 100 -trash-days 0 -families=false || { echo "gen $r failed: $(tail -1 $R/gen-$r.err)"; exit 1; }
	t1=$(date +%s)
	files=$(( (r + 1) * 1000000 ))
	d=$(du -sb $DATA | awk '{print $1}')
	echo "round=$r files=$files gen_s=$((t1 - t0)) dir_bytes=$d b_per_file=$((d / files)) sst_bytes=$(find $DATA -name '*.sst' -printf '%s\n' | awk '{s+=$1} END{print s+0}') vlog_bytes=$(find $DATA -name '*.vlog' -printf '%s\n' | awk '{s+=$1} END{print s+0}')"
	capped r$r-hot10k $S/bin/probe3 -meta "$M" -random -users 1 -dirs 100 -files 100 -n 3000 -label r$r-hot10k || exit 1
	echo "r$r-hot10k $(cat $R/r$r-hot10k.json)"
	capped r$r-all $S/bin/probe3 -meta "$M" -random -users $(( (r + 1) * 100 )) -dirs 100 -files 100 -n 3000 -label r$r-all || exit 1
	echo "r$r-all $(cat $R/r$r-all.json)"
done
echo "== done $(date -u +%T)"
