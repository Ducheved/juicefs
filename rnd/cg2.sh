S=${S:-/srv/stand}
ENGINE_CPUS=${ENGINE_CPUS:-0-1}
LOAD_CPUS=${LOAD_CPUS:-2-3}
MINFREE_KB=${MINFREE_KB:-1500000}
container_cg() { echo /sys/fs/cgroup/system.slice/docker-$(docker inspect -f '{{.Id}}' $1).scope; }
cg_mb() { echo $(( $(cat $1/$2) / 1048576 )); }
cg_oom() { awk '/^oom_kill /{print $2}' $1/memory.events; }
cg_cpu_us() { awk '/^usage_usec /{print $2}' $1/cpu.stat; }
cg_make() { rmdir $1 2>/dev/null || true; mkdir $1; echo $2 > $1/memory.max; echo 0 > $1/memory.swap.max; }
cg_exec() { local cg=$1; shift; bash -c "echo \$\$ > $cg/cgroup.procs; exec \"\$@\"" cg_exec "$@"; }
load() { taskset -c $LOAD_CPUS "$@"; }
free_ok() { local free; free=$(df --output=avail -k $S | tail -1); [ "$free" -ge "$MINFREE_KB" ] || { echo "stop: free ${free} KB"; return 1; }; }
