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
metad_start() {
  local data=$1 port=$2 cg=$3 mem=$4
  cg_make $cg $mem
  GOMAXPROCS=2 GOMEMLIMIT=1GiB cg_exec $cg taskset -c $ENGINE_CPUS $S/bin/juicefs-metad metad --listen 127.0.0.1:$port --cache-size ${METAD_CACHE_MB:-768} $data > $data.log 2>&1 &
  for i in $(seq 1 60); do
    if (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null; then
      METAD_PID=$(head -1 $cg/cgroup.procs)
      return 0
    fi
    sleep 1
  done
  echo "metad did not start"; tail -5 $data.log; return 1
}
metad_stop() {
  kill $METAD_PID
  for i in $(seq 1 60); do [ -z "$(cat $1/cgroup.procs)" ] && break; sleep 1; done
  rmdir $1
}
