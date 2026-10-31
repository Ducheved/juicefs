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
  METAD_JOB=$!
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
  wait $METAD_JOB || true
  rmdir $1
}
metad3_start() {
  local data=$1 port=$2 cg=$3 mem=$4 peers= i
  cg_make $cg $mem
  for i in 1 2 3; do peers="$peers${peers:+,}$i=127.0.0.1:$((port + i - 1))"; done
  METAD_PIDS=
  if [ -n "${NODE_MEM:-}" ]; then
    echo +memory > $cg/cgroup.subtree_control
    for i in 1 2 3; do mkdir $cg/n$i; echo $NODE_MEM > $cg/n$i/memory.max; echo 0 > $cg/n$i/memory.swap.max; done
  fi
  for i in 1 2 3; do
    mkdir -p $data-$i
    local node=$cg; [ -n "${NODE_MEM:-}" ] && node=$cg/n$i
    bash -c "echo \$\$ > $node/cgroup.procs; GOMAXPROCS=2 GOMEMLIMIT=${METAD_GOMEMLIMIT:-600MiB} exec taskset -c $ENGINE_CPUS $S/bin/juicefs-metad metad --id $i --listen 127.0.0.1:$((port + i - 1)) --peers $peers --cache-size ${METAD_CACHE_MB:-256} $data-$i" > $data-$i.log 2>&1 &
    METAD_PIDS="$METAD_PIDS $!"
  done
  METAD_URL=127.0.0.1:$port,127.0.0.1:$((port + 1)),127.0.0.1:$((port + 2))
}
metad3_stop() {
  kill $METAD_PIDS
  wait $METAD_PIDS || true
  [ -d $1/n1 ] && rmdir $1/n1 $1/n2 $1/n3
  rmdir $1
}
