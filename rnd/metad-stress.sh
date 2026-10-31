#!/bin/bash
set -uo pipefail
. $(dirname $0)/cg2.sh
SCENARIO=${SCENARIO:-pause}
LOAD=${LOAD:-check}
LABEL=${LABEL:-stress-$SCENARIO}
R=$S/results/metad-$LABEL
CG=/sys/fs/cgroup/metad-stress
CHK=/sys/fs/cgroup/metad-stress-chk
DURATION=${DURATION:-300}
PERIOD=${PERIOD:-15}
WORKERS=${WORKERS:-32}
CLIENTS=${CLIENTS:-4}
KEYS=${KEYS:-256}
DELAY=${DELAY:-50us}
CACHE=${CACHE:-128}
IMG_MB=${IMG_MB:-1536}
PORT=9530
IP=(10.77.0.11 10.77.0.12 10.77.0.13)
ADDRS=${IP[0]}:$PORT,${IP[1]}:$PORT,${IP[2]}:$PORT
PEERS=1=${IP[0]}:$PORT,2=${IP[1]}:$PORT,3=${IP[2]}:$PORT
URL="$ADDRS/chk?conns=8"
PIDS=(0 0 0)
DATA=(/srv/stand/stand/stress-1 /srv/stand/stand/stress-2 /srv/stand/stand/stress-3)
DISK=$(cat /sys/class/block/$(basename $(readlink -f /sys/class/block/$(basename $(findmnt -no SOURCE -T $S))/..))/dev)
mkdir -p $R
log() { echo "$(date +%T.%3N) $*" | tee -a $R/stress.log; }

net_up() {
  ip link add br-md type bridge
  ip addr add 10.77.0.1/24 dev br-md
  ip link set br-md up
  for i in 1 2 3; do
    ip netns add md$i
    ip link add vmd$i type veth peer name eth0 netns md$i
    ip link set vmd$i master br-md up
    ip -n md$i addr add ${IP[$((i - 1))]}/24 dev eth0
    ip -n md$i link set eth0 up
    ip -n md$i link set lo up
    ip netns exec md$i tc qdisc add dev eth0 root netem delay $DELAY
    tc qdisc add dev vmd$i root netem delay $DELAY
  done
}
net_down() {
  for i in 1 2 3; do ip link del vmd$i 2>/dev/null; ip netns del md$i 2>/dev/null; done
  ip link del br-md 2>/dev/null
}

disks_up() {
  for i in 0 1 2; do
    rm -rf ${DATA[$i]} ${DATA[$i]}.img
    mkdir -p ${DATA[$i]}
    if [ $SCENARIO = diskfull ]; then
      truncate -s ${IMG_MB}M ${DATA[$i]}.img
      mkfs.ext4 -q -m 0 ${DATA[$i]}.img
      mount -o loop ${DATA[$i]}.img ${DATA[$i]}
    fi
  done
}
disks_down() {
  for i in 0 1 2; do
    mountpoint -q ${DATA[$i]} && umount ${DATA[$i]}
    rm -rf ${DATA[$i]} ${DATA[$i]}.img
  done
}

cg_up() {
  rmdir $CG/n1 $CG/n2 $CG/n3 $CG $CHK 2>/dev/null
  mkdir $CG $CHK
  echo "+memory +io +cpu" > $CG/cgroup.subtree_control
  echo ${MEM:-2G} > $CG/memory.max
  echo 0 > $CG/memory.swap.max
  for n in n1 n2 n3; do mkdir $CG/$n; done
}

start() {
  local i=$1
  bash -c "echo \$\$ > $CG/n$((i + 1))/cgroup.procs; exec ip netns exec md$((i + 1)) env GOMAXPROCS=1 taskset -c $ENGINE_CPUS $S/bin/juicefs-metad metad --id $((i + 1)) --listen ${IP[$i]}:$PORT --peers $PEERS --cache-size $CACHE --memtable-size 32 ${DATA[$i]}/db" >> $R/node-$((i + 1)).log 2>&1 &
  PIDS[$i]=$!
  log "start node $((i + 1)) pid ${PIDS[$i]}"
}
alive() { [ ${PIDS[$1]} -ne 0 ] && kill -0 ${PIDS[$1]} 2>/dev/null; }
reap() {
  local i
  for i in 0 1 2; do
    if [ ${PIDS[$i]} -ne 0 ] && ! kill -0 ${PIDS[$i]} 2>/dev/null; then
      wait ${PIDS[$i]} 2>/dev/null
      log "node $((i + 1)) pid ${PIDS[$i]} exited: $(tail -1 $R/node-$((i + 1)).log | cut -c1-200)"
      PIDS[$i]=0
    fi
  done
}
leader() { $S/bin/metadcheck -meta "$ADDRS" -find-leader; }
other() { echo $(( ($1 + 1 + RANDOM % 2) % 3 )); }

isolate() {
  local i=$1 j
  for j in 0 1 2; do
    [ $j -eq $i ] && continue
    ip netns exec md$((i + 1)) iptables -A INPUT -s ${IP[$j]} -j DROP
    ip netns exec md$((i + 1)) iptables -A OUTPUT -d ${IP[$j]} -j DROP
  done
}
heal() { local i; for i in 1 2 3; do ip netns exec md$i iptables -F; done; }

slow() { echo "$DISK wiops=${SLOW_IOPS:-20} wbps=${SLOW_BPS:-1048576}" > $CG/n$(($1 + 1))/io.max; }
unslow() { local i; for i in 1 2 3; do echo "$DISK wiops=max wbps=max" > $CG/n$i/io.max; done; }

fill() { fallocate -l $(( $(df --output=avail -B1 ${DATA[$1]} | tail -1) - 65536 )) ${DATA[$1]}/ballast; }
unfill() { local i; for i in 0 1 2; do rm -f ${DATA[$i]}/ballast; done; }

fault() {
  local kind=$1 l=$2 v
  case $kind in
    pause)
      v=$(( 1500 + RANDOM % 3000 ))
      log "pause leader node $((l + 1)) for ${v} ms"
      kill -STOP ${PIDS[$l]}; sleep $(awk "BEGIN{print $v/1000}"); kill -CONT ${PIDS[$l]} ;;
    partition)
      v=$(( 3 + RANDOM % 6 ))
      log "isolate leader node $((l + 1)) from its peers for ${v} s (clients still reach it)"
      isolate $l; sleep $v; heal ;;
    partition-follower)
      v=$(other $l)
      log "isolate follower node $((v + 1)) for 8 s"
      isolate $v; sleep 8; heal ;;
    slowdisk)
      log "throttle leader node $((l + 1)) disk to ${SLOW_IOPS:-20} write iops for 10 s"
      slow $l; sleep 10; unslow ;;
    slowdisk-follower)
      v=$(other $l)
      log "throttle follower node $((v + 1)) disk for 20 s"
      slow $v; sleep 20; unslow ;;
    diskfull)
      log "fill leader node $((l + 1)) disk"
      fill $l; sleep 6; reap; unfill
      alive $l || start $l ;;
    kill)
      log "kill -9 leader node $((l + 1))"
      kill -9 ${PIDS[$l]}; wait ${PIDS[$l]} 2>/dev/null; PIDS[$l]=0; sleep $(( 1 + RANDOM % 3 )); start $l ;;
  esac
}

trap 'heal 2>/dev/null; unslow 2>/dev/null' EXIT
net_down; net_up; disks_up; cg_up
rm -rf $S/stand/stress-bucket
: > $R/stress.log
log "scenario $SCENARIO duration ${DURATION}s period ${PERIOD}s delay $DELAY disk $DISK"
for i in 0 1 2; do start $i; done
sleep 3
load $S/bin/juicefs-metad format --storage file --bucket $S/stand/stress-bucket/ "metad://$URL" chk > $R/format.log 2>&1 || {
  log "format failed"; tail -3 $R/format.log
  for i in 0 1 2; do kill ${PIDS[$i]}; wait ${PIDS[$i]} 2>/dev/null; done
  exit 1
}
cpu0=$(cg_cpu_us $CG); t0=$(date +%s%N)
if [ $LOAD = fleet ]; then
  bash -c "echo \$\$ > $CHK/cgroup.procs; exec taskset -c $LOAD_CPUS $S/bin/fleet -meta 'metad://$ADDRS/chk?conns=${FLEET_CONNS:-2}' -clients ${FLEET_CLIENTS:-1000} -active ${FLEET_ACTIVE:-0.2} -rate ${FLEET_RATE:-10} -quotas ${FLEET_QUOTAS:-100} -duration ${DURATION}s -label $LABEL" > $R/check.json 2> $R/check.err &
  CHECK=$!
  log "fleet pid $CHECK clients ${FLEET_CLIENTS:-1000} active ${FLEET_ACTIVE:-0.2} rate ${FLEET_RATE:-10} quotas ${FLEET_QUOTAS:-100}"
else
  bash -c "echo \$\$ > $CHK/cgroup.procs; exec taskset -c $LOAD_CPUS $S/bin/metadcheck -meta '$URL' -workers $WORKERS -clients $CLIENTS -keys $KEYS -duration ${DURATION}s -out $R" > $R/check.json 2> $R/check.err &
  CHECK=$!
  log "checker pid $CHECK workers $WORKERS clients $CLIENTS keys $KEYS"
fi
KINDS=(${KINDS:-$SCENARIO})
end=$(( $(date +%s) + DURATION - PERIOD ))
round=0
while [ "${KINDS[0]}" != none ] && [ $(date +%s) -lt $end ] && kill -0 $CHECK 2>/dev/null; do
  sleep $PERIOD
  reap
  for i in 0 1 2; do alive $i || start $i; done
  round=$((round + 1))
  l=$(leader)
  if [ "$l" -lt 0 ]; then log "round $round: no ready leader"; continue; fi
  fault ${KINDS[$((RANDOM % ${#KINDS[@]}))]} $l
done
wait $CHECK
rc=$?
log "checker exit $rc, anon $(( $(awk '/^anon /{print $2}' $CG/memory.stat) / 1048576 )) MB, file $(( $(awk '/^file /{print $2}' $CG/memory.stat) / 1048576 )) MB, engine cores $(awk "BEGIN{printf \"%.2f\", ($(cg_cpu_us $CG) - $cpu0) * 1000 / ($(date +%s%N) - $t0)}")"
cat $R/check.json
for i in 0 1 2; do alive $i && kill ${PIDS[$i]} && wait ${PIDS[$i]} 2>/dev/null; done
log "cgroup peak $(cg_mb $CG memory.peak) MB, oom_kills $(cg_oom $CG)"
for n in n1 n2 n3; do rmdir $CG/$n; done
rmdir $CG $CHK
du -sh ${DATA[@]} | tee -a $R/stress.log
disks_down; net_down
