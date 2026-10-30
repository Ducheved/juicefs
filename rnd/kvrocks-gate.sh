#!/bin/bash
set -euo pipefail
S=${S:-/tmp/rnd}
R=$S/results/gate
REDIS=${REDIS:-6379}
KV=6672
FULL=6673
mkdir -p $R $S/stand/gate-kv $S/stand/gate-full
chmod 777 $S/stand/gate-kv
LK=$(sed -n '/^const scriptLookup = `/,/^`/p' pkg/meta/lua_scripts.go | sed '1d;$d')
RS=$(sed -n '/^const scriptResolve = `/,/^`/p' pkg/meta/lua_scripts.go | sed '1d;$d')

kvrocks() {
	docker rm -f $1 >/dev/null 2>&1 || true
	docker run -d --name $1 --network host -v $2:/data apache/kvrocks:latest --bind 127.0.0.1 --port $3 --dir /data --redis-databases 16 --rocksdb.write_options.sync yes "${@:4}" >/dev/null
	for i in $(seq 1 60); do redis-cli -p $3 ping 2>/dev/null | grep -q PONG && return 0; sleep 1; done
	echo "kvrocks $1 did not start"
	docker logs --tail 5 $1
	exit 1
}

probe() {
	local P=$1
	c() { redis-cli -p $P -n 15 "$@" 2>&1; }
	c flushdb >/dev/null
	local SL SR
	SL=$(c script load "$LK")
	SR=$(c script load "$RS")
	printf '\x00\x41\xed\x00\x00\x03\xe8\x00\x00\x03\xe8' | redis-cli -p $P -n 15 -x set i1 >/dev/null
	printf '\x02\x00\x00\x00\x00\x00\x00\x00\x02' | redis-cli -p $P -n 15 -x hset d1 sub >/dev/null
	printf '\x01\x00\x00\x00\x00\x00\x00\x00\x03' | redis-cli -p $P -n 15 -x hset d2 f >/dev/null
	printf '\x00\x41\xc0\x00\x00\x03\xe8\x00\x00\x03\xe8' | redis-cli -p $P -n 15 -x set i2 >/dev/null
	printf '\x00\x81\xa4\x00\x00\x03\xe8\x00\x00\x03\xe8' | redis-cli -p $P -n 15 -x set i3 >/dev/null
	printf '\x01\x00\x10\x00\x00\x00\x00\x00\x00' | redis-cli -p $P -n 15 -x hset d1 big >/dev/null
	echo "lookup ok: $(c evalsha $SL 2 d1 sub | od -An -tx1 | tr -s ' \n' ' ')"
	echo "lookup ro: $(c evalsha_ro $SL 2 d1 sub | od -An -tx1 | tr -s ' \n' ' ')"
	echo "lookup enoent: $(c evalsha $SL 2 d1 nope | grep -o 'ENOENT')"
	echo "lookup big: $(c evalsha $SL 2 d1 big | grep -o 'ENOTSUP')"
	echo "resolve ok: $(c evalsha $SR 3 1 sub/f 1000 1000 | od -An -tx1 | tr -s ' \n' ' ')"
	echo "resolve eacces: $(c evalsha $SR 3 1 sub/f 2000 2000 | grep -o 'EACCESS')"
	echo "resolve enotdir: $(c evalsha $SR 3 1 sub/f/x 1000 1000 | grep -o 'ENOTDIR')"
	echo "resolve enoent: $(c evalsha $SR 3 1 sub/zz 1000 1000 | grep -o 'ENOENT')"
	echo "noscript: $(c evalsha 0000000000000000000000000000000000000000 0 | grep -o 'NOSCRIPT')"
	echo "wrongtype: $(c set s x >/dev/null; c hincrby s f 1)"
	echo "overflow: $(c hset h f 9223372036854775807 >/dev/null; c hincrby h f 1)"
	for cmd in "evalsha $SL 2 d1 sub" "evalsha_ro $SL 2 d1 sub" "hget d1 sub" "eval return(1) 1 d1"; do
		local out
		out=$( (printf 'WATCH d1\r\n'; sleep 0.3; printf 'MULTI\r\nHDEL d1 probe\r\nEXEC\r\n'; sleep 0.3) | redis-cli -p $P -n 15 &
			sleep 0.1
			redis-cli -p $P -n 15 $cmd >/dev/null
			wait)
		if [ "$(echo "$out" | tail -1)" = "0" ]; then r=committed; else r=aborted; fi
		echo "watch d1 then [${cmd%% *} ${cmd##* }] by another client: exec $r"
	done
}

kvrocks gate-kv $S/stand/gate-kv $KV
echo "kvrocks $(redis-cli -p $KV info server | grep -a kvrocks_version | tr -d '\r')" | tee $R/versions.txt
echo "redis $(redis-cli -p $REDIS info server | grep -a '^redis_version' | tr -d '\r')" | tee -a $R/versions.txt
missing=0
for c in $(awk '{print $2}' rnd/results/gate/commands.txt) CONFIG DBSIZE FLUSHALL CLIENT MSET ZRANGE EVALSHA_RO; do
	[ -n "$(redis-cli -p $KV command info $c | head -1)" ] || { echo "MISSING $c"; missing=$((missing + 1)); }
done
echo "commands missing on kvrocks: $missing" | tee $R/coverage.txt
probe $REDIS > $R/probe.redis
probe $KV > $R/probe.kvrocks
diff $R/probe.redis $R/probe.kvrocks > $R/probe.diff || true
cat $R/probe.diff

SL=$(redis-cli -p $KV -n 15 script load "$LK")
bench() { redis-benchmark -p $KV --dbnum 15 -c 32 -n ${2:-60000} -q $1 2>&1 | tr '\r' '\n' | grep -a 'requests per second' | tail -1; }
{
	for rep in 1 2 3; do
		for bg in "evalsha $SL 2 d1 sub" "evalsha_ro $SL 2 d1 sub" "hget d1 sub"; do
			timeout 60 redis-benchmark -p $KV --dbnum 15 -c 32 -n 100000000 -q $bg >/dev/null 2>&1 &
			bp=$!
			sleep 2
			echo "rep$rep background=${bg%% *} set: $(bench 'set k v')"
			kill $bp
			wait $bp 2>/dev/null || true
		done
	done
} | tee $R/interference.txt

mountpoint -q $S/stand/gate-full && umount $S/stand/gate-full
mount -t tmpfs -o size=24m tmpfs $S/stand/gate-full
chmod 777 $S/stand/gate-full
kvrocks gate-full $S/stand/gate-full $FULL
head -c 150000 /dev/urandom | base64 -w0 > $S/stand/gate-blob
{
	for i in $(seq 1 400); do
		r=$(redis-cli -p $FULL -x set fill$i < $S/stand/gate-blob 2>&1)
		[ "$r" = OK ] || { echo "fill stopped at $i: $r"; break; }
	done
	echo "exec on a full disk:"
	printf 'MULTI\r\nSET tx1 a\r\nHSET tx2 f v\r\nINCRBY tx3 5\r\nEXEC\r\n' | redis-cli -p $FULL
	echo "after exec: tx1=$(redis-cli -p $FULL get tx1) tx3=$(redis-cli -p $FULL get tx3) tx2.f=$(redis-cli -p $FULL hget tx2 f)"
	mount -o remount,size=64m $S/stand/gate-full
	for t in 0 5 30 60 120; do
		sleep $t
		echo "space freed, write after +${t}s: $(redis-cli -p $FULL set after 1 2>&1)"
	done
	docker restart gate-full >/dev/null
	for i in $(seq 1 30); do redis-cli -p $FULL ping 2>/dev/null | grep -q PONG && break; sleep 1; done
	echo "after restart: $(redis-cli -p $FULL set after 1) fill1=$(redis-cli -p $FULL strlen fill1) tx1=$(redis-cli -p $FULL get tx1)"
} | tee $R/enospc.txt
docker rm -f gate-kv gate-full >/dev/null
umount $S/stand/gate-full
