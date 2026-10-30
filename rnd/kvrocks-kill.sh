#!/bin/bash
set -uo pipefail
S=${S:-/tmp/rnd}
B=$S/bin
R=$S/results/kill
P=${P:-6674}
C=${C:-kvrocks-kill}
M=redis://127.0.0.1:$P/0
mkdir -p $R
$B/nsgen -meta $M -users 2 -dirs 4 -files 500 -threads 32 -random -families=false > $R/nsgen.out 2> $R/nsgen.err || { echo "nsgen failed"; tail -3 $R/nsgen.err; exit 1; }
for rep in 1 2 3; do
	$B/mixload -meta $M -random -users 2 -dirs 4 -files 500 -readers 16 -writers 16 -seconds 15 -label kill$rep > $R/mix$rep.out 2> $R/mix$rep.err &
	mp=$!
	sleep $((4 + rep * 2))
	docker kill -s KILL $C >/dev/null
	wait $mp
	echo "rep$rep mixload exit=$? last=$(grep -a 'I | ' $R/mix$rep.err | tail -1 | cut -c28-)"
	docker start $C >/dev/null
	for i in $(seq 1 30); do redis-cli -p $P ping 2>/dev/null | grep -q PONG && break; sleep 1; done
	$B/juicefs dump $M $R/dump$rep.json --fast > /dev/null 2>&1 || { echo "dump failed"; exit 1; }
	python3 - $R/dump$rep.json <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
c = d["Counters"]
n = {"inodes": 0, "space": 0, "empty": 0}
def walk(x, top=False):
    a = x.get("attr", {})
    if a.get("type") == "directory":
        if not top:
            n["inodes"] += 1
            n["space"] += 4096
        for ch in (x.get("entries") or {}).values():
            walk(ch)
    else:
        L = a.get("length", 0)
        n["inodes"] += 1
        n["empty"] += L == 0
        n["space"] += 4096 if L == 0 else (L + 4095) // 4096 * 4096
walk(d["FSTree"], True)
walk(d.get("Trash") or {}, True)
print(f"usedInodes {c['usedInodes']} tree+trash {n['inodes']} diff {c['usedInodes'] - n['inodes']} | usedSpace {c['usedSpace']} tree+trash {n['space']} diff {c['usedSpace'] - n['space']} | empty {n['empty']} delfiles {len(d.get('DelFiles') or [])} sustained {len(d.get('Sustained') or [])}")
PY
done
