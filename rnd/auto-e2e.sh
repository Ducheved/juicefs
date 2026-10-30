#!/bin/bash
S=/tmp/claude-0/-home-user/40fd7f72-6e5a-5b1e-910b-f8925f83d035/scratchpad; C=$S/juicefs-cli; M=redis://127.0.0.1:6379/13; E=$S/e2e-auto
sleep 62
date -u +%T; for u in 1 2 3; do ls $E/b/home/u$u/docs >/dev/null; cat $E/b/home/u$u/docs/f1 >/dev/null; done
sleep 66
echo "access: $(redis-cli -n 13 hgetall capsuleAccess | tr '\n' ' ') hits: $(redis-cli -n 13 hgetall capsuleHits | tr '\n' ' ')"
echo "keys before: $(redis-cli -n 13 dbsize) mem: $(redis-cli info memory | grep '^used_memory:')"
$C metadata auto-evict $M --idle 100s --min-resident 0 --once --log-level warn
echo "keys after: $(redis-cli -n 13 dbsize)"
$C metadata status $M --log-level error | cut -c1-120
(cd $E/a && find home -type f -exec sha256sum {} + | sort > $E/manifest2.txt); cmp $E/manifest.txt $E/manifest2.txt || { echo "manifest differs after transparent load"; exit 1; }
echo "manifest equal after transparent load"
echo "keys after read: $(redis-cli -n 13 dbsize)"
$C metadata status $M --log-level error | wc -l
grep -c "Loaded metadata" $E/a.log $E/b.log
grep -E "ERROR|FATAL" $E/a.log $E/b.log | tail -5
echo "== done $(date -u +%T)"
