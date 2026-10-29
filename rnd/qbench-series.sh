#!/bin/bash
S=${S:-/tmp/rnd}
R=$S/results
for q in 100 1000 10000 100000; do
  echo "== setup Q=$q $(date -u +%T)"
  $S/qbench-main -quotas $q -periods 1 -build setup >/dev/null 2>$R/qbench-setup-$q.err || { echo setup failed; tail -3 $R/qbench-setup-$q.err; exit 1; }
  for b in v142 main branch; do
    $S/qbench-$b -setup=false -quotas $q -periods 5 -build $b > $R/qbench-$b-$q.json 2> $R/qbench-$b-$q.err
    grep -E "Q=" $R/qbench-$b-$q.err | tail -1
  done
done
echo "== done $(date -u +%T)"
