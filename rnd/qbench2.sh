#!/bin/bash
S=${S:-/tmp/rnd}
M=redis://127.0.0.1:6381/2
for q in 0 1000 100000; do
  $S/qbench-main -meta $M -quotas $q -periods 1 -build setup >/dev/null 2>&1
  for b in main branch2; do $S/qbench-$b -meta $M -setup=false -quotas $q -periods 5 -build $b > $S/results/qbench2-$b-$q.json 2> $S/results/qbench2-$b-$q.err; grep "Q=" $S/results/qbench2-$b-$q.err; done
done
echo done
