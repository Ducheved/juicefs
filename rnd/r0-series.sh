#!/bin/bash
S=${S:-/tmp/rnd}
R=$S/results
M=redis://127.0.0.1:6380/1
run() { name=$1; shift; echo "== $name $(date -u +%FT%TZ)"; $S/nsgen -meta $M "$@" > $R/r0-$name.json 2> $R/r0-$name.err; tail -1 $R/r0-$name.err; }
run A-fan100      -users 100 -dirs 100  -files 100
run B-fan1000     -users 100 -dirs 10   -files 1000
run C-fan10       -users 100 -dirs 1000 -files 10
run D-empty       -users 100 -dirs 100  -files 100 -chunks 0
run E-big16chunk  -users 10  -dirs 10   -files 1000 -chunks 16
run F-5slices     -users 100 -dirs 10   -files 100 -slices 5
run G-mixed       -users 100 -dirs 100  -files 100 -xattr 0.1 -symlink 0.05 -hardlink 0.02
run H-quotas      -users 1000 -dirs 10  -files 100 -user-quota -dir-quota
echo "== done $(date -u +%FT%TZ)"
