#!/bin/sh
set -e
ref=${1:-origin/integration/metadata-tiering}
dir=$(mktemp -d)
git archive "$ref" | tar -x -C "$dir"
(cd "$dir" && STATIC=1 REVISION=$(git rev-parse --short "$ref") make juicefs)
cp "$dir/juicefs" "$(dirname "$0")/juicefs"
docker build -t "juicefs-mount:ce-tiering-$(git rev-parse --short "$ref")" "$(dirname "$0")"
