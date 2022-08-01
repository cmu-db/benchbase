#!/bin/bash

set -eu

# When we are running the full image we don't generally want to have to rebuild it repeatedly.
CLEAN_BUILD=${CLEAN_BUILD:-false}

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh
./build-full-image.sh

if [ "$imagename" != 'benchbase' ]; then
    echo "ERROR: Unexpected imagename: $imagename" >&2
fi

cd "$rootdir"
mkdir -p results/
set -x
docker run -it --rm \
    --env=http_proxy="${http_proxy:-}" --env=https_proxy="${https_proxy:-}" \
    --env BENCHBASE_PROFILE="$BENCHBASE_PROFILE" \
    --user "$CONTAINERUSER_UID:$CONTAINERUSER_GID" \
    -v "$PWD/results:/benchbase/results" benchbase-$BENCHBASE_PROFILE:latest $*
set +x
