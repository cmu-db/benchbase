#!/bin/bash

set -eu

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
./build-full-image.sh

cd "$rootdir"
mkdir -p results/
docker run -it --rm -v "$PWD/results:/benchbase/results" --env "BENCHBASE_PROFILE=$BENCHBASE_PROFILE" benchbase $*
