#!/bin/bash

set -eu
set -x

benchmark="${1:-noop}"

# Let these pass through from the .env file from the devcontainer.
export BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
export BENCHBASE_PROFILES="$BENCHBASE_PROFILE"

# When we are running the full image we don't generally want to have to rebuild it repeatedly.
export CLEAN_BUILD="${CLEAN_BUILD:-false}"

# Move to the repo root.
scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/..")
cd "$rootdir"

if [ ! -x "docker/${BENCHBASE_PROFILE}-latest/up.sh" ]; then
    echo "ERROR: No docker up.sh script available for '$BENCHBASE_PROFILE'"
fi

pushd "docker/${BENCHBASE_PROFILE}-latest"
./up.sh
popd

SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host" \
./docker/benchbase/run-full-image.sh \
    --config "config/sample_${benchmark}_config.xml" --bench "$benchmark" \
    --create=true --load=true --execute=true \
    --sample 1 --interval-monitor 1000 \
    --json-histograms results/histograms.json