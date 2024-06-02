#!/bin/bash

set -eu
set -x

benchmark="${1:-noop}"

# Let these pass through from the .env file from the devcontainer.
export BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
export BENCHBASE_PROFILES="$BENCHBASE_PROFILE"
PROFILE_VERSION=${PROFILE_VERSION:-latest}

# When we are running the full image we don't generally want to have to rebuild it repeatedly.
export CLEAN_BUILD="${CLEAN_BUILD:-false}"

# Move to the repo root.
scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/..")
cd "$rootdir"

# Do the rebuild (if necessary) build first.
if [ "${BUILD_IMAGE:-true}" != "false" ]; then
    SKIP_TESTS=${SKIP_TESTS:-true} ./docker/benchbase/build-full-image.sh
fi

EXTRA_DOCKER_ARGS=''
if [ "$BENCHBASE_PROFILE" == 'sqlite' ]; then
    # Map the sqlite db back to the host.
    touch $PWD/$benchmark.db
    SRC_DIR="$PWD"
    if [ -n "${LOCAL_WORKSPACE_FOLDER:-}" ]; then
        SRC_DIR="$LOCAL_WORKSPACE_FOLDER"
    fi
    EXTRA_DOCKER_ARGS="-v $SRC_DIR/$benchmark.db:/benchbase/profiles/sqlite/$benchmark.db"

    if echo "$benchmark" | egrep -qx '(templated|chbenchmark)'; then
        # See notes below:
        EXTRA_DOCKER_ARGS+=" -v $SRC_DIR/$benchmark.db:/benchbase/profiles/sqlite/tpcc.db"
    fi
else
    if [ ! -x "docker/${BENCHBASE_PROFILE}-${PROFILE_VERSION}/up.sh" ]; then
        echo "ERROR: No docker up.sh script available for '$BENCHBASE_PROFILE'"
    fi

    "./docker/${BENCHBASE_PROFILE}-${PROFILE_VERSION}/up.sh"
fi

if [ "${SKIP_LOAD_DB:-false}" != 'true' ]; then
    # For templated benchmarks, we need to preload some data for the test since by
    # design, templated benchmarks do not support the 'load' operation
    # In this case, we load the tpcc data.
    if echo "$benchmark" | egrep -qx '(templated|chbenchmark)'; then
        load_benchmark='tpcc'

        echo "INFO: Loading tpcc data for templated benchmark"
        if [ "$BENCHBASE_PROFILE" == 'sqlite' ]; then
            # Sqlite will load much faster if we disable sync.
            config="config/sample_tpcc_nosync_config.xml"
        else
            config="config/sample_tpcc_config.xml"
        fi

        BUILD_IMAGE=false EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
        ./docker/benchbase/run-full-image.sh \
            --config "$config" --bench "$load_benchmark" \
            --create=true --load=true --execute=false
    fi

    # For chbenchmark, we also load it's data in addition to tpcc.
    if ! echo "$benchmark" | egrep -qx '(templated)'; then
        echo "INFO: Loading $benchmark data"
        load_benchmark="$benchmark"
        config="config/sample_${benchmark}_config.xml"

        BUILD_IMAGE=false EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
        ./docker/benchbase/run-full-image.sh \
            --config "$config" --bench "$load_benchmark" \
            --create=true --load=true --execute=false
    fi
else
    echo "INFO: Skipping load of $benchmark data"
fi

if [ "${WITH_SERVICE_INTERRUPTIONS:-false}" == 'true' ]; then
    # Randomly interrupt the docker db service by killing it.
    # Used to test connection error handling during a benchmark.
    (sleep 10 && ./scripts/interrupt-docker-db-service.sh "$BENCHBASE_PROFILE") &
fi

rm -f results/histograms.json
BUILD_IMAGE=false EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
./docker/benchbase/run-full-image.sh \
    --config "config/sample_${benchmark}_config.xml" --bench "$benchmark" \
    --create=false --load=false --execute=true \
    --sample 1 --interval-monitor 1000 \
    --json-histograms results/histograms.json
rc=$?
wait    # for the interrupt script, if any
if [ $rc -ne 0 ]; then
    echo "ERROR: benchmark execution failed with exit code $rc" >&2
    exit $rc
fi
# else, check that the results look ok
./scripts/check_latest_benchmark_results.sh "$benchmark"
./scripts/check_histogram_results.sh results/histograms.json