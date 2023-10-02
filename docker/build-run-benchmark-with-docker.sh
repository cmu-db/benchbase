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

EXTRA_DOCKER_ARGS=''
if [ "$BENCHBASE_PROFILE" == 'sqlite' ]; then
    # Map the sqlite db back to the host.
    touch $PWD/$benchmark.db
    SRC_DIR="$PWD"
    if [ -n "${LOCAL_WORKSPACE_FOLDER:-}" ]; then
        SRC_DIR="$LOCAL_WORKSPACE_FOLDER"
    fi
    EXTRA_DOCKER_ARGS="-v $SRC_DIR/$benchmark.db:/benchbase/profiles/sqlite/$benchmark.db"

    if [ "$benchmark" == 'templated' ]; then
        # See notes below:
        EXTRA_DOCKER_ARGS+=" -v $SRC_DIR/$benchmark.db:/benchbase/profiles/sqlite/tpcc.db"
    fi
else
    if [ ! -x "docker/${BENCHBASE_PROFILE}-${PROFILE_VERSION}/up.sh" ]; then
        echo "ERROR: No docker up.sh script available for '$BENCHBASE_PROFILE'"
    fi

    "./docker/${BENCHBASE_PROFILE}-${PROFILE_VERSION}/up.sh"
fi

CREATE_DB_ARGS='--create=true --load=true'
if [ "${SKIP_LOAD_DB:-false}" == 'true' ]; then
    CREATE_DB_ARGS=''
elif [ "$benchmark" == 'templated' ]; then
    # For templated benchmarks, we need to preload some data for the test since by
    # design, templated benchmarks do not support the 'load' operation
    # In this case, we load the tpcc data.
    echo "INFO: Loading tpcc data for templated benchmark"
    if [ "$BENCHBASE_PROFILE" == 'sqlite' ]; then
        # Sqlite will load much faster if we disable sync.
        tpcc_config="config/sample_tpcc_nosync_config.xml"
    else
        tpcc_config="config/sample_tpcc_config.xml"
    fi
    SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
    ./docker/benchbase/run-full-image.sh \
        --config "$tpcc_config" --bench tpcc \
        $CREATE_DB_ARGS --execute=false

    # Mark those actions as completed.
    CREATE_DB_ARGS=''
    SKIP_TESTS=true
fi

SKIP_TESTS=${SKIP_TESTS:-true} EXTRA_DOCKER_ARGS="--network=host $EXTRA_DOCKER_ARGS" \
./docker/benchbase/run-full-image.sh \
    --config "config/sample_${benchmark}_config.xml" --bench "$benchmark" \
    $CREATE_DB_ARGS --execute=true \
    --sample 1 --interval-monitor 1000 \
    --json-histograms results/histograms.json
./scripts/check_histogram_results.sh results/histograms.json
