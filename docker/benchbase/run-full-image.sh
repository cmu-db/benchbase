#!/bin/bash

set -eu

# When we are running the full image we don't generally want to have to rebuild it repeatedly.
CLEAN_BUILD=${CLEAN_BUILD:-false}
BUILD_IMAGE=${BUILD_IMAGE:-true}

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh
if ! docker image ls --quiet benchbase-$BENCHBASE_PROFILE:latest | grep -q .; then
    # Missing image, need to build it.
    CLEAN_BUILD=true
fi

if [ "$BUILD_IMAGE" != 'false' ]; then
    ./build-full-image.sh
fi

if [ "$imagename" != 'benchbase' ]; then
    echo "ERROR: Unexpected imagename: $imagename" >&2
fi

SRC_DIR="$rootdir"
if [ -n "${LOCAL_WORKSPACE_FOLDER:-}" ]; then
    SRC_DIR="$LOCAL_WORKSPACE_FOLDER"
fi

cd "$rootdir"
mkdir -p results/
set -x
docker run -it --rm \
    ${EXTRA_DOCKER_ARGS:-} \
    --env=http_proxy="${http_proxy:-}" --env=https_proxy="${https_proxy:-}" --env=no_proxy="${no_proxy:-}" \
    --env BENCHBASE_PROFILE="$BENCHBASE_PROFILE" \
    --user "$CONTAINERUSER_UID:$CONTAINERUSER_GID" \
    -v "$SRC_DIR/results:/benchbase/results" benchbase-$BENCHBASE_PROFILE:latest $*
