#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh
./build-dev-image.sh

if [ "$imagename" != 'benchbase-dev' ]; then
    echo "ERROR: Unexpected imagename: $imagename" >&2
fi

cd "$rootdir"
set -x
docker run -it --rm \
    --env=http_proxy="${http_proxy:-}" --env=https_proxy="${https_proxy:-}" \
    --env MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
    --env BENCHBASE_PROFILES="$BENCHBASE_PROFILES" \
    --env CLEAN_BUILD="$CLEAN_BUILD" \
    --env TEST_TARGET="$TEST_TARGET" \
    --user "$CONTAINERUSER_UID:$CONTAINERUSER_GID" \
    -v "$PWD:/benchbase" benchbase-dev:latest $*
set +x
