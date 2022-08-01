#!/bin/bash

set -eu

INTERACTIVE="${INTERACTIVE:-true}"

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh
./build-dev-image.sh

if [ "$imagename" != 'benchbase-dev' ]; then
    echo "ERROR: Unexpected imagename: $imagename" >&2
fi

if [ "$INTERACTIVE" == 'true' ]; then
    INTERACTIVE_ARGS='-it'
elif [ "$INTERACTIVE" == 'false' ]; then
    # NOTE: When this mode is in effect Ctrl-C can't be passed to kill a run.
    # Instead, use "docker kill {container_id}"
    INTERACTIVE_ARGS='-d'
else
    echo "WARNING: Unhandled INTERACTIVE mode: '$INTERACTIVE'" >&2
fi

cd "$rootdir"
MAVEN_CONFIG="${MAVEN_CONFIG:-$HOME/.m2}"
mkdir -p "$MAVEN_CONFIG"
set -x
docker run ${INTERACTIVE_ARGS:-} --rm \
    --env=http_proxy="${http_proxy:-}" --env=https_proxy="${https_proxy:-}" \
    --env MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
    --env BENCHBASE_PROFILES="$BENCHBASE_PROFILES" \
    --env CLEAN_BUILD="$CLEAN_BUILD" \
    --env SKIP_TESTS="$SKIP_TESTS" \
    --user "$CONTAINERUSER_UID:$CONTAINERUSER_GID" \
    -v "$MAVEN_CONFIG:/home/containeruser/.m2" \
    -v "$PWD:/benchbase" benchbase-dev:latest $*
set +x
