#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh

logs_child_pid=
container_id=
function trap_ctrlc() {
    docker stop -t 1 $container_id >/dev/null || true
    if [ -n "$logs_child_pid" ]; then
        kill $logs_child_pid 2>/dev/null || true
    fi
    exit 1
}

# Build the requested profiles using the dev image.
./build-dev-image.sh
# Use non-interactive mode so that the build doesn't prompt us to accept git ssh keys.
# But setup some Ctrl-C handlers as well.
container_id=$(INTERACTIVE='false' ./run-dev-image.sh /benchbase/docker/benchbase/devcontainer/build-in-container.sh)
trap trap_ctrlc SIGINT SIGTERM
echo "INFO: build-devcontainer-id: $container_id"
docker logs -f $container_id &
logs_child_pid=$!
rc=$(docker wait $container_id)
trap - SIGINT SIGTERM
if [ "$rc" != 0 ]; then
    echo "ERROR: Build in devcontainer failed." >&2
    exit $rc
fi

cd "$rootdir"

# Make (hard-linked) copies of those results that we can put into the image.
rm -rf .docker-build-stage/
mkdir -p .docker-build-stage/
for profile in $BENCHBASE_PROFILES; do
    if ! [ -f "profiles/$profile/benchbase.jar" ]; then
        echo "ERROR: build for $profile appears to have failed." >&2
        exit 1
    fi
    cp -al "profiles/$profile" .docker-build-stage/
done

set -x
docker build --progress=plain \
    --build-arg="http_proxy=${http_proxy:-}" --build-arg="https_proxy=${https_proxy:-}" \
    --build-arg MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
    --build-arg BENCHBASE_PROFILES="${BENCHBASE_PROFILES}" \
    --build-arg CONTAINERUSER_UID="$CONTAINERUSER_UID" --build-arg CONTAINERUSER_GID="$CONTAINERUSER_GID" \
    -t benchbase:latest ${image_tag_args:-} -f ./docker/benchbase/fullimage/Dockerfile .
set +x

# Cleanup the temporary copies.
rm -rf .docker-build-stage/
