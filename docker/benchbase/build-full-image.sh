#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh

# Build the requested profiles using the dev image.
./build-dev-image.sh
INTERACTIVE='false' ./run-dev-image.sh /benchbase/docker/benchbase/devcontainer/build-in-container.sh

cd "$rootdir"

# Make (hard-linked) copies of those results that we can put into the image.
rm -rf .docker-build-stage/
mkdir -p .docker-build-stage/
for profile in $BENCHBASE_PROFILES; do
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
