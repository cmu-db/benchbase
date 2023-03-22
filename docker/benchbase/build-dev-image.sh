#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir"
. ./common-env.sh

set -x
# See comments in devcontainer/Dockerfile explaining empty build context (/dev/null or empty tmpdir).
tmpdir=$(mktemp -d)
docker build $docker_build_args \
    --build-arg BUILDKIT_INLINE_CACHE=1 \
    --build-arg="http_proxy=${http_proxy:-}" --build-arg="https_proxy=${https_proxy:-}" --build-arg="no_proxy=${no_proxy:-}" \
    --build-arg MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
    --build-arg BENCHBASE_PROFILES="${BENCHBASE_PROFILES}" \
    --build-arg CONTAINERUSER_UID="$CONTAINERUSER_UID" --build-arg CONTAINERUSER_GID="$CONTAINERUSER_GID" \
    --tag benchbase-dev:latest ${image_tag_args:-} -f ./devcontainer/Dockerfile "$tmpdir"
rmdir "$tmpdir"
set +x
