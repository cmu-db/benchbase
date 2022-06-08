#!/bin/bash

set -eu

BENCHBASE_PROFILES="${BENCHBASE_PROFILES:-cockroachdb mariadb mysql postgres spanner phoenix sqlserver}"

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")
cd "$rootdir"

http_proxy_host=''
http_proxy_port=''
https_proxy_host=''
https_proxy_port=''

if echo "$http_proxy" | egrep -q 'http[s]?://[^:]+:[0-9]+[/]?$'; then
    http_proxy_host=$(echo "$http_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\1|')
    http_proxy_port=$(echo "$http_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\2|')
fi

if echo "$https_proxy" | egrep -q 'http[s]?://[^:]+:[0-9]+[/]?$'; then
    https_proxy_host=$(echo "$https_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\1|')
    https_proxy_port=$(echo "$https_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\2|')
fi

target=
tag=
basename=$(basename "$0")
if [ "$basename" == "build-full-image.sh" ]; then
    target='fullimage'
    tag='benchbase'
elif [ "$basename" == "build-dev-image.sh" ]; then
    target='devcontainer'
    tag='benchbase-dev'
else
    echo "ERROR: Unhandled mode: $basename" >&2
    exit 1
fi

GID=$(getent passwd $UID | cut -d: -f4)

set -x
docker build --progress=plain --build-arg=http_proxy=${http_proxy:-} --build-arg=https_proxy=${https_proxy:-} \
    --build-arg MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
    --build-arg BENCHBASE_PROFILES="${BENCHBASE_PROFILES}" \
    --build-arg UID=$UID --build-arg GID=$GID \
    -t $tag -f ./docker/benchbase/Dockerfile --target $target .
