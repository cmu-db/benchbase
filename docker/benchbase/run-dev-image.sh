#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
./build-dev-image.sh

# Sync with Dockerfile
containeruser_uid=1000

if [ $EUID != $containeruser_uid ]; then
    echo "NOTE: Local host user uid ($EUID) does not match container user uid $containeruser_uid." >&2
    # TODO: Fix it up for them?
fi

cd "$rootdir"
docker run -it --rm -v "$PWD:/benchbase" benchbase-dev
