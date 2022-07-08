#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
./build-dev-image.sh

cd "$rootdir"
docker run -it --rm -v "$PWD:/benchbase" benchbase-dev
