#!/bin/bash

set -eu
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/"

docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
