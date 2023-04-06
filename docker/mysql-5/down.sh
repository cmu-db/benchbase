#!/bin/bash

set -eu
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/"

docker compose down --remove-orphans --volumes
