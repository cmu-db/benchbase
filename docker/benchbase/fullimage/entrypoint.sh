#!/bin/bash

# fullimage docker entrypoint script

set -eu

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
cd /benchbase
echo "INFO: Using environment variable BENCHBASE_PROFILE=${BENCHBASE_PROFILE} with args: $*" >&2
if ! [ -f "profiles/${BENCHBASE_PROFILE}/benchbase.jar" ]; then
    echo "ERROR: Couldn't find profile '${BENCHBASE_PROFILE}' in container image." >&2
    exit 1
fi
cd ./profiles/${BENCHBASE_PROFILE}/
if ! [ -d results/ ] || ! [ -w results/ ]; then
    echo "ERROR: The results directory either doesn't exist or isn't writable." >&2
fi
exec java -jar benchbase.jar $*
