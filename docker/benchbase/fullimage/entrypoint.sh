#!/bin/bash

# fullimage docker entrypoint script

set -eu

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
cd /benchbase
id || true
ls -ld results || true
echo "INFO: Using environment variable BENCHBASE_PROFILE=${BENCHBASE_PROFILE} with args: $*" >&2
if ! [ -f "profiles/${BENCHBASE_PROFILE}/benchbase.jar" ]; then
    echo "ERROR: Couldn't find profile '${BENCHBASE_PROFILE}' in container image." >&2
    exit 1
fi
cd ./profiles/${BENCHBASE_PROFILE}/
ls -ld results || true
java -jar benchbase.jar $*
