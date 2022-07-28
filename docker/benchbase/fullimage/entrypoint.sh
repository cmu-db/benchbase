#!/bin/bash

# fullimage docker entrypoint script

set -eu

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
cd /benchbase
echo "INFO: Using environment variable BENCHBASE_PROFILE=${BENCHBASE_PROFILE} with args: $*" >&2
cd ./profiles/${BENCHBASE_PROFILE}/
java -jar benchbase.jar $*
