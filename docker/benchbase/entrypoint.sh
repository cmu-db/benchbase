#!/bin/bash

# fullimage docker entrypoint script

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
cd /benchbase
echo "INFO: Using environment variable BENCHBASE_PROFILE=${BENCHBASE_PROFILE} with args: $*" >&2
java -jar "./profiles/${BENCHBASE_PROFILE}/benchbase.jar" $*
