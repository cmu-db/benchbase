#!/bin/bash

# Randomly interrupt the docker db service by killing it.
# Used to test connection error handling during a benchmark.

set -eu
set -x

SERVICE_NAME="${1:${BENCHBASE_PROFILE:-}}"
OFFSET="${2:-20}"
SPLAY="${3:-10}"
DELAY=${4:-1}

if [ -z "$SERVICE_NAME" ]; then
    echo "ERROR: Missing SERVICE_NAME (e.g., mysql, postgres, sqlserver, etc.)" >&2
    exit 1
elif [ "$SERVICE_NAME" == 'sqlite' ]; then
    echo "ERROR: Not supported for sqlite." >&2
    exit 1
fi

container_id=$(docker ps --filter "name=^/${SERVICE_NAME}\$" --format '{{.ID}}')

if [ -z "$container_id" ]; then
    echo "ERROR: Failed to lookup container_id for $SERVICE_NAME." >&2
fi

timeout=$(($RANDOM % $SPLAY + $OFFSET))
echo "[$(date +%s)]: Interrupting $SERVICE_NAME container $container_id in $timeout seconds ..."
docker kill $SERVICE_NAME

sleep $DELAY
docker start $SERVICE_NAME

timeout=$(($RANDOM % $SPLAY + $OFFSET))
echo "[$(date +%s)]: Killing $SERVICE_NAME container $container_id in $timeout seconds ..."
docker kill -s KILL $SERVICE_NAME

sleep $DELAY
docker start $SERVICE_NAME
