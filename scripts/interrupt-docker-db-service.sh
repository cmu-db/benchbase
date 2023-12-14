#!/bin/bash

# Randomly interrupt the docker db service by killing it.
# Used to test connection error handling during a benchmark.

set -eu
set -x

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-}"
SERVICE_NAME="${1:-$BENCHBASE_PROFILE}"
OFFSET="${2:-10}"
SPLAY="${3:-10}"
DELAY=${4:-1}

if [ -z "$SERVICE_NAME" ]; then
    echo "ERROR: Missing SERVICE_NAME (e.g., mysql, postgres, sqlserver, etc.)" >&2
    exit 1
elif [ "$SERVICE_NAME" == 'cockroachdb' ]; then
    # FIXME: TODO: Unclear how to do this for cockroachdb yet since it's multi node.
    echo "ERROR: Not (currently) supported for cockroachdb." >&2
    exit 1
elif [ "$SERVICE_NAME" == 'sqlite' ]; then
    echo "ERROR: Not supported for sqlite." >&2
    exit 1
fi

function lookup_container_id() {
    local service_name="$1"
    if [ -n "${GITHUB_ACTION:-}" ]; then
        # In GitHub Actions, the container name is somewhat obfuscated, so we need
        # to look it up by --network-alias instead.
        # Note: this logic picks the first container with the given network alias,
        # which may cause issues if there's more than one.
        docker ps -q | xargs docker container inspect | jq -r '
            [
                [
                    .[] | {
                        "Id": .Id,
                        "Image": .Config.Image,
                        "Hostname": .Config.Hostname,
                        "NetworkAliases": (.NetworkSettings.Networks | to_entries | .[].value.Aliases)
                    }
                ] | .[] | select(.NetworkAliases | index("'$service_name'"))
            ] | first | .Id'
    else
        docker ps --filter "name=^/${service_name}\$" --format '{{.ID}}'
    fi
}

function interrupt_container() {
    local container_id="$1"
    local signal="${2:-}"
    local timeout=$(($RANDOM % $SPLAY + $OFFSET))
    echo "[$(date +%s)]: Interrupting container $container_id in $timeout seconds ..."
    sleep $timeout
    if [ -n "$signal" ]; then
        docker kill -s "$signal" "$container_id"
    else
        docker kill "$container_id"
    fi
}

container_id=$(lookup_container_id "$SERVICE_NAME")

if [ -z "$container_id" ]; then
    echo "ERROR: Failed to lookup container_id for $SERVICE_NAME." >&2
fi

interrupt_container $container_id

sleep $DELAY
docker start $container_id

# Give it some time to startup and run normally again.
sleep 10

interrupt_container $container_id KILL

sleep $DELAY
docker start $container_id
