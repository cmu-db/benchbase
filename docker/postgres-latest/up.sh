#!/bin/bash

set -eu
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/"

services="$@"

docker compose up -d $services

# Wait until ready
for i in {1..5}; do
    if /usr/bin/docker inspect --format="{{print .State.Health.Status}}" postgres | grep -q -x healthy; then
        break
    else
        sleep 10
    fi
done

function run_psql_in_docker() {
    set -x
    docker exec --env PGPASSWORD=password postgres psql -h localhost -U admin benchbase --csv -c "$@"
    set +x
}

run_psql_in_docker "CREATE EXTENSION IF NOT EXISTS pg_stat_statements"
run_psql_in_docker "SELECT COUNT(*) FROM pg_stat_statements" | egrep '^[1-9][0-9]*$'
