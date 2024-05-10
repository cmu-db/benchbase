#!/bin/bash

set -eu
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/"

docker compose up -d

# Wait until ready
for i in {1..30}; do
    if docker exec postgres pg_isready; then
        break
    else
        sleep 1
    fi
done

function run_psql_in_docker() {
    set -x
    docker exec --env PGPASSWORD=password postgres psql -h localhost -U admin benchbase --csv -c "$@"
    set +x
}

run_psql_in_docker "CREATE EXTENSION pg_stat_statements"
run_psql_in_docker "SELECT COUNT(*) FROM pg_stat_statements" | egrep '^[1-9][0-9]*$'