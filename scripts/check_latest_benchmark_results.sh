#!/bin/bash

# Simple script to try and sanity check the results of the latest benchmark run.

set -eu
set -x

# Move to the root of the repository.
scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/..")
cd "$rootdir"

# Check that the results directory exists.
if ! [ -d results ]; then
    echo "ERROR: Missing results directory" >&2
    exit 1
fi

BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-}"
benchmark="${1:-}"
if [ -z "$benchmark" ]; then
    echo "ERROR: Missing benchmark argument." >&2
fi

config_file=$(ls -1t results/${benchmark}_*.config.xml | head -n1)
if [ -z "$config_file" ]; then
    echo "ERROR: Failed to find $benchmark benchmark results files." >&2
    exit 1
fi

ts=$(basename "$config_file" | sed -e "s/^${benchmark}_//" -e 's/\.config\.xml//')
summary_json="results/${benchmark}_${ts}.summary.json"

if ! type xmllint 2>/dev/null; then
    # Attempt to install xmllint.
    # TODO: Add support for non apt based systems.
    sudo -n /bin/bash -c "apt-get update && apt-get install -y libxml2-utils" || true
fi
if ! type xmllint 2>/dev/null; then
    echo "ERROR: Missing xmllint utility." >&2
    exit 1
fi

# TODO: include warmup?
expected_runtime=$(xmllint --xpath '//works/work/time/text()' "$config_file" | awk '{ print sum=sum+$1 }' | tail -n1)

if xmllint --xpath '//works/work/serial/text()' "$config_file" | grep -q -x true; then
    if [ -n "$expected_runtime" ]; then
        echo "ERROR: Unhandled: Found expected runtime in config file for serial workloads." >&2
        exit 1
    fi

    expected_query_count=$(xmllint --xpath '//works/work/weights/text()' "$config_file" | sed 's/,/\n/g' | grep -c -x 1 || true)
    if xmllint --xpath '//works/work/weights/text()' "$config_file" | sed 's/,/\n/g' | grep -q -v '^[01]$'; then
        echo "ERROR: Unsupported weight specification for serial workloads.  Only 0,1 are handled currently." >&2
        exit 1
    fi

    measured_requests=$(cat "$summary_json" | jq -e '.["Measured Requests"]')
    if [ "$measured_requests" -ne "$expected_query_count" ]; then
        echo "ERROR: Benchmark measured requests ($measured_requests) was less than expected ($expected_query_count) or failed to parse output." >&2
        exit 1
    else
        echo "OK: Benchmark measured requests ($measured_requests) matched ($expected_query_count)."
    fi
else
    if [ -z "$expected_runtime" ]; then
        echo "ERROR: Failed to find expected runtime in config file: $config_file" >&2
        exit 1
    fi

    elapsed_time=$(cat "$summary_json" | jq -e '.["Elapsed Time (nanoseconds)"] / 1000000000 | round')
    if [ "$elapsed_time" -lt "$expected_runtime" ]; then
        echo "ERROR: Benchmark elapsed runtime ($elapsed_time) was less than expected ($expected_runtime) or failed to parse output." >&2
        exit 1
    else
        echo "OK: Benchmark elapsed runtime ($elapsed_time) matched expected ($expected_runtime)."
    fi
fi