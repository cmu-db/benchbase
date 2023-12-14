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

# FIXME: Doesn't currently handle multiple workloads.
runtime=$(xmllint --xpath '//works/work/time/text()' "$config_file" || true)
# TODO: include warmup?

if [ -z "$runtime" ]; then
    # FIXME: Doesn't currently handle serial benchmarks.
    echo "ERROR: Failed to find expected runtime in config file: $config_file" >&2
    exit 1
fi

if ! cat "$summary_json" | jq -e '(.["Elapsed Time (nanoseconds)"] / 1000000000 | round) >= '$runtime; then
    echo "ERROR: Benchmark runtime is too short or failed to parse output." >&2
    exit 1
fi