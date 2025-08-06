#!/bin/bash

# A simple script to check the error rate in the --json-histograms file results.

set -eu
set -o pipefail

# Move to the root of the repository.
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/.."

if ! type jq >/dev/null 2>&1; then
    echo "ERROR: Missing jq utility.  Please install it (e.g. using apt-get)." >&2
    exit 1
fi

results_json="${1:-results/histograms.json}"
threshold="${2:-0.01}"

echo "INFO: Checking that error results in $results_json are below $threshold"

if ! [ -s "$results_json" ]; then
    echo "ERROR: Missing or empty results file: $results_json" >&2
    exit 1
fi

# First transform the histograms into a single object with aggregate count of error and completed samples.
summary_json=$(cat "$results_json" | jq -e '
    to_entries
    | {
        "completed_samples": ( .[] | select(.key == "completed") | .value.NUM_SAMPLES ),
        "errored_samples": ( [ .[] | select(.key != "completed" ) | .value.NUM_SAMPLES ] | add )
    }
    | .error_rate = (.errored_samples / .completed_samples)'
)

# Print it out for debugging.
echo "$summary_json" | jq -e .

# Check the value of the error rate.
if ! echo "$summary_json" | jq -e '(.error_rate < '$threshold')' >/dev/null; then
    echo "ERROR: error rate is too high"
    exit 1
fi
# else
echo 'OK!'
exit 0