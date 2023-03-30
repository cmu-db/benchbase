#!/bin/bash

set -eu

# Move to the root of the repository.
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/.."

if ! type jq >/dev/null 2>&1; then
    echo "ERROR: Missing jq utility.  Please install it (e.g. using apt-get)i." >&2
    exit 1
fi

results_json="${1:-results/histograms.json}"
threshold="${2:-0.01}"

echo "INFO: Checking that error results in $results_json are below $threshold"

# First transform the histograms into a single object with aggregate count of error and completed samples.
summary_json=$(cat "$results_json" | jq '
    to_entries
    | {
        "completed_samples": ( .[] | select(.key == "completed") | .value.NUM_SAMPLES ),
        "errored_samples": ( [ .[] | select(.key != "completed" ) | .value.NUM_SAMPLES ] | add )
    }
    | .error_rate = (.errored_samples / .completed_samples)'
)

# Print it out for debugging.
echo "$summary_json" | jq .

# Check the value of the error rate.
if ! echo "$summary_json" | jq -e '(.error_rate < '$threshold')' >/dev/null; then
    echo "ERROR: error rate is too high"
    exit 1
fi
# else
echo 'OK!'
exit 0