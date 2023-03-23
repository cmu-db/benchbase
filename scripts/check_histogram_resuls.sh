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

for error_type in {aborted,rejected,unexpected}; do
    if cat "$results_json" | jq -e '(.'$error_type'.NUM_SAMPLES / .completed.NUM_SAMPLES) >= '$threshold >/dev/null; then
        echo "ERROR: $error_type errors in $results_json are too high"
        exit 1
    fi
done
echo 'OK!'
exit 0