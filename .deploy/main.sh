#!/bin/bash
set -e
set -u
set -C
# Read the file passed in through STDIN to disk for OLTPBench to reference.
cat /dev/stdin > config/docker_workload.xml
# Run the benchmarker using the remaining arguments.
./oltpbenchmark -c config/docker_workload.xml "$@"
