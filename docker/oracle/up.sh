#!/bin/bash
# Copyright (c) 2023,  Oracle and/or its affiliates.

set -eu
scriptdir=$(dirname "$(readlink -f "$0")")
cd "$scriptdir/"

docker-compose up -d
