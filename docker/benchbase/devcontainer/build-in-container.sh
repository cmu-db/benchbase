#!/bin/bash
#
# A simple script for building one or more profiles (in parallel) inside the container.

set -eu -o pipefail

BENCHBASE_PROFILES="${BENCHBASE_PROFILES:-cockroachdb mariadb mysql postgres spanner phoenix sqlserver sqlite}"
CLEAN_BUILD="${CLEAN_BUILD:-true}"    # true, false, pre, post
SKIP_TESTS="${SKIP_TESTS:-false}"

cd /benchbase

function build_profile() {
    local profile="$1"
    # Build in a separate directory so we can do it in parallel.
    rm -rf profiles/$profile/
    mkdir -p target/$profile
    mvn -T 2C -B --file pom.xml package -P $profile -D skipTests -D buildDirectory=target/$profile
    # Extract the resultant package.
    mkdir -p profiles/$profile
    tar -C profiles/$profile/ --strip-components=1 -xvzf target/$profile/benchbase-$profile.tgz
    # Try to save some space by linking to a shared data directory.
    rm -rf profiles/$profile/data/ && ln -s ../../data profiles/$profile/data
}

function test_profile_build() {
    local profile="$1"
    if [ "$(readlink -f profiles/$profile/data)" != "/benchbase/data" ]; then
        echo "ERROR: profiles/$profile/data is not /benchbase/data." >&2
        false
    fi
}

function clean_profile_build() {
    local profile="$1"
    if [ -d target/$profile ]; then
        mvn -B --file pom.xml -D buildDirectory=target/$profile clean
        rm -rf target/$profile
    fi
}

if [ "$CLEAN_BUILD" == 'true' ] || [ "$CLEAN_BUILD" == 'pre' ]; then
    for profile in ${BENCHBASE_PROFILES}; do
        clean_profile_build "$profile" &
    done
fi
wait

if [ "$SKIP_TESTS" == 'true' ]; then
    TEST_TARGET=''
elif [ "$SKIP_TESTS" == 'false' ]; then
    TEST_TARGET='test'
else
    echo "WARNING: Unhandled SKIP_TESTS mode: '$SKIP_TESTS'" >&2
fi

# Make sure that we've built the base stuff first before we start.
mvn -T 2C -B --file pom.xml process-resources compile ${TEST_TARGET:-}

for profile in ${BENCHBASE_PROFILES}; do
    build_profile "$profile" &
done
wait

test -d data
for profile in ${BENCHBASE_PROFILES}; do
    test_profile_build "$profile" || exit 1
done

if [ "$CLEAN_BUILD" == 'true' ] || [ "$CLEAN_BUILD" == 'post' ]; then
    for profile in ${BENCHBASE_PROFILES}; do
        clean_profile_build "$profile" &
    done
    wait
fi
