#!/bin/bash
#
# A simple script for building one or more profiles (in parallel) inside the container.

# TODO: Convert this to a multi-stage build for better caching.

# Make sure any failure halts the rest of the operation.
set -eu -o pipefail

BENCHBASE_PROFILES="${BENCHBASE_PROFILES:-cockroachdb mariadb mysql oracle phoenix postgres spanner sqlite sqlserver}"
CLEAN_BUILD="${CLEAN_BUILD:-true}"    # true, false, pre, post
SKIP_TESTS="${SKIP_TESTS:-false}"
DO_FORMAT_CHECKS="${DO_FORMAT_CHECKS:-false}"

cd /benchbase
mkdir -p results
mkdir -p profiles

SKIP_TEST_ARGS='-D skipTests -D maven.test.skip -D maven.javadoc.skip=true'
EXTRA_MAVEN_ARGS="${EXTRA_MAVEN_ARGS:-}"

if [ "$CLEAN_BUILD" == false ]; then
    # In tight dev build loops we want to avoid regenerating classes when only
    # the git properties have changed.
    EXTRA_MAVEN_ARGS+=" -D maven.gitcommitid.skip=true"
fi

function build_profile() {
    local profile="$1"
    # Build in a separate directory so we can do it in parallel.
    rm -rf profiles/$profile/
    mkdir -p target/$profile
    # Build the profile without tests (we did that separately).
    mvn -T 2C -B --file pom.xml package -D descriptors=src/main/assembly/dir.xml -P $profile \
        -Dfmt.skip $SKIP_TEST_ARGS $EXTRA_MAVEN_ARGS -D buildDirectory=target/$profile
    # Copy the resultant output to the profiles directory.
    cp -rlv target/$profile/benchbase-$profile/benchbase-$profile profiles/$profile
    # Later the container entrypoint will move into this directory to run it, so
    # save all of the results back to the common volume mapping location.
    ln -s ../../results profiles/$profile/results
}

function test_profile_build() {
    local profile="$1"
    if ! [ -f "profiles/$profile/benchbase.jar" ]; then
        echo "ERROR: build/packaging failed: profiles/$profile/benchbase.jar does not exist!" >&2
        false
    fi
    if [ "$(readlink -f profiles/$profile/results)" != "/benchbase/results" ]; then
        echo "ERROR: profiles/$profile/results is not /benchbase/results." >&2
        false
    fi
}

function deduplicate_profile_files() {
    echo "INFO: Deduplicating files in /benchbase/profiles/"
    find /benchbase/profiles/ -type f -print0 | xargs -0 md5sum > /tmp/md5sums
    dup_md5sums=$(cat /tmp/md5sums | awk '{ print $1 }' | sort | uniq -c | awk '( $1 > 1 ) { print $2 }')
    for dup_md5sum in $dup_md5sums; do
        dup_files="$(grep "^$dup_md5sum" /tmp/md5sums | sed -r -e "s/^$dup_md5sum\s+//")"
        target_file="$(echo -e "$dup_files" | head -n1)"
        echo -e "$dup_files" | tail -n +2 | while read dup_file; do
            if [ "$dup_file" == "$target_file" ]; then
                continue
            fi
            if cmp "$dup_file" "$target_file"; then
                #echo "DEBUG: Hardlinking duplicate file $dup_file to $target_file." >&2
                ln -f "$target_file" "$dup_file"
            fi
        done
    done
    rm -f /tmp/md5sums
}

function clean_profile_build() {
    local profile="$1"
    echo "INFO: Cleaning profile $profile"
    if [ -d target/$profile ]; then
        mvn -B --file pom.xml $SKIP_TEST_ARGS $EXTRA_MAVEN_ARGS -D buildDirectory=target/$profile clean
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

# Pre format checks are mostly for CI.
# Else, things are reformatted during compile.
if [ "${DO_FORMAT_CHECKS:-}" == 'true' ]; then
    mvn -B --file pom.xml fmt:check
fi

# Fetch resources serially to work around mvn races with downloading the same
# file in multiple processes (mvn uses *.part instead of use tmpfile naming).
for profile in ${BENCHBASE_PROFILES}; do
    mvn -T2C -B --file pom.xml -Dfmt.skip -D buildDirectory=target/$profile $EXTRA_MAVEN_ARGS process-resources dependency:copy-dependencies
done

# Make sure that we've built the base stuff (and test) before we build individual profiles.
mvn -T 2C -B --file pom.xml $SKIP_TEST_ARGS $EXTRA_MAVEN_ARGS compile # ${TEST_TARGET:-}
if [ -n "${TEST_TARGET:-}" ]; then
    # FIXME: Run tests without parallelism to work around some buggy behavior.
    mvn -B --file pom.xml $EXTRA_MAVEN_ARGS $TEST_TARGET
fi

for profile in ${BENCHBASE_PROFILES}; do
    build_profile "$profile" &
done
wait

FIRST_BENCHBASE_PROFILE=$(echo "$BENCHBASE_PROFILES" | awk '{ print $1 }')
if [ "$FIRST_BENCHBASE_PROFILE" == "$BENCHBASE_PROFILES" ]; then
    echo "INFO: Single profile build: $FIRST_BENCHBASE_PROFILE. Skipping file dedup."
else
    echo "INFO: deduplicating files in combo build."
    deduplicate_profile_files
fi

for profile in ${BENCHBASE_PROFILES}; do
    test_profile_build "$profile" || exit 1
done

if [ "$CLEAN_BUILD" == 'true' ] || [ "$CLEAN_BUILD" == 'post' ]; then
    for profile in ${BENCHBASE_PROFILES}; do
        clean_profile_build "$profile" &
    done
    wait
fi
