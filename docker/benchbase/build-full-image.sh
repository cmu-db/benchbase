#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh


if [ "$CLEAN_BUILD" == 'true' ]; then
    grep '^FROM ' fullimage/Dockerfile \
        | sed -r -e 's/^FROM\s+//' -e 's/--platform=\S+\s+//' -e 's/\s+AS \S+\s*$/ /' \
        | while read base_image; do
            set -x
            docker pull $base_image &
            set +x
        done
        wait
fi


logs_child_pid=
container_id=
function trap_ctrlc() {
    docker stop -t 1 $container_id >/dev/null || true
    if [ -n "$logs_child_pid" ]; then
        kill $logs_child_pid 2>/dev/null || true
    fi
    exit 1
}

# Build the requested profiles using the dev image.
./build-dev-image.sh
# Use non-interactive mode so that the build doesn't prompt us to accept git ssh keys.
# But setup some Ctrl-C handlers as well.
container_id=$(INTERACTIVE='false' ./run-dev-image.sh /benchbase/docker/benchbase/devcontainer/build-in-container.sh)
trap trap_ctrlc SIGINT SIGTERM
echo "INFO: build-devcontainer-id: $container_id"
docker logs -f $container_id &
logs_child_pid=$!
rc=$(docker wait $container_id)
trap - SIGINT SIGTERM
if [ "$rc" != 0 ]; then
    echo "ERROR: Build in devcontainer failed." >&2
    exit $rc
fi


function create_image() {
    local profiles="$1"
    local default_profile=$(echo "$profiles" | awk '{ print $1 }')

    # Prepare the build context.

    # Make (hard-linked) copies of the build results that we can put into the image.
    pushd "$scriptdir/fullimage/"
    rm -rf tmp/
    mkdir -p tmp/config/
    mkdir -p tmp/profiles/
    cp -a "$rootdir/config/plugin.xml" tmp/config/
    for profile in $profiles; do
        if ! [ -f "$rootdir/profiles/$profile/benchbase.jar" ]; then
            echo "ERROR: build for $profile appears to have failed." >&2
            exit 1
        fi
        cp -al "$rootdir/profiles/$profile" tmp/profiles/

        # Consolidate the configs across profiles.
        cp -a "$rootdir/profiles/$profile/config/$profile" tmp/config/
        rm -rf tmp/profiles/$profile/config
        ln -s ../../config/$profile "tmp/profiles/$profile/config"
        ln -s . tmp/profiles/$profile/config/$profile
        ln -s ../plugin.xml tmp/config/$profile/
    done

    # Make a copy of the entrypoint script that changes the default profile to
    # execute for singleton images.
    cp -a entrypoint.sh tmp/entrypoint.sh
    sed -i "s/:-postgres/:-${default_profile}/" tmp/entrypoint.sh

    # Adjust the image tags.
    local image_name='benchbase'
    if [ "$profiles" == "$default_profile" ]; then
        # This is a singleton image, mark it as such.
        image_name="benchbase-${default_profile}"
    fi
    local target_image_tag_args=$(echo "-t benchbase:latest ${image_tag_args:-}" | sed "s/benchbase:/$image_name:/g")

    set -x
    docker build $docker_build_args \
        --build-arg BUILDKIT_INLINE_CACHE=1 \
        --build-arg="http_proxy=${http_proxy:-}" --build-arg="https_proxy=${https_proxy:-}" --build-arg="no_proxy=${no_proxy:-}" \
        --build-arg CONTAINERUSER_UID="$CONTAINERUSER_UID" --build-arg CONTAINERUSER_GID="$CONTAINERUSER_GID" \
        $target_image_tag_args -f "$scriptdir/fullimage/Dockerfile" "$scriptdir/fullimage/tmp/"
    set +x

    # Cleanup the temporary copies.
    rm -rf "$scriptdir/fullimage/tmp/"
    popd
}

# Create the combo image.
if [ $(echo "$BENCHBASE_PROFILES" | wc -w) -gt 1 ]; then
    create_image "$BENCHBASE_PROFILES"
fi
# Now create split images as well.
for profile in $BENCHBASE_PROFILES; do
    create_image $profile
done
