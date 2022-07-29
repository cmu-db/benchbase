#!/bin/bash

set -eu

scriptdir=$(dirname "$(readlink -f "$0")")
rootdir=$(readlink -f "$scriptdir/../../")

cd "$scriptdir"
. ./common-env.sh


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

    # Make (hard-linked) copies of the build results that we can put into the image.
    pushd "$scriptdir/fullimage/"
    rm -rf tmp/
    mkdir -p tmp/docker-build-stage/
    for profile in $profiles; do
        if ! [ -f "$rootdir/profiles/$profile/benchbase.jar" ]; then
            echo "ERROR: build for $profile appears to have failed." >&2
            exit 1
        fi
        cp -al "$rootdir/profiles/$profile" tmp/docker-build-stage/
    done
    tar -C tmp/docker-build-stage -cJvf docker-build-stage.tar.xz .

    # Adjust the image tags.
    local image_name='benchbase'
    if [ "$profiles" == "$default_profile" ]; then
        # This is a singleton image, mark it as such.
        image_name="benchbase-${default_profile}"
    fi
    local target_image_tag_args=$(echo "-t benchbase:latest ${image_tag_args:-}" | sed "s/benchbase:/$image_name:/g")

    # Build with just the staged files and entrypoint.sh as the context.
    set -x
    docker build --progress=plain \
        --build-arg="http_proxy=${http_proxy:-}" --build-arg="https_proxy=${https_proxy:-}" \
        --build-arg MAVEN_OPTS="-Dhttp.proxyHost=${http_proxy_host} -Dhttp.proxyPort=${http_proxy_port} -Dhttps.proxyHost=${https_proxy_host} -Dhttps.proxyPort=${https_proxy_port}" \
        --build-arg CONTAINERUSER_UID="$CONTAINERUSER_UID" --build-arg CONTAINERUSER_GID="$CONTAINERUSER_GID" \
        --build-arg DEFAULT_BENCHBASE_PROFILE=$profile
        -t $target_iamge_tag_args -f ./docker/benchbase/fullimage/Dockerfile "$scriptdir/fullimage/"
    set +x

    # Cleanup the temporary copies.
    rm -rf .docker-build-stage/
    rm -rf .docker-build-stage.tar.xz
    popd
}

# Create the combo image.
create_image "$BASEBENCH_PROFILES"
# Now create split images as well.
for profile in $BASEBENCH_PROFILES; do
    create_image $profile
done