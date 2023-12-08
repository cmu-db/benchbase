# Try to read some values from the environment or else set some defaults.
# The profiles to build:
export BENCHBASE_PROFILES="${BENCHBASE_PROFILES:-cockroachdb mariadb mysql oracle phoenix postgres spanner sqlite sqlserver}"
# The profile to run:
export BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
# Whether to clean the build before/after/both/never:
export CLEAN_BUILD="${CLEAN_BUILD:-true}"   # true, pre, post, false
# Whether to run the test target during build.
export SKIP_TESTS="${SKIP_TESTS:-false}"
# Whether to do format checks during build (mostly for CI).
export DO_FORMAT_CHECKS="${DO_FORMAT_CHECKS:-false}"

# Setting this allows us to easily tag and publish the image name in our CI pipelines or locally.
CONTAINER_REGISTRY_NAME="${CONTAINER_REGISTRY_NAME:-}"

http_proxy_host=''
http_proxy_port=''
https_proxy_host=''
https_proxy_port=''

if echo "${http_proxy:-}" | egrep -q 'http[s]?://[^:]+:[0-9]+[/]?$'; then
    http_proxy_host=$(echo "$http_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\1|')
    http_proxy_port=$(echo "$http_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\2|')
fi

if echo "${https_proxy:-}" | egrep -q 'http[s]?://[^:]+:[0-9]+[/]?$'; then
    https_proxy_host=$(echo "$https_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\1|')
    https_proxy_port=$(echo "$https_proxy" | sed -r -e 's|^http[s]?://([^:]+):([0-9]+)[/]?$|\2|')
fi

CONTAINERUSER_UID="${CONTAINERUSER_UID:-$UID}"
if [ "$CONTAINERUSER_UID" -eq 0 ] && [ -n "${SUDO_UID:-}" ]; then
    CONTAINERUSER_UID="$SUDO_UID"
fi
CONTAINERUSER_GID=${CONTAINERUSER_GID:-$(getent passwd "$CONTAINERUSER_UID" | cut -d: -f4)}
if [ -z "$CONTAINERUSER_GID" ]; then
    echo "WARNING: missing CONTAINERUSER_GID." >&2
fi

if [[ "$0" == *-full-image.sh ]]; then
    imagename='benchbase'
elif [[ "$0" == *-dev-image.sh ]]; then
    imagename='benchbase-dev'
else
    echo "ERROR: Unhandled calling script name: '$0'!" >&2
    exit 1
fi

# Determine the tags to apply for the images produced.

git_main_rev=$(git rev-parse --revs-only refs/heads/main)
git_rev=$(git rev-parse --revs-only HEAD)
git_vers_tag=$(git tag -l --points-at HEAD | grep ^v | sort -V | tail -n1)

# We could also include a short tag to follow semantic versioning, but it adds
# cleanup complexity, so we omit it for now.
git_rev_short=''
if [ "${WITH_GIT_SHORT_REV_TAG:-false}" == 'true' ]; then
    git_rev_short=$(git rev-parse --revs-only --short HEAD)
fi

# Local image gets a :latest tag, always, for local dev/run convenience.
image_tag_args="-t $imagename:latest"
if [ -n "$CONTAINER_REGISTRY_NAME" ]; then
    # Mark the latest main commit as the latest image tag.
    if [ "$git_rev" == "$git_main_rev" ]; then
        image_tag_args+=" -t $CONTAINER_REGISTRY_NAME/$imagename:latest"
    fi
fi
# If this commit was tagged, mark it as such.
if [ -n "$git_vers_tag" ]; then
    image_tag_args+=" -t $imagename:$git_vers_tag"
    if [ -n "$CONTAINER_REGISTRY_NAME" ]; then
        image_tag_args+=" -t $CONTAINER_REGISTRY_NAME/$imagename:$git_vers_tag"
    fi
fi

if [ -n "$git_rev_short" ]; then
    image_tag_args+=" -t $imagename:$git_rev_short"
    if [ -n "$CONTAINER_REGISTRY_NAME" ]; then
        image_tag_args+=" -t $CONTAINER_REGISTRY_NAME/$imagename:$git_rev_short"
    fi
fi

docker_build_args=''
if ! docker buildx version >/dev/null 2>&1; then
    echo 'NOTE: docker buildkit is unavailable.' >&2
    DOCKER_BUILDKIT=0
    docker_build_args=''
elif [ -z "${DOCKER_BUILDKIT:-}" ]; then
    # If not already set, default to buildkit.
    DOCKER_BUILDKIT=1
fi
if [ "$DOCKER_BUILDKIT" == 1 ]; then
    docker_build_args='--progress=plain'
fi
export DOCKER_BUILDKIT

if [ "${NO_CACHE:-false}" == 'true' ]; then
    docker_build_args+=' --pull --no-cache'
else
    upstream_image="benchbase.azurecr.io/$imagename:latest"
    docker_build_args+=" --cache-from=$upstream_image"
fi