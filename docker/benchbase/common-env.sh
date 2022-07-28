# Try to read some values from the environment or else set some defaults.
# The profiles to build:
export BENCHBASE_PROFILES="${BENCHBASE_PROFILES:-cockroachdb mariadb mysql postgres spanner phoenix sqlserver sqlite}"
# The profile to run:
export BENCHBASE_PROFILE="${BENCHBASE_PROFILE:-postgres}"
# Whether to clean the build before/after/both/never:
export CLEAN_BUILD="${CLEAN_BUILD:-true}"   # true, pre, post, false
# Whether to run the test target during build.
export SKIP_TESTS="${SKIP_TESTS:-false}"

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

git_rev=$(git rev-list -1 --abbrev-commit HEAD)
git_vers_tag=$(git tag -l --points-at HEAD | grep ^v | sort -V | tail -n1)

image_tag_args="-t $imagename:latest"
if [ -n "$CONTAINER_REGISTRY_NAME" ]; then
    image_tag_args+=" -t $CONTAINER_REGISTRY_NAME/$imagename:latest"
fi
if [ -n "$git_vers_tag" ]; then
    image_tag_args+=" -t $imagename:$git_vers_tag"
    if [ -n "$CONTAINER_REGISTRY_NAME" ]; then
        image_tag_args+=" -t $CONTAINER_REGISTRY_NAME/$imagename:latest"
    fi
fi
