# Benchbase Containers

This directory provides scripts for producing and running two different containers.

## Building Containers

1. `benchbase-dev` which can be used for *building* and *developing* `benchbase`.

    It contains a full JDK and is expected to be run with the local source code checkout mapped into it (e.g. `docker run -v $repo_root:/benchbase benchbase-dev`).

    The [`.devcontainer.json`](../../.devcontainer/devcontainer.json) config references this one and can be used with [Github Codespaces](https://github.com/features/codespaces) or [VS Code dev containers](https://code.visualstudio.com/docs/remote/containers).

    See [`build-dev-image.sh`](./build-dev-image.sh) and [`run-dev-image.sh`](./run-dev-image.sh) for further details.

2. `benchbase` which can be used for easily *running* `benchbase` in a container environment (e.g. via Kubernetes).

    It contains just a JRE and can be used for running prebuilt profiles of `benchbase`.

    The `benchbase-dev` image is used to produce the prebuilt profiles.

    By default all profiles are built, though this can be controlled by setting the `BENCHBASE_PROFILES` environment variable to a specific subset of profiles when building the image with the `build-full-image.sh` script.
    For instance:

    ```sh
    BENCHBASE_PROFILES='postgres mysql' ./build-full-image.sh
    ```

    The test suite can also be skipped during the build phase by setting `SKIP_TESTS='true'`.

    Additionally, an `benchbase-{profile_name}` image with just that profile in it will be created for each of the `BENCHBASE_PROFILES`.

    When running the `benchbase` image, results are placed in `/benchbase/results`, which is expected to be mapped back into the host environment (e.g. `docker run -v /place/to/save/results:/benchbase/results benchbase`)

    See [`build-full-image.sh`](./build-full-image.sh) and [`run-full-image.sh`](./run-full-image.sh) for further details.

### Publishing Containers

These scripts are used to publish the images to a container registry for easy consumption with `docker pull` as well.

If you would like to publish them to an alternative repository, you can setup the appropriate tags during the build by setting the `CONTAINER_REGISTRY_NAME` environment variable.
For instance:

```sh
export CONTAINER_REGISTRY_NAME='benchbasedev.azurecr.io'
./build-dev-image.sh
./build-full-image.sh

docker push $CONTAINER_REGISTRY_NAME/benchbase-dev
docker push $CONTAINER_REGISTRY_NAME/benchbase
```

## Running Containers

### With Local Builds

```sh
# This will build and run a container shell with maven and java preloaded and the current source checkout mapped into it:
./docker/benchbase/run-dev-image.sh
```


```sh
# This will build and run a container for running the postgres benchbase profile:
BENCHBASE_PROFILE='postgres' ./docker/benchbase/run-full-image.sh
```

### Prebuilt Containers

To use prebuilt containers, the following can be used:

```sh
docker pull benchbase.azurecr.io/benchbase-dev

# Provide a build environment for working with the local source code:
docker run -it --rm -v /path/to/src:/benchbase benchbase.azurecr.io/benchbase-dev
```

> Optional: also reuse the local `MAVEN_CONFIG_DIR` and it's repository download cache with the following argument:
>
> `-v "${MAVEN_CONFIG_DIR:-$HOME/.m2}:/home/containeruser/.m2"`

```sh
docker pull benchbase.azurecr.io/benchbase

# Run benchbase against a postgres instance and store the results in /results:
docker run --rm --env BENCHBASE_PROFILE='postgres' -v /results:/benchbase/results benchbase.azurecr.io/benchbase --help

# Or by referencing the standalone image for that profile:
docker run --rm -v /results:/benchbase/results benchbase.azurecr.io/benchbase-postgres --help
```