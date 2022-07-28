# Benchbase Containers

This directory provides scripts for producing two different containers.

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

    When running the `benchbase` image, results are placed in `/benchbase/results`, which is expected to be mapped back into the host environment (e.g. `docker run -v /place/to/save/results:/benchbase/results benchbase`)

    See [`build-full-image.sh`](./build-full-image.sh) and [`run-full-image.sh`](./run-full-image.sh) for further details.

## Publishing Containers

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

## Prebuilt Containers

To use prebuilt containers, the following can be used:

```sh
docker pull benchbase.azurecr.io/benchbase-dev

# Provide a build environment for working with the local source code:
docker run -it --rm -v /src:/benchbase benchbase.azurecr.io/benchbase-dev
```

```sh
docker pull benchbase.azurecr.io/benchbase

# Run benchbase against a postgres instance and store the results in /results:
docker run --rm --env BENCHBASE_PROFILE='postgres' -v /results:/benchbase/results benchbase.azurecr.io/benchbase
```