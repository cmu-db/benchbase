# Docker Use

Scripts in this directory can be used to quickly run a benchmark against a sample database.

For instance:

```bash
# Set which database to target.
export BENCHBASE_PROFILE='sqlserver'
# Set which profiles to build.
export BENCHBASE_PROFILES=$BENCHBASE_PROFILE
# Whether or not to rebuild the package/image.
export CLEAN_BUILD="false"
# When rebuilding, whether or not to run the unit tests.
export SKIP_TESTS="true"

# Set which benchmark to run.
benchmark='tpcc'

./docker/build-run-benchmark-with-docker.sh $benchmark
```

This will use the selected profile's `up.sh` script to start the database as a local container, and the [`run-full-image.sh`](./benchbase/run-full-image.sh) to optionally build benchbase and then run the benchmark against it.