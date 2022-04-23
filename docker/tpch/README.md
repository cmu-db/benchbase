# How to generate data for TPC-H

To generate data files download the latest TPC-H tools distribution from https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp

Place the zip file in this directory and rename it to `tpc-h-tool.zip` then execute the following commands.

This will build a docker file that uses `tpc-h-tool.zip` to `make` `dbgen` where `VERSION` is the current version of the `tpc-h` benchmark.
```
docker build --no-cache --build-arg VERSION=2.18.0_rc2 -t tpch:latest .
```

Execute this command to run `dbgen` with the provided `SCALE`. This will place the resulting `.tbl` files in a subdirectory called `output`
```
docker run --rm -t -e SCALE=.1 -v ${PWD}/output:/opt/tpch-output tpch:latest
```

The resulting `.tbl` files can then be placed in `./data/tpch` to be used during benchmark execution.  Some small starter files are already present in that directory.
