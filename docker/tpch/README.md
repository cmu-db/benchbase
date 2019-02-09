# How to generate data for TPC-H

To generate data files download the latest TPC-H tools distribution from http://www.tpc.org/tpc_documents_current_versions/current_specifications.asp

Place the zip file in this directory and rename it to `tpc-h-tool.zip` then execute the following commands.

This will build a docker file that uses `tpc-h-tool.zip` to `make` `dbgen` where `VERSION` is the current version of the `tpc-h` benchmark.
```
docker build --no-cache --build-arg VERSION=2.17.3 -t tpch:latest .
```

Execute this command to run `dbgen` with the provided `SCALE`. This will place the resulting `.tbl` files in a subdirectory called `output`
```
docker run --rm -t -e SCALE=.1 -v ${PWD}/output:/opt/tpch-output tpch:latest
```