# Templated Benchmark Unit Tests

Note: The templated benchmark does not currently support data loading.

The current sample config files are built off using the TPC-C data which is expected to be loaded first.

Hence, in this directory we omit the usual `TestTemplatedBenchmark.java` and `TestTemplatedLoader.java` files.

To make the unit tests work, we reuse the TPC-C data loader as a part of the `setUp` override.