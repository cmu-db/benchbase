# Benchmark Implementations

Each directory has the following layout:

* `{Prefix}Benchmark.java` - This is the BenchmarkModule implementation that is responsible for setting up all the class paths for the benchmark.
* `{Prefix}Loader.java` - This is the Loader implementation that is responsible for populating the database.
* `{Prefix}Worker.java` - The Worker for this benchmark. It is provided a random TransactionType from the abstract Worker driver code and then invokes the proper procedure
* `procedures` - The implementations for all of the transaction types in the benchmark.

Benchmark specific `dialects` and `ddls` files are now stored in the `src/main/resources` directory.
