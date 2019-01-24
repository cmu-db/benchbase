# Benchmark Implementations

Each of these directories contains all of the code for a particular benchmark.
Each directory has the following layout:

* `{Prefix}Benchmark.java` - This is the BenchmarkModule implementation that is responsible for setting up all the class paths for the benchmark.
* `{Prefix}Loader.java` - This is the Loader implementation that is responsible for populating the database.
* `{Prefix}Worker.java` - The Worker for this benchmark. It is provided a random TransactionType from the abstract Worker driver code and then invokes the proper procedure
* `ddls` - The location of the DBMS-specific DDL files. The filename has to match the supported systems defined in `com.oltpbenchmark.types.DatabaseType`.
* `dialects` - The location of the DBMS-specific Dialect xml files. The filename has to match the supported systems defined in `com.oltpbenchmark.types.DatabaseType`.
* `procedures` - The implementations for all of the transaction types in the benchmark.
