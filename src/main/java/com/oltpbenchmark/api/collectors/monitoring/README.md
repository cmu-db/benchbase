# Monitoring in BenchBase

Monitoring in BenchBase can be enabled using the
```text
 -im,--interval-monitor <arg>   Monitoring Interval in milliseconds
 -mt,--monitor-type <arg>       Type of Monitoring (throughput/advanced)
```
command line option when executing BenchBase, where the monitoring interval describes the sleeping period of the thread between recording monitoring information.
We currently support two types of monitoring:

1. Basic throughput monitoring to track the progress while executing a benchmark (`-mt=throughput`), and
2. monitoring of query and system properties via system tables for both SQLServer and Postgres (`-mt=advanced`).
    Support for other engines can also be added.

The former is the default setting unless the monitoring type is explicitly set to advanced which will trigger system monitoring if the database type is supported.

Throughput monitoring logs updated throughput values directly to the system output, while advanced monitoring creates csv files recording their findings in folder `results/monitor/`.
Advanced monitoring collects data for a variety of events, such as one-off information about a query (for example query plans, query text, etc.), repeated information about a query (elapsed time per query execution, worker time, execution count etc.), and repeated system information (cache hits, number of transactions etc.).
Which events are collected depends on the database system and is customized in corresponding drivers.
The code for the drivers can be found in package [`src.main.java.com.oltpbenchmark.api.collectors.monitoring`](./../monitoring/).

For advanced monitoring to function with SQLServer, the user needs to have access to the system tables, for Postgres, `pg_stat_statements` needs to be enabled.
Queries will fail gracefully, i.e., without interrupting the benchmark execution but instead logging an error.
Note that in either database system, frequent (additional) queries against the DBMS may distort the benchmarking results.
That is, a high additional query load via frequent pulling of data from the DBMS will incur system load and can potentially block the execution of the actual benchmark queries.