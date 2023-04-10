# Monitoring in BenchBase

Monitoring in BenchBase can be enabled using the
```text
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
```
command line option when executing BenchBase, where the monitoring interval describes the sleeping period of the thread between recording monitoring information.
We currently support two types of monitoring: 1) Basic throughput monitoring and 2) Monitoring of query and system statistics via system tables for both SQLServer and Postgres.
The latter are automatically enabled if the DBMS that BenchBase is executed against is either SQLServer or Postgres, otherwise, monitoring will default to basic throughput monitoring.

The output of the monitoring queries are in logs in .proto format where each element corresponds to a query resp. a monitoring event.
The proto definitions can be found at src/main/java/com/oltpbenchmark/api/collectors/monitoring/proto/monitor.proto.
Query information (such as the query plan) is logged once per executed query using the query cache that the DBMS provide to avoid unnecessary work of the DBMS.
This information is captured in the QueryEvent proto.
Performance information (such as CPU utilization, spills, or lock information) captures the system state at a specific moment in time and is logged as a PerfEvent proto.

For monitoring to work correctly in SQLServer, the user needs to have access to the system tables.
For monitoring to work correctly in Postgres, pg_stat_statements needs to be enabled.
Note that in either case, frequent (additional) queries against the DBMS may distort the benchmarking results.
That is, a high additional query load via frequent pulling of data from the DBMS will incur system load and can potentially block the execution of the actual benchmark queries.

The output of the monitor are logged to the same results folder as the other BenchBase telemetry, in the subfolder `monitor`.