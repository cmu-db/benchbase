# BenchBase

[![BenchBase (Java with Maven)](https://github.com/cmu-db/benchbase/actions/workflows/maven.yml/badge.svg?branch=main)](https://github.com/cmu-db/benchbase/actions/workflows/maven.yml)

BenchBase (formerly [OLTPBench](https://github.com/oltpbenchmark/oltpbench/)) is a Multi-DBMS SQL Benchmarking Framework via JDBC.

**Table of Contents**

- [Quickstart](#quickstart)
- [Description](#description)
- [Usage Guide](#usage-guide)
- [Contributing](#contributing)
- [Known Issues](#known-issues)
- [Credits](#credits)
- [Citing This Repository](#citing-this-repository)

---

## Quickstart

To clone and build BenchBase,

```bash
git clone --depth 1 https://github.com/cmu-db/benchbase.git
cd oltpbench
./mvnw clean package
```

This produces artifacts in the `target` folder, which can be extracted,

```bash
cd target
tar xvzf benchbase-x-y-z.tgz # Change x-y-z appropriately.
cd benchbase-x-y-z           # Change x-y-z appropriately.
```

Inside this folder, you can run BenchBase. For example, to execute the `tpcc` benchmark,

```bash
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

A full list of options can be displayed,

```bash
java -jar benchbase.jar -h
```

---

## Description

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of
PhDs/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. We hope this
will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion. 

BenchBase is a multi-threaded load generator. The framework is designed to be able to produce variable rate,
variable mixture load against any JDBC-enabled relational database. The framework also provides data collection
features, e.g., per-transaction-type latency and throughput logs.

The BenchBase framework has the following benchmarks:

(TODO(WAN): Get permission to migrate/copy Tim's wiki for benchmark descriptions)

* [AuctionMark](https://github.com/timveil-cockroach/oltpbench/wiki/AuctionMark)
* [CH-benCHmark](https://github.com/timveil-cockroach/oltpbench/wiki/CH-benCHmark)
* [Epinions.com](https://github.com/timveil-cockroach/oltpbench/wiki/epinions)
* hyadapt -- pending configuration files
* [NoOp](https://github.com/timveil-cockroach/oltpbench/wiki/NoOp)
* [Resource Stresser](https://github.com/timveil-cockroach/oltpbench/wiki/Resource-Stresser)
* [SEATS](https://github.com/timveil-cockroach/oltpbench/wiki/Seats)
* [SIBench](https://github.com/timveil-cockroach/oltpbench/wiki/SIBench)
* [SmallBank](https://github.com/timveil-cockroach/oltpbench/wiki/SmallBank)
* [TATP](https://github.com/timveil-cockroach/oltpbench/wiki/TATP)
* [TPC-C](https://github.com/timveil-cockroach/oltpbench/wiki/TPC-C)
* [TPC-H](https://github.com/timveil-cockroach/oltpbench/wiki/TPC-H)
* TPC-DS -- pending configuration files
* [Twitter](https://github.com/timveil-cockroach/oltpbench/wiki/Twitter)
* [Voter](https://github.com/timveil-cockroach/oltpbench/wiki/Voter)
* [Wikipedia](https://github.com/timveil-cockroach/oltpbench/wiki/Wikipedia)
* [YCSB](https://github.com/timveil-cockroach/oltpbench/wiki/YCSB)

This framework is design to allow for easy extension. We provide stub code that a contributor can use to include a new
benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc.)

---

## Usage Guide

### How to Build
Run the following command to build the distribution:
```bash
./mvnw clean package
```

The following files will be placed in the `./target` folder:

* `benchbase-x.y.z.tgz`
* `benchbase-x.y.z.zip`

### How to Run
Once you build and unpack the distribution, you can run `benchbase` just like any other executable jar.  The following examples assume you are running from the root of the expanded `.zip` or `.tgz` distribution.  If you attempt to run `benchbase` outside of the distribution structure you may encounter a variety of errors including `java.lang.NoClassDefFoundError`.

To bring up help contents:
```bash
java -jar benchbase.jar -h
```

To execute the `tpcc` benchmark:
```bash
java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
```

For composite benchmarks like `chbenchmark`, which require multiple schemas to be created and loaded, you can provide a comma separated list: `
```bash
java -jar benchbase.jar -b tpcc,chbenchmark -c config/postgres/sample_chbenchmark_config.xml --create=true --load=true --execute=true
```

The following options are provided:

```text
usage: oltpbenchmark
 -b,--bench <arg>               [required] Benchmark class. Currently
                                supported: [tpcc, tpch, tatp, wikipedia,
                                resourcestresser, twitter, epinions, ycsb,
                                seats, auctionmark, chbenchmark, voter,
                                sibench, noop, smallbank, hyadapt]
 -c,--config <arg>              [required] Workload configuration file
    --clear <arg>               Clear all records in the database for this
                                benchmark
    --create <arg>              Initialize the database for this benchmark
 -d,--directory <arg>           Base directory for the result files,
                                default is current directory
    --dialects-export <arg>     Export benchmark SQL to a dialects file
    --execute <arg>             Execute the benchmark workload
 -h,--help                      Print this help
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
    --load <arg>                Load data using the benchmark's data
                                loader
 -s,--sample <arg>              Sampling window
```

### How to Enable Logging
To enable logging, e.g., for the PostgreSQL JDBC driver, add the following JVM property when starting...
```
-Djava.util.logging.config.file=src/main/resources/logging.properties
```
To modify the logging level you can update [`logging.properties`](https://github.com/oltpbenchmark/oltpbench/blob/oltpbench_tim/src/main/resources/logging.properties) and/or [`log4j.properties`](https://github.com/oltpbenchmark/oltpbench/blob/oltpbench_tim/src/main/resources/log4j.properties).

### How to Release
```
./mvnw -B release:prepare
./mvnw -B release:perform
```

### How to Add Support for a New Database

TODO(WAN): sqlite PR

---

## Contributing

We welcome all contributions! Please open a pull request. Common contributions may include:

- Adding support for a new DBMS.
- Adding more tests of existing benchmarks.
- Fixing any bugs or known issues.

## Known Issues

Please use GitHub's issue tracker for all issues.

## Credits

BenchBase is the official modernized version of the original OLTPBench.

The original OLTPBench code was largely written by the authors of the original paper, [OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf), D. E. Difallah, A. Pavlo, C. Curino, and P. Cudré-Mauroux. In VLDB 2014. Please see the citation guide below.

A significant portion of the modernization was contributed by [Tim Veil @ Cockroach Labs](https://github.com/timveil-cockroach), including but not limited to:

* Built with and for Java 11.
* Migration from Ant to Maven.
    * Reorganized project to fit Maven structure.
    * Removed static `lib` directory and dependencies.
    * Updated required dependencies and removed unused or unwanted dependencies.
    * Moved all non `.java` files to standard Maven `resources` directory.
    * Shipped with [Maven Wrapper](https://github.com/takari/maven-wrapper).
* Improved packaging and versioning.
    * Moved to Calendar Versioning (https://calver.org/).
    * Project is now distributed as a `.tgz` or `.zip` with an executable `.jar`.
    * All code updated to read `resources` from inside `.jar` instead of directory.
* Moved from direct dependence on Log4J to SLF4J.
* Reorganized and renamed many files for clarity and consistency.
* Applied countless fixes based on "Static Analysis".
    * JDK migrations (boxing, un-boxing, etc.).
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances.
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging.
* Reformatted code and cleaned up imports.
* Removed all calls to `assert`.
* Removed various forms of dead code and stale configurations.
* Removed calls to `commit()` during `Loader` operations.
* Refactored `Worker` and `Loader` usage of `Connection` objects and cleaned up transaction handling.
* Introduced `HikariCP` as connection pool and `DataSource` instead of building connections from `DriverManager` as needed (default `poolsize` is 12).
* Introduced [Dependabot](https://dependabot.com/) to keep Maven dependencies up to date.
* Simplified output flags by removing most of them, generally leaving the reporting functionality enabled by default.
* Provided an alternate `Catalog` that can be populated directly from the configured Benchmark database. The old catalog was proxied through `HSQLDB` -- this remains an option for DBMSes that may have incomplete catalog support.

## Citing This Repository

If you use this repository in an academic paper, please cite this repository:

> D. E. Difallah, A. Pavlo, C. Curino, and P. Cudré-Mauroux, "OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases," PVLDB, vol. 7, iss. 4, pp. 277-288, 2013.

The BibTeX is provided below for convenience.

```bibtex
@article{DifallahPCC13,
  author = {Djellel Eddine Difallah and Andrew Pavlo and Carlo Curino and Philippe Cudr{\'e}-Mauroux},
  title = {OLTP-Bench: An Extensible Testbed for Benchmarking Relational Databases},
  journal = {PVLDB},
  volume = {7},
  number = {4},
  year = {2013},
  pages = {277--288},
  url = {http://www.vldb.org/pvldb/vol7/p277-difallah.pdf},
 }
 ```
