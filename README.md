# OLTP-Bench II

![Java CI with Maven](https://github.com/timveil-cockroach/oltpbench/workflows/Java%20CI%20with%20Maven/badge.svg?branch=maven)

Forked from https://github.com/oltpbenchmark/oltpbench with a focus on cleanup and modernization.  Given the volume and scope of these changes, I have elected not to submit pull requests to the original project as it is unlikely they would or could be accepted.  Please see [Modifications to Original](#modifications-to-original) for changes in this fork.

See also: [OLTP-Bench: An extensible testbed for benchmarking relational databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf) D. E. Difallah, A. Pavlo, C. Curino, and P. Cudre-Mauroux. In VLDB 2014.

## Benchmarks

### From Original Paper
* [AuctionMark](https://github.com/timveil-cockroach/oltpbench/wiki/AuctionMark)
* [CH-benCHmark](https://github.com/timveil-cockroach/oltpbench/wiki/CH-benCHmark)
* [Epinions.com](https://github.com/timveil-cockroach/oltpbench/wiki/epinions)
* [Resource Stresser](https://github.com/timveil-cockroach/oltpbench/wiki/Resource-Stresser)
* [SEATS](https://github.com/timveil-cockroach/oltpbench/wiki/Seats)
* [SIBench](https://github.com/timveil-cockroach/oltpbench/wiki/SIBench)
* [SmallBank](https://github.com/timveil-cockroach/oltpbench/wiki/SmallBank)
* [TATP](https://github.com/timveil-cockroach/oltpbench/wiki/TATP)
* [TPC-C](https://github.com/timveil-cockroach/oltpbench/wiki/TPC-C)
* [Twitter](https://github.com/timveil-cockroach/oltpbench/wiki/Twitter)
* [Voter](https://github.com/timveil-cockroach/oltpbench/wiki/Voter)
* [Wikipedia](https://github.com/timveil-cockroach/oltpbench/wiki/Wikipedia)
* [YCSB](https://github.com/timveil-cockroach/oltpbench/wiki/YCSB)

### Added Later
* [TPC-H](https://github.com/timveil-cockroach/oltpbench/wiki/TPC-H)
* TPC-DS - no configuration
* hyadapt - no configuration
* [NoOp](https://github.com/timveil-cockroach/oltpbench/wiki/NoOp)

### Removed
* JPAB - this project appears abandoned and hasn't seen an update since 2012.  I don't have a great deal of faith in a Hibernate benchmark that hasn't kept pace with Hibernate.
* [LinkBench](http://people.cs.uchicago.edu/~tga/pubs/sigmod-linkbench-2013.pdf) - no implementation

## How to Build
Run the following command to build the distribution:
```bash
./mvnw clean package
```

The following files will be placed in the `./target` folder, `oltpbench2-x.y.z.tgz` and `oltpbench2-x.y.z.zip`.  Pick your poison.

The resulting `.zip` or `.tgz` file will have the following contents: 

```text
├── CONTRIBUTORS.md
├── LICENSE
├── README.md
├── config
│   ├── cockroachdb
│   │   ├── sample_auctionmark_config.xml
│   │   ├── sample_chbenchmark_config.xml
│   │   ├── sample_epinions_config.xml
│   │   ├── sample_noop_config.xml
│   │   ├── sample_resourcestresser_config.xml
│   │   ├── sample_seats_config.xml
│   │   ├── sample_sibench_config.xml
│   │   ├── sample_smallbank_config.xml
│   │   ├── sample_tatp_config.xml
│   │   ├── sample_tpcc_config.xml
│   │   ├── sample_tpcds_config.xml
│   │   ├── sample_tpch_config.xml
│   │   ├── sample_twitter_config.xml
│   │   ├── sample_voter_config.xml
│   │   ├── sample_wikipedia_config.xml
│   │   └── sample_ycsb_config.xml
│   ├── plugin.xml
│   └── postgres
│       └── ...
├── data
│   ├── tpch
│   │   ├── customer.tbl
│   │   ├── lineitem.tbl
│   │   ├── nation.tbl
│   │   ├── orders.tbl
│   │   ├── part.tbl
│   │   ├── partsupp.tbl
│   │   ├── region.tbl
│   │   └── supplier.tbl
│   └── twitter
│       ├── twitter_tweetids.txt
│       └── twitter_user_ids.txt
├── lib
│   └── ...
└── oltpbench2.jar
```

## How to Run
Once you build and unpack the distribution, you can run `oltpbench2` just like any other executable jar.

To bring up help contents:
```bash
java -jar oltpbench2.jar -h
```

To execute the `tpcc` benchmark:
```bash
java -jar oltpbench2.jar -b tpcc -c config/cockroachdb/sample_tpcc_config.xml --create=true --load=true --execute=true -s 5
```

For composite benchmarks like `chbenchmark`, which require multiple schemas to be created and loaded, you can provide a comma separated list: `
```bash
java -jar oltpbench2.jar -b tpcc,chbenchmark -c config/cockroachdb/sample_chbenchmark_config.xml --create=true --load=true --execute=true -s 5
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

## How to see Postgres Driver logging
To enable logging for the PostgreSQL JDBC driver, add the following JVM property when starting...
```
-Djava.util.logging.config.file=src/main/resources/logging.properties
```
To modify the logging level you can update `logging.properties`

## How to Release
```
./mvnw -B release:prepare
./mvnw -B release:perform
```

## How to Add Support for a New Database
coming soon


## Known Issues

### Cockroach DB

My first priority is simply getting this code working against CockroachDB.  No work has been put in to optimizing either the Database or the configurations for performance.

| Benchmark | Config | Load | Run | Notes |
| -------------| ------------- | ------------- | ------------- | ------------- |
| `auctionmark` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #4~~](https://github.com/timveil-cockroach/oltpbench/issues/4), [~~issue #40~~](https://github.com/timveil-cockroach/oltpbench/issues/40) |
| `chbenchmark` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #5~~](https://github.com/timveil-cockroach/oltpbench/issues/5), [~~issue #6~~](https://github.com/timveil-cockroach/oltpbench/issues/6) |
| `epinions` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #7~~](https://github.com/timveil-cockroach/oltpbench/issues/7) |
| `hyadapt` | :x: |  |  | no config - [issue #8](https://github.com/timveil-cockroach/oltpbench/issues/8) |
| `noop` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `resourcestresser` | :heavy_check_mark: | :heavy_check_mark: | :wavy_dash: | [issue #41](https://github.com/timveil-cockroach/oltpbench/issues/41) |
| `seats` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #10~~](https://github.com/timveil-cockroach/oltpbench/issues/10) |
| `sibench` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `smallbank` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tatp` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcc` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcds` | :x: |  |  | no config - [issue #11](https://github.com/timveil-cockroach/oltpbench/issues/11) |
| `tpch` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #12~~](https://github.com/timveil-cockroach/oltpbench/issues/12) |
| `twitter` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #13~~](https://github.com/timveil-cockroach/oltpbench/issues/13) |
| `voter` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `wikipedia` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `ycsb` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |

## Modifications to Original
This fork contains a number of significant **structural** modifications to the original project.  This was done in an effort to clean up and modernize the code base, not to alter the spirit or function of the project.  To this end, I did my best to leave the actual benchmark code **functionally** unchanged while improving where possible.  My modifications are summarized below:

* Moved from Ant to Maven
    * Reorganized project to fit Maven structure
    * Removed static `lib` directory and dependencies
    * Updated required dependencies and removed unused or unwanted dependencies
    * Moved all non `.java` files to standard Maven `resources` directory
    * Shipped with [Maven Wrapper](https://github.com/takari/maven-wrapper)
* Improved packaging and versioning
    * Moved to Calendar Versioning (https://calver.org/)
    * Project is now distributed as a `.tgz` or `.zip` with an executable `.jar`
    * All code updated to read `resources` from inside `.jar` instead of directory
* Built with and for Java ~~1.8~~ 11
* Moved from direct dependence on Log4J to SLF4J
* Reorganized and renamed many files (mostly `resources`) for clarity and consistency
* Applied countless fixes based on "Static Analysis"
    * JDK migrations (boxing, un-boxing, etc.)
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging
* Reformatted code and cleaned up imports based on my preferences and using IntelliJ
* Removed all calls to `assert`... `assert` is disabled by default thus providing little real value while making the code incredibly hard to read and unnecessarily verbose
* Removed considerable amount of dead code, configurations, detritus and other nasty accumulations that didn't appear directly related to executing benchmarks
    * Removed IDE specific settings
    * Removed references to personal setups or cloud instances
    * Removed directories such as `run`, `tools`, `nbproject`, `matlab`, `traces`
    * Removed all references to `JPAB` benchmark, this project has not been updated since 2012
* Removed calls to `commit()` during `Loader` operations
* Refactored `Worker` and `Loader` usage of `Connection` objects and cleaned up transaction handling
* Introduced `HikariCP` as connection pool and `DataSource` instead of building connections from `DriverManager` as needed (default `poolsize` is 25)
* Introduced [Dependabot](https://dependabot.com/) to keep Maven dependencies up to date
* Removed `upload`, `output`, `output-raw`, `output-samples` `timestamp`, `tracescript`, `histograms`, `run-script` and `verbose` options.  Those related to "output" were simply enabled by default.
* Refactored `Catalog` to be populated directly from the configured Benchmark database instead of proxied via `HSQLDB`.  This eliminates the dependency on this project.
