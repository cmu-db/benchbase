# OLTP-Bench: Part Deux

Forked from https://github.com/oltpbenchmark/oltpbench with a focus on cleanup and modernization.  Given the volume and scope of these changes, I have elected not to submit pull requests to the original project as it is unlikely they would or could be accepted.

See also: [OLTP-Bench: An extensible testbed for benchmarking relational databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf) D. E. Difallah, A. Pavlo, C. Curino, and P. Cudre-Mauroux. In VLDB 2014.

## Modifications to Original
This fork contains a number of significant **structural** modifications to the original project.  This was done in an effort to cleanup and modernize the code base, not to alter the spirit or function of the project.  To this end, I did my best to leave the actual benchmark code **functionally** unchanged while improving where possible.  My modifications are summarized below:

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
* Built with and for Java 1.8
* Moved from direct dependence on Log4J to SLF4J
* Reorganized and renamed many files (mostly `resources`) for clarity and consistency
* Applied countless fixes based on "Static Analysis"
    * JDK migrations (boxing, un-boxing, etc.)
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging
* Reformatted code and cleaned up imports based on my preferences and using IntelliJ
* Removed all calls to `assert`... `assert` is disabled by default thus providing little real value while making the code incredibly hard to read and unnecessarily verbose
* Removed considerable amount of dead code, configurations, detritus and other nasty accumulations that didn't appear directly related to excuting benchmarks
    * Removed IDE specific settings
    * Removed references to personal setups or cloud instances
    * Removed directories such as `run`, `tools`, `nbproject`, `matlab`, `traces`
    * Removed all references to `JPAB` benchmark, this project has not been updated since 2012
* Removed calls to `commit()` during `Loader` operations
* Refactored `Worker` and `Loader` useage of `Connection` objects and cleaned up transaction handling
* Introduced `HikariCP` as connection pool and `DataSource` instead of building connections from `DriverManager` as needed

## Benchmarks

### From Original Paper
* [AuctionMark](http://hstore.cs.brown.edu/projects/auctionmark/)
* [CH-benCHmark](http://www-db.in.tum.de/research/projects/CHbenCHmark/?lang=en), mixed workload based on `TPC-C` and `TPC-H`
* Epinions.com
* [LinkBench](http://people.cs.uchicago.edu/~tga/pubs/sigmod-linkbench-2013.pdf)
* Synthetic Resource Stresser 
* [SEATS](http://hstore.cs.brown.edu/projects/seats)
* [SIBench](http://sydney.edu.au/engineering/it/~fekete/teaching/serializableSI-Fekete.pdf)
* [SmallBank](http://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf)
* [TATP](http://tatpbenchmark.sourceforge.net/)
* [TPC-C](http://www.tpc.org/tpcc/)
* Twitter
* [Voter](https://github.com/VoltDB/voltdb/tree/master/examples/voter) (Japanese "American Idol")
* Wikipedia
* [YCSB](https://github.com/brianfrankcooper/YCSB)

### Added Later
* [TPC-H](http://www.tpc.org/tpch)
* [TPC-DS](http://www.tpc.org/tpcds)
* hyadapt
* NoOp

### Removed
* JPAB - this project appears abandoned and hasn't seen a an update since 2012.  I don't have a great deal of faith in a Hibernate benchmark that hasn't kept pace with Hibernate.

## How to Build
Run the following command to build the distribution:
```
./mvnw clean package
```

The following files will be placed in the `./target` folder, `oltpbench-2019.1-BETA.tgz` and `oltpbench-2019.1-BETA.zip`.  Pick your poison.

The resulting `.zip` or `.tgz` file will have the following contents: 

```
├── CONTRIBUTORS.md
├── LICENSE
├── README.md
├── config
│   ├── cockroachdb
│   │   ├── sample_auctionmark_config.xml
│   │   ├── sample_chbenchmark_config.xml
│   │   ├── sample_epinions_config.xml
│   │   ├── sample_linkbench_config.xml
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
│   ├── linkbench
│   │   └── LinkBenchDistribution.dat
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
└── oltpbench-2019.1-BETA.jar
```

## How to Run
Once you build and unpack the distribution, you can run `oltpbenchmark` just like any other executable jar.

To bring up help contents:
```
java -jar oltpbench-2019.1-BETA.jar -h
```

To execute the `tpcc` benchmark:
```
java -jar oltpbench-2019.1-BETA.jar -b tpch -c config/cockroachdb/sample_tpch_config.xml --create=true --load=true --execute=true -s 5
```


The following options are provided:

```
usage: oltpbenchmark
 -b,--bench <arg>               [required] Benchmark class. Currently
                                supported: [tpcc, tpch, tatp, wikipedia,
                                resourcestresser, twitter, epinions, ycsb,
                                seats, auctionmark, chbenchmark, voter,
                                linkbench, sibench, noop, smallbank,
                                hyadapt]
 -c,--config <arg>              [required] Workload configuration file
    --clear <arg>               Clear all records in the database for this
                                benchmark
    --create <arg>              Initialize the database for this benchmark
 -d,--directory <arg>           Base directory for the result files,
                                default is current directory
    --dialects-export <arg>     Export benchmark SQL to a dialects file
    --execute <arg>             Execute the benchmark workload
 -h,--help                      Print this help
    --histograms                Print txn histograms
 -im,--interval-monitor <arg>   Throughput Monitoring Interval in
                                milliseconds
    --load <arg>                Load data using the benchmark's data
                                loader
 -o,--output <arg>              Output file (default System.out)
    --output-raw <arg>          Output raw data
    --output-samples <arg>      Output sample data
    --runscript <arg>           Run an SQL script
 -s,--sample <arg>              Sampling window
 -ss                            Verbose Sampling per Transaction
 -t,--timestamp                 Each result file is prepended with a
                                timestamp for the beginning of the
                                experiment
 -ts,--tracescript <arg>        Script of transactions to execute
    --upload <arg>              Upload the result
 -v,--verbose                   Display Messages
```

## How to Add Support for a New Database
coming soon


## Known Issues

### Cockroach DB

My first priority is simply getting this code working against CockroachDB.  No work has been put in to optimizing either the Database or the configurations for performance.

| Benchmark | Config | Load | Run | Notes |
| -------------| ------------- | ------------- | ------------- | ------------- |
| `auctionmark` | :heavy_check_mark: | :x: | :heavy_minus_sign: | self-referencing insert constaint [issue #4](https://github.com/timveil-cockroach/oltpbench/issues/4) |
| `chbenchmark` | :heavy_check_mark: | :heavy_check_mark: | :wavy_dash: | fails in CRDB `v2.1.4`, [~~issue #5~~](https://github.com/timveil-cockroach/oltpbench/issues/5), [~~issue #6~~](https://github.com/timveil-cockroach/oltpbench/issues/6); works well in CRDB `v2.2.0-alpha` |
| `epinions` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #7~~](https://github.com/timveil-cockroach/oltpbench/issues/7) |
| `hyadapt` | :x: | :heavy_minus_sign: | :heavy_minus_sign: | no config, [issue #8](https://github.com/timveil-cockroach/oltpbench/issues/8) |
| `linkbench` | :x: | :heavy_minus_sign: | :heavy_minus_sign: | no implementation [issue #9](https://github.com/timveil-cockroach/oltpbench/issues/9) |
| `noop` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `resourcestresser` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `seats` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #10~~](https://github.com/timveil-cockroach/oltpbench/issues/10) |
| `sibench` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `smallbank` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tatp` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcc` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcds` | :x: | :heavy_minus_sign: | :heavy_minus_sign: | no config, [issue #11](https://github.com/timveil-cockroach/oltpbench/issues/11) |
| `tpch` | :heavy_check_mark: | :heavy_check_mark: | :wavy_dash: | fails on `Q9` in CRDB `v2.1.4`, [~~issue #12~~](https://github.com/timveil-cockroach/oltpbench/issues/12); works well in CRDB `v2.2.0-alpha` |
| `twitter` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | [~~issue #13~~](https://github.com/timveil-cockroach/oltpbench/issues/13) |
| `voter` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `wikipedia` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |
| `ycsb` | :heavy_check_mark: | :heavy_check_mark: | :heavy_check_mark: | |