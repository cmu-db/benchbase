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
* Improved packaging and versioning
    * Moved to Calendar Versioning (https://calver.org/)
    * Project is now distributed as a `.tgz` with executable `.jar`
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
comming soon

## How to Run
comming soon

## How to Add Support for a New Database
comming soon


## Known Issues

### Cockroach DB

| Benchmark  | Create & Load | Run | Notes |
| ------------- | ------------- | ------------- | ------------- |
| `auctionmark` | :x: |  | [issue #4](https://github.com/timveil-cockroach/oltpbench/issues/4) |
| `chbenchmark` | :heavy_check_mark: | :x: | [issue #5](https://github.com/timveil-cockroach/oltpbench/issues/5), [issue #6](https://github.com/timveil-cockroach/oltpbench/issues/6)|
| `epinions` | :heavy_check_mark: | :x: | [issue #7](https://github.com/timveil-cockroach/oltpbench/issues/7) |
| `hyadapt` | :x: |  | [issue #8](https://github.com/timveil-cockroach/oltpbench/issues/8) |
| `linkbench` | :x: |  | [issue #9](https://github.com/timveil-cockroach/oltpbench/issues/9) |
| `noop` | :heavy_check_mark: | :heavy_check_mark: | |
| `resourcestresser` | :heavy_check_mark: | :heavy_check_mark: | |
| `seats` | :x: |  | [issue #10](https://github.com/timveil-cockroach/oltpbench/issues/10) |
| `sibench` | :heavy_check_mark: | :heavy_check_mark: | |
| `smallbank` | :heavy_check_mark: | :heavy_check_mark: | |
| `tatp` | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcc` | :heavy_check_mark: | :heavy_check_mark: | |
| `tpcds` | :x: |  | [issue #11](https://github.com/timveil-cockroach/oltpbench/issues/11) |
| `tpch` | :heavy_check_mark: | :x: | |
| `twitter` | :heavy_check_mark: | :heavy_check_mark: | |
| `voter` | :heavy_check_mark: | :heavy_check_mark: | |
| `wikipedia` | :heavy_check_mark: | :heavy_check_mark: | |
| `ycsb` | :heavy_check_mark: | :heavy_check_mark: | |