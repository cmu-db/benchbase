# OLTP-Bench: Part Deux

Forked from https://github.com/oltpbenchmark/oltpbench with a focus on cleanup and modernization.  Additional context can be found in the original academic publication from 2013: [OLTP-Bench: An extensible testbed for benchmarking relational databases](http://www.cs.cmu.edu/~pavlo/static/papers/oltpbench.pdf).

Given the volume and scope of these changes, I have elected not to submit pull requests to the original project as it is unlikely they would or could be accepted.


## Modifications from Original
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
* Reformatted code and cleaned up imports based on my preferences and using IntelliJ
* Removed all calls to `assert`... `assert` is disabled by default thus providing little real value while making the code incredibly hard to read and unnecessarily verbose
* Removed considerable amount of dead code, configurations, detritus and other nasty accumulations
    * Removed IDE specific settings
    * Removed references to personal setups or cloud instances
    * Removed directories such as `run`, `tools`, `nbproject`, `matlab`, `traces`
    * Removed all references to `JPAB` benchmark, this project has not been updated since 2012
* Removed calls to `commit()` during `Loader` operations


## Known Issues

* `tpch` - references files and directory that don't exist.  not clear what they should be.  see https://relational.fit.cvut.cz/dataset/TPCH
* `tpcds` - doesnt have a sample config.  will probably need data like tpc-h  see https://relational.fit.cvut.cz/dataset/TPCDS
* `seats` - having difficult loading `seats`
* `hyadapt` - no sample config 
* `linkbench` - loader needs to be fixed, wrong tables referenced in loader
* `chbenchmark` - needs more love
* `auctionmark` - not yet working; data loading issue; lots of resource leaks

## Supported Benchmarks

* [AuctionMark](http://hstore.cs.brown.edu/projects/auctionmark/)
* [CH-benCHmark](http://www-db.in.tum.de/research/projects/CHbenCHmark/?lang=en), mixed workload based on `chbenchmark` and `tpcc`
* Epinions.com
* hyadapt
* [LinkBench](http://people.cs.uchicago.edu/~tga/pubs/sigmod-linkbench-2013.pdf)
* NoOp
* Synthetic Resource Stresser 
* SEATS
* [SIBench](http://sydney.edu.au/engineering/it/~fekete/teaching/serializableSI-Fekete.pdf) (Snapshot Isolation)
* [SmallBank](http://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf)
* [TATP](http://tatpbenchmark.sourceforge.net/)
* [TPC-C](http://www.tpc.org/tpcc/)
* TPC-DS
* TPC-H
* Twitter
* [Voter](https://github.com/VoltDB/voltdb/tree/master/examples/voter) (Japanese "American Idol")
* Wikipedia
* [YCSB](https://github.com/brianfrankcooper/YCSB)

## How to Build..

## How to Run...

