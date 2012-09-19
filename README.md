# OLTPBench

Benchmarking is incredibly useful, yet endlessly painful. This benchmark suite is the result of a group of Phd/post-docs/professors getting together and combining their workloads/frameworks/experiences/efforts. 
We hope this will save other people's time, and will provide an extensible platform, that can be grown in an open-source fashion. 

OLTPBenchmark is a multi-threaded load generator. The framework is designed to be able to produce variable rate, variable mixture load against any JDBC-enabled relational database. The framework also provides data collection features, e.g., per-transaction-type latency and throughput logs.

Together with the framework we provide the following OLTP/Web benchmarks:
  * TPC-C
  * Wikipedia
  * Synthetic Resource Stresser 
  * Twitter
  * Epinions.com
  * TATP
  * !AuctionMark
  * SEATS
  * YCSB
  * JPAB (Hibernate)


This framework is design to allow easy extension, we provide stub code that a contributor can use to include a new benchmark, leveraging all the system features (logging, controlled speed, controlled mixture, etc..)

If you want to contribute a new benchmark please contact: carlo.curino@gmail.com 

If you are using this benchmark for your papers or for your work, please cite us, and let us know so we can add you to our publications-using-us page: http://code.google.com/p/oltpbenchmark/wiki/Publications

Please visit the project homepage for anything other than source code: http://oltpbenchmark.com