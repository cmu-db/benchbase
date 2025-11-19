# Stitcher Benchmark

This shell script is used to execute a collection of benchmarks over 24 hours that mimic a resource utilization pattern observed as part of the work done in the following paper:
[Stitcher: Learned Workload Synthesis from Historical Performance Footprints.](https://openproceedings.org/2023/conf/edbt/paper-19.pdf), Chengcheng Wan, Yiwen Zhu, Joyce Cahoon, Wenjing Wang, Katherine Lin, Sean Liu, Raymond Truong, Neetu Singh, Alexandra M. Ciortea, Konstantinos Karanasos, Subru Krishnan. In EDBT 2023.
If you use this workload, please reference this paper appropriately.

Prior to executing the shell scripts, the data needs to be preloaded with the following commands:

```sh
java -jar benchbase.jar -b tpcc -c config/$dbms/stitcher/2023-02-24-15-11-28-957898.xml --create=true --load=true --execute=false
java -jar benchbase.jar -b tpcc -c config/$dbms/stitcher/2023-02-24-15-26-45-119435.xml --create=true --load=true --execute=false
java -jar benchbase.jar -b tpch -c config/$dbms/stitcher/tpch-large.xml --create=true --load=true --execute=false
java -jar benchbase.jar -b ycsb -c config/$dbms/stitcher/2023-02-24-15-16-36-026578.xml --create=true --load=true --execute=false
```

These commands will load four datasets: TPC-C (scale factors 16 & 160), TPC-H (scale factor 10), and YCSB (scale factor 1200).

Using these instances, the shell script mimics resource utilization by calling instantiations with varying load of each of these workloads, mimicking real-world workload patterns.
Note that this workload was designed to imitate an 8-core SQLServer instance with 32GB memory.

To execute the script, copy it to the head folder that also contains the benchbase executable, then instantiate the execution for example like this:

```sh
./stitcher_sqlserver
```

Note that the execution of this workload takes 24 hours.