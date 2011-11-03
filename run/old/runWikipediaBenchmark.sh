# run wikipedia with 20 terminals over a small trace with 10 sec warmup and 50 sec measurement time
java -Xmx1024m -cp `./classpath.sh` edu.mit.benchmark.wikipedia.WikipediaBenchmark com.mysql.jdbc.Driver  jdbc:mysql://127.0.0.1:3500/wiki1k root "" trace_100p.txt 50 30 600
