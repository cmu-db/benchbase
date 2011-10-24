# run wikipedia with 20 terminals over a small trace with 10 sec warmup and 50 sec measurement time
java -Xmx1024m -cp `run/classpath.sh` com.oltpbenchmark.DBWorkload -b wikipedia -c config/sample_wiki_config.xml

