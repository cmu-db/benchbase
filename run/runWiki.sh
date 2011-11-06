java -Xmx1024m -cp `run/classpath.sh` com.oltpbenchmark.DBWorkload -b wikipedia -c config/sample_wiki_config.xml -o ~/trans-wiki1k-io 2>&1 > ~/err-wiki1k-io

