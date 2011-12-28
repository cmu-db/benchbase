#!/bin/sh -x

ant execute \
    -Dbenchmark=wikipedia \
    -Dconfig=config/sample_wiki_config.xml \
    -Dcreate=false \
    -Dload=false \
    -Dexecute=true
