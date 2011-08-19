#!/usr/bin/env bash
set -x
exec java -cp `./classpath.sh` `cat $1 | dos2unix | sed 's/^/-D/'` client.jTPCCHeadless
