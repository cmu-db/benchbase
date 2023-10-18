#!/bin/bash
# I gave it the .log extension so that it would be ignored by git
if [ "$#" -eq 0 ]; then
  # EXEC_ARGS="-b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true"
  EXEC_ARGS="-b replay -c config/postgres/sample_replay_config.xml --execute=true"
else
  EXEC_ARGS="$@"
fi
./mvnw clean package -P postgres -Dmaven.test.skip=true
cd target/benchbase-postgres/benchbase-postgres
java -jar benchbase.jar $EXEC_ARGS