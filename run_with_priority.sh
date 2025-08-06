#!/bin/bash

# Alternative approach: Run with system property to force our JSON library
BENCHBASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

java \
  -Djava.system.class.loader=java.net.URLClassLoader \
  -Xbootclasspath/p:"$BENCHBASE_DIR/benchbase-postgres/lib/json-20240303.jar" \
  -cp "$BENCHBASE_DIR/benchbase-postgres/lib/*:$BENCHBASE_DIR/benchbase-postgres/benchbase-postgres.jar" \
  com.oltpbenchmark.DBWorkload "$@"