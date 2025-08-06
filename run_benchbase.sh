#!/bin/bash

# BenchBase runner with classpath isolation
# This ensures we use the correct JSON library version

BENCHBASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHBASE_JAR="$BENCHBASE_DIR/benchbase-postgres/benchbase-postgres.jar"
BENCHBASE_LIBS="$BENCHBASE_DIR/benchbase-postgres/lib/*"

# Put our JSON library first in classpath to override system versions
CLASSPATH="$BENCHBASE_DIR/benchbase-postgres/lib/json-20240303.jar:$BENCHBASE_LIBS:$BENCHBASE_JAR"

# Run with explicit classpath
java -cp "$CLASSPATH" com.oltpbenchmark.DBWorkload "$@"