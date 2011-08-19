#!/bin/sh
#
# Loads TPC-C for relational cloud

set -e

WAREHOUSES=2

PARTITIONER=$HOME/partitioner
BENCHMARK="."
PROPERTIES_FILE=$BENCHMARK/run/relcloud.properties

CLASSPATH="$BENCHMARK/build/classes
    $BENCHMARK/lib/commons-lang-2.5.jar
    $PARTITIONER/classes
    $PARTITIONER/lib/dtxn.jar
    $PARTITIONER/lib/mysql-connector-java-5.1.10-bin.jar
    $PARTITIONER/lib/protobuf-java-2.3.0.jar"
CLASSPATH=`echo $CLASSPATH | sed 's/ /:/g'`
JAVA="java -ea -cp $CLASSPATH"

echo $JAVA -Dprop=$PROPERTIES_FILE -DcommandFile=$BENCHMARK/run/sql/sqlTableCreates jdbc.ExecJDBC 
echo $JAVA -Dprop=$PROPERTIES_FILE LoadData.LoadData numWarehouses $WAREHOUSES
echo $JAVA -Dprop=$PROPERTIES_FILE -DcommandFile=$BENCHMARK/run/sql/sqlIndexCreates jdbc.ExecJDBC 

# Combine these custom properties with properties from the relcloud.properties file
TERMINALS=`expr 10 \* $WAREHOUSES`
PROPERTIES="nwarehouses=$WAREHOUSES
    nterminals=$TERMINALS
    timeLimit=60"
PROPERTIES=`echo $PROPERTIES | cat - $PROPERTIES_FILE`
PROPERTIES=`echo $PROPERTIES | sed 's/^ */-D/; s/ / -D/g' ;`

echo $JAVA $PROPERTIES client.jTPCCHeadless mysql-tpcc 0
