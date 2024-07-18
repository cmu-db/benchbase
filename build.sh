#!/bin/sh

./mvnw clean package -P yugabyte -DskipTests
cd target
tar -xzf benchbase-yugabyte.tgz
cd benchbase-yugabyte
