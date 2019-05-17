@echo off

REM classpath string will be changed with windows batch script

set "oltpbenchmark_cp=build;lib/c3p0-0.9.1.2.jar;lib/commons-cli-1.2.jar;lib/commons-collections15-4.01.jar;lib/commons-collections-3.2.1.jar;lib/commons-configuration-1.6.jar;lib/commons-io-2.2.jar;lib/commons-jxpath-1.3.jar;lib/commons-lang-2.6.jar;lib/commons-logging-1.1.1.jar;lib/commons-math3-3.0.jar;lib/db2jcc4.jar;lib/edb-jdbc14-8_0_3_14.jar;lib/ganymed-ssh2-build250.jar;lib/h2-1.3.163.jar;lib/hsqldb.jar;lib/httpclient-4.3.1.jar;lib/httpcore-4.3.jar;lib/httpmime-4.3.1.jar;lib/jdo2-index.jar;lib/jpa2.jar;lib/jtds-1.2.5.jar;lib/junit-4.4.jar;lib/log4j-1.2.15.jar;lib/log4jdbc4-1.2.jar;lib/monetdb-jdbc-2.9.jar;lib/mysql-connector-java-5.1.47.jar;lib/ojdbc6.jar;lib/opencsv-2.3.jar;lib/openjpa-index-annotation.jar;lib/postgresql-9.1-901.jdbc3.jar;lib/postgresql-9.1-901.jdbc4.jar;lib/slf4j-simple-1.7.5.jar;lib/sqlite-jdbc-3.6.20.1.jar;lib/sqljdbc4.jar;lib/hibernate/antlr-2.7.6.jar;lib/hibernate/dom4j-1.6.1.jar;lib/hibernate/hibernate3.jar;lib/hibernate/hibernate-jpa-2.0-api-1.0.1.Final.jar;lib/hibernate/javassist-3.12.0.GA.jar;lib/hibernate/jta-1.1.jar;lib/hibernate/slf4j-api-1.6.1.jar"

REM echo %oltpbenchmark_cp%

java -Xmx8G -cp %oltpbenchmark_cp% -Dlog4j.configuration=log4j.properties com.oltpbenchmark.DBWorkload %* 

