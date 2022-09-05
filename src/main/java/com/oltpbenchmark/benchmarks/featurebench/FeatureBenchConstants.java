package com.oltpbenchmark.benchmarks.featurebench;

public interface FeatureBenchConstants {


    public static final String DROP_QUERY_1="DROP TABLE IF EXISTS accounts";
    public static final String DROP_DB="DROP DATABASE TEST_DB";

    // table and database names
    public static final String DB_NAME = "TEST_DB";

    public static final String TABLE_NAME3 = "accounts";


    // Create database
    public static final String USE_DATABASE_QUERY = "CREATE DATABASE TEST_DB;";

    // CREATE DDL COMMANDS
    public static final String CREATE_TABLE_1 = " CREATE TABLE accounts ("
        +"id bigint NOT NULL,"
        +"name varchar(64) NOT NULL,"
        +"CONSTRAINT pk_accounts PRIMARY KEY (custid)"
        +");";

    public static final String INDEX_TABLE_1= "CREATE INDEX idx_accounts_name ON accounts (name);";
    public static final String DROP_TABLE_1 = "DROP TABLE accounts";
    public static final String DROP_DATABASE = "DROP DATABASE TEST_DB";


}










