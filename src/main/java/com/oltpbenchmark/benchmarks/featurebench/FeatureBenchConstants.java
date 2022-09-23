package com.oltpbenchmark.benchmarks.featurebench;

public interface FeatureBenchConstants {

    String DROP_QUERY_1 = "DROP TABLE IF EXISTS accounts";
    String DROP_DB = "DROP DATABASE TEST_DB";

    // table and database names
    String DB_NAME = "TEST_DB";

    String TABLE_NAME3 = "accounts";


    // Create database
    String USE_DATABASE_QUERY = "CREATE DATABASE TEST_DB;";

    // CREATE DDL COMMANDS
    String CREATE_TABLE_1 = " CREATE TABLE accounts ("
        + "id int NOT NULL,"
        + "name varchar(64) NOT NULL,"
        + "CONSTRAINT pk_accounts PRIMARY KEY (id)"
        + ");";

    String INDEX_TABLE_1 = "CREATE INDEX idx_accounts_name ON accounts (name);";
    String DROP_TABLE_1 = "DROP TABLE accounts";
    String DROP_DATABASE = "DROP DATABASE TEST_DB";

}
