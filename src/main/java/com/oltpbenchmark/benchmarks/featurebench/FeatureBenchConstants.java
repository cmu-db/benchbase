package com.oltpbenchmark.benchmarks.featurebench;

public interface FeatureBenchConstants {

    //Drop TABLES IF THEY EXIST
//    public static final String DROP_QUERY_1= "DROP TABLE IF EXISTS checking;";
//    public static final String DROP_QUERY_2 ="DROP TABLE IF EXISTS savings;";
    public static final String DROP_QUERY_1="DROP TABLE IF EXISTS accounts";
    public static final String DROP_DB="DROP DATABASE TEST_DB";

    // table and database names
    public static final String DB_NAME = "TEST_DB";
//    public static final String TABLE_NAME1 = "checking";
//    public static final String TABLE_NAME2 = "savings";
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
//
//    public static final  String CREATE_TABLE_2= "CREATE TABLE savings ("
//        +"custid bigint NOT NULL,"
//        +"bal float  NOT NULL,"
//        +"CONSTRAINT pk_savings PRIMARY KEY (custid),"
//        +"FOREIGN KEY (custid) REFERENCES accounts (custid)"
//        +");";
//
//    public static final String CREATE_TABLE_3= "CREATE TABLE checking ("
//        +"custid bigint NOT NULL,"
//        +"bal    float  NOT NULL,"
//        +"CONSTRAINT pk_checking PRIMARY KEY (custid),"
//        +"FOREIGN KEY (custid) REFERENCES accounts (custid)"
//        +");";

    // DROP DDL COMMAND
//    public static final String DROP_TABLE_1 = "DROP TABLE checking;";
//    public static final String DROP_TABLE_2 = "DROP TABLE savings;";
    public static final String DROP_TABLE_1 = "DROP TABLE accounts";
    public static final String DROP_DATABASE = "DROP DATABASE TEST_DB";


    /*

   // PROCEDURES provided by execute rule.

    public static final String GetAccount = "SELECT * FROM " + FeatureBenchConstants.TABLE_NAME3 + " WHERE name = ?;";
    public static final String GetSavingsBalance = "SELECT bal FROM "+ FeatureBenchConstants.TABLE_NAME2 + "WHERE custid = ?;";
    public static final String GetCheckingBalance="SELECT bal FROM " + FeatureBenchConstants.TABLE_NAME1 + "WHERE custid = ?;";
*/


}










