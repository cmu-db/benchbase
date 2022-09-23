package com.oltpbenchmark.benchmarks.featurebench;

public interface FeatureBenchConstants2 {


    String DROP_QUERY_1 = "DROP TABLE IF EXISTS distributors";
    String DROP_DB = "DROP DATABASE test_db";

    // table and database names
    String DB_NAME = "test_db";


    String TABLE_NAME1 = "distributors";

    // Create database
    String USE_DATABASE_QUERY = "CREATE DATABASE test_db;";

    String drop_sequence = "DROP SEQUENCE IF EXISTS serial_no";
    String Sequence_generator = "CREATE SEQUENCE serial_no";

    String CREATE_TABLE_1 = "CREATE TABLE distributors("
        + " did   DECIMAL(3)  DEFAULT NEXTVAL('serial_no'),"
        + " dname  VARCHAR(40) DEFAULT 'lusofilms' "
        + ");";
    String DROP_TABLE_1 = "DROP TABLE distributors";

    String DROP_DATABASE = "DROP DATABASE TEST_DB";
    enum random_names {lusofilms, AssociatedComputing, Inc, XYZWidgets, GizmoTransglobal, RedlineGmbH, AcmeCorporation}

}
