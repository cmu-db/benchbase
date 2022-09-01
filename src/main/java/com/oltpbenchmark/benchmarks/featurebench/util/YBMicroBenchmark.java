package com.oltpbenchmark.benchmarks.featurebench.util;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public interface YBMicroBenchmark {

    public void createDB(Connection conn) throws SQLException;

    public ArrayList<LoadRule> loadRule();

    public ArrayList<ExecuteRule> executeRule();


    public void cleanUp(Connection conn) throws SQLException;
}
