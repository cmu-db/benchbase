package com.oltpbenchmark.benchmarks.featurebench.CustomBenchmark;


import com.oltpbenchmark.benchmarks.featurebench.FeatureBenchConstants;
import com.oltpbenchmark.benchmarks.featurebench.util.*;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;


public class YBMicroBenchmarkImplementation implements YBMicroBenchmark, FeatureBenchConstants {

    public final static Logger logger = Logger.getLogger(YBMicroBenchmarkImplementation.class);

    @Override
    public void createDB(Connection conn) throws SQLException {

        Statement stmtOBj = null;
        stmtOBj = conn.createStatement();

        // DDL Statement 1 - DROP EXISTING TABLES IF THEY EXIST
        logger.info("\n=======DROPPING TABLES IF THEY EXIST=======");
        stmtOBj.executeUpdate(DROP_QUERY_1);
        logger.info("\n=======DROPPING DATABASE IF IT EXISTS=======");
        stmtOBj.executeUpdate(DROP_DB);

        // DDL Statement 2 - CREATE ALL THE DATABASE AND TABLES
        logger.info("\n=======CREATE " + DB_NAME + " DATABASE=======");
        stmtOBj.executeUpdate(USE_DATABASE_QUERY);
        logger.info("\n=======DATABASE IS SUCCESSFULLY CREATED=======\n");
        stmtOBj.executeUpdate(CREATE_TABLE_1);
        stmtOBj.executeUpdate(INDEX_TABLE_1);

        try {
            stmtOBj.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public ArrayList<LoadRule> loadRule() {
        int startIndex = 0;
        int endIndex = 10000;
        ArrayList<Integer> range = new ArrayList<>();
        range.add(startIndex);
        range.add(endIndex);
        ParamsForUtilFunc paramsFunc1 = new ParamsForUtilFunc(range);
        ArrayList<Integer> string_len = new ArrayList<>();
        int desired_len = 10;
        string_len.add(desired_len);
        ParamsForUtilFunc paramsFunc2 = new ParamsForUtilFunc(string_len);
        ArrayList<ParamsForUtilFunc> list1 = new ArrayList<>();
        ArrayList<ParamsForUtilFunc> list2 = new ArrayList<>();
        list1.add(paramsFunc1);
        list2.add(paramsFunc2);
        UtilityFunc uf1 = new UtilityFunc("get_int_primary_key", list1);
        UtilityFunc uf2 = new UtilityFunc("numberToIdString", list2);
        columnsDetails cd1 = new columnsDetails("id", uf1);
        columnsDetails cd2 = new columnsDetails("account_id", uf2);
        ArrayList<columnsDetails> col_det = new ArrayList<>();
        col_det.add(cd1);
        col_det.add(cd2);
        TableInfo ti = new TableInfo(10, "accounts", col_det);
        LoadRule lr = new LoadRule(ti);
        ArrayList<LoadRule> rule = new ArrayList<>();
        rule.add(lr);
        return rule;

    }

    public ArrayList<ExecuteRule> executeRule() {

        int startIndex = 10;
        int endIndex = 100;
        int fix_len = 20;
        ArrayList<Integer> range = new ArrayList<>();
        range.add(startIndex);
        range.add(endIndex);
        ArrayList<Integer> string_len = new ArrayList<>();
        string_len.add(fix_len);
        ParamsForUtilFunc paramsFunc1 = new ParamsForUtilFunc(range);
        ParamsForUtilFunc paramsFunc2 = new ParamsForUtilFunc(string_len);
        ArrayList<ParamsForUtilFunc> list1 = new ArrayList<>();
        ArrayList<ParamsForUtilFunc> list2 = new ArrayList<>();
        list1.add(paramsFunc1);
        list2.add(paramsFunc2);
        UtilityFunc uf1 = new UtilityFunc("get_int_primary_key", list1);
        UtilityFunc uf2 = new UtilityFunc("numberToIdString", list2);
        ArrayList<UtilityFunc> list_of_util = new ArrayList<>();
        list_of_util.add(uf1);
        list_of_util.add(uf2);
        BindParams bd = new BindParams(list_of_util);
        String query = "SELECT * FROM ACCOUNTS WHERE ID > ?";
        ArrayList<BindParams> bp = new ArrayList<>();
        bp.add(bd);
        QueryDetails qd = new QueryDetails(query, bp);
        ArrayList<QueryDetails> list3 = new ArrayList<>();
        list3.add(qd);
        TransactionDetails td = new TransactionDetails("Account_query", 100, list3);
        ExecuteRule ob = new ExecuteRule(td);
        ArrayList<ExecuteRule> list4 = new ArrayList<>();
        list4.add(ob);
        return list4;
    }

    @Override
    public void cleanUp(Connection conn) throws SQLException {

        Statement stmtOBj = null;
        stmtOBj = conn.createStatement();

        // DDL Statement - DROP TABLES
        logger.info("\n=======DROP ALL THE TABLES=======");
        stmtOBj.executeUpdate(DROP_TABLE_1);
        logger.info("\n=======TABLES ARE SUCCESSFULLY DROPPED FROM THE DATABASE=======\n");
        // DDL Statement DROP DATABASE
        logger.info("\n=======DROP DATABASE=======");
        stmtOBj.executeUpdate(DROP_DATABASE);
        logger.info("\n=======DATABASE IS SUCCESSFULLY DROPPED=======");
        try {

            stmtOBj.close();
            conn.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}









