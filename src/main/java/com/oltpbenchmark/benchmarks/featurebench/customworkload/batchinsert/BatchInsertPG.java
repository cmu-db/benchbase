package com.oltpbenchmark.benchmarks.featurebench.customworkload.batchinsert;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BatchInsertPG extends YBMicroBenchmark {
    private static final Logger LOG = LoggerFactory.getLogger(BatchInsertPG.class);

    public BatchInsertPG(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
        this.executeOnceImplemented = true;
        this.loadOnceImplemented = true;
    }

    @Override
    public void create(Connection conn) throws SQLException {
        Statement stmtOBj = conn.createStatement();
        stmtOBj.executeUpdate("drop table if exists index_test_4;" +
            "create table index_test_4 (id bigint, col_int_1 int, col_int_2 int, col_int_3 int, col_int_4 int, col_int_5 int, col_int_6 int, col_int_7 int, col_int_8 int, col_int_9 int, col_int_10 int, col_varchar_1 varchar(20), col_varchar_2 varchar(20), col_varchar_3 varchar(20), col_date text, col_boolean boolean, primary key (id));" +
            "DROP PROCEDURE IF EXISTS insert_demo_hash;" +
            "CREATE PROCEDURE insert_demo_hash(_numRows int)" +
            "    LANGUAGE plpgsql" +
            "    AS $$" +
            "    DECLARE" +
            "    BEGIN" +
            "    insert into index_test_4 (id, col_int_1, col_int_2, col_int_3, col_int_4, col_int_5, col_int_6, col_int_7, col_int_8, col_int_9, col_int_10, col_varchar_1, col_varchar_2, col_varchar_3, col_date, col_boolean) select n, n, n+100, (n%100)+1, (n%1000)+1, n%50, n*10, n%10, n*2, n, n, 'aaa'||(n%1000)+1,  'bbb'||n, 'ccc'||(n%100), '2022-12-10', RANDOM()::INT::BOOLEAN from generate_series(1,_numRows) n;" +
            "    END;" +
            "    $$;" +
            "call insert_demo_hash(1000000);");
        stmtOBj.close();
    }

    public void loadOnce(Connection conn) throws SQLException {
    }

    public void executeOnce(Connection conn) throws SQLException {
        String query = "insert into index_test_4 select n, n, n+100, (n%100)+1, (n%1000)+1, n%50, n*10, n%10, n*2, " +
            "n, n, 'aaa'||(n%1000)+1,  'bbb'||n, 'ccc'||(n%100), '2022-12-10', RANDOM()::INT::BOOLEAN " +
            "from generate_series(1000001,1100000) n;";
        int rows = conn.createStatement().executeUpdate(query);
        LOG.info("Rows inserted: " + rows);
    }

    @Override
    public void cleanUp(Connection conn) throws SQLException {
        String countQuery = "select count(*) from index_test_4";
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(countQuery);
        long count = 0;
        while (rs.next()) {
            count = rs.getLong(1);
        }
        LOG.info("Number of rows inserted are: " + count);
    }

}
