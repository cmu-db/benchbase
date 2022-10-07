package com.oltpbenchmark.benchmarks.featurebench.customworkload;

import com.oltpbenchmark.benchmarks.featurebench.YBMicroBenchmark;
import com.oltpbenchmark.benchmarks.featurebench.helpers.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.helpers.LoadRule;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class CreateAndDeleteTables extends YBMicroBenchmark {

    public CreateAndDeleteTables(HierarchicalConfiguration<ImmutableNode> config) {
        super(config);
    }

    @Override
    public void create(Connection conn) throws SQLException {
        Statement stmtObj = conn.createStatement();
        if (config.getBoolean("/testDrop")) {
            for (int i = 0; i < config.getInt("/numTables"); i++) {
                stmtObj.execute("create table a" + i);
            }
        }
    }

    @Override
    public ArrayList<LoadRule> loadRules() {
        return null;
    }

    @Override
    public ArrayList<ExecuteRule> executeRules() {
        return null;
    }

    @Override
    public void cleanUp(Connection conn) throws SQLException {
        Statement stmtObj = conn.createStatement();
        if (config.getBoolean("/testCreate")) {
            for (int i = 0; i < config.getInt("/numTables"); i++) {
                stmtObj.execute("drop table a" + i);
            }
        }
    }

    @Override
    public void executeOnce(Connection conn) throws SQLException {
        Statement stmtObj = conn.createStatement();
        if (config.getBoolean("/testCreate")) {
            for (int i = 0; i < config.getInt("/numTables"); i++) {
                stmtObj.execute("create table a" + i);
            }
        }
        if (config.getBoolean("/testDrop")) {
            for (int i = 0; i < config.getInt("/numTables"); i++) {
                stmtObj.execute("drop table a" + i);
            }
        }
    }

}
