package com.oltpbenchmark.benchmarks.featurebench;

import com.oltpbenchmark.benchmarks.featurebench.util.ExecuteRule;
import com.oltpbenchmark.benchmarks.featurebench.util.LoadRule;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public abstract class YBMicroBenchmark {

    public boolean createDBImplemented = true;
    public boolean cleanUpImplemented = true;
    public boolean loadOnceImplemented = false;
    public boolean executeOnceImplemented = false;

    public boolean afterLoadImplemented = false;

    public boolean beforeLoadImplemented = false;


    public HierarchicalConfiguration<ImmutableNode> config;

    public YBMicroBenchmark(HierarchicalConfiguration<ImmutableNode> config) {
        this.config = config;
    }

    public void create(Connection conn) throws SQLException {
    }

    public abstract ArrayList<LoadRule> loadRules();

    public abstract ArrayList<ExecuteRule> executeRules();

    public void cleanUp(Connection conn) throws SQLException {
    }

    public void loadOnce(Connection conn) throws SQLException {
    }

    public void executeOnce(Connection conn) throws SQLException {
    }

    public void afterLoad(Connection conn) throws SQLException {
    }

    public void execute(Connection conn) throws SQLException {
    }

    public void beforeLoad(Connection conn) throws SQLException {
    }

}


