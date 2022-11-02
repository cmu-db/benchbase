package com.oltpbenchmark.benchmarks.featurebench;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class YBMicroBenchmark {
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

    public void cleanUp(Connection conn) throws SQLException {
    }

    public void loadOnce(Connection conn) throws SQLException{
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


