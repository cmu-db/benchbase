package com.oltpbenchmark.benchmarks.linkbench.procedures;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.pojo.Link;
import com.oltpbenchmark.api.Procedure;

public class UpdateLink extends Procedure{
    
    private static final Logger LOG = Logger.getLogger(UpdateLink.class);

    public void run(Connection conn, Link l, boolean noinverse) throws SQLException {
        // executed through addLink Procedure
    }
}
