package com.oltpbenchmark.benchmarks.sibench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class MinRecord extends Procedure{
    public final SQLStmt minStmt = new SQLStmt(
        "SELECT id FROM sitest ORDER BY value ASC LIMIT 1"
    );

    public int run(Connection conn) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, minStmt);
        ResultSet r=stmt.executeQuery();
        int minId = 0;
        while(r.next())
        {
        	minId = r.getInt(1);
        }
        r.close();
        return minId;
    }
}
