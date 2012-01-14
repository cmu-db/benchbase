package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetReviewItemById extends Procedure {

    public final SQLStmt getReviewItem = new SQLStmt(
        "SELECT * FROM review r, item i WHERE i.i_id = r.i_id and r.i_id=? " +
        "ORDER BY rating LIMIT 10;"
    );
    
    public void run(Connection conn, long iid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getReviewItem);
        stmt.setLong(1, iid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
    
}
