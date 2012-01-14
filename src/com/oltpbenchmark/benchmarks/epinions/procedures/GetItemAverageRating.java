package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetItemAverageRating extends Procedure {

    public final SQLStmt getAverageRating = new SQLStmt(
        "SELECT avg(rating) FROM review r WHERE r.i_id=?"
    );
    
    public void run(Connection conn, long iid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getAverageRating);
        stmt.setLong(1, iid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
}
