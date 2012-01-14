package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetReviewsByUser extends Procedure {
    
    public final SQLStmt getReviewUser = new SQLStmt(
        "SELECT * FROM review r, user u WHERE u.u_id = r.u_id AND r.u_id=? " +
        "ORDER BY rating LIMIT 10;"
    );

    public void run(Connection conn, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getReviewUser);
        stmt.setLong(1, uid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
}
