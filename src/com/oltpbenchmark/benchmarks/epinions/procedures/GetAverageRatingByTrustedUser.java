package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetAverageRatingByTrustedUser extends Procedure {

    public final SQLStmt getAverageRating = new SQLStmt(
        "SELECT avg(rating) FROM review r, trust t WHERE r.u_id=t.target_u_id AND r.i_id=? AND t.source_u_id=?"
    );
    
    public void run(Connection conn, long iid, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getAverageRating);
        stmt.setLong(1, iid);
        stmt.setLong(2, uid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
    
}
