package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateReviewRating extends Procedure {
    
    public final SQLStmt updateReview = new SQLStmt(
        "UPDATE review SET rating = ? WHERE i_id=? AND u_id=?"
    );
    
    public void run(Connection conn, long iid, long uid, int rating) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateReview);
        stmt.setInt(1, rating);
        stmt.setLong(2, iid);
        stmt.setLong(3, uid);
        stmt.executeUpdate();
    }
}
