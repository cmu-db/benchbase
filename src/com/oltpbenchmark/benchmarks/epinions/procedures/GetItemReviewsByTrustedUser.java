package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetItemReviewsByTrustedUser extends Procedure {

	//FIXME: CARLO, does this make sense?
    public final SQLStmt getReview = new SQLStmt(
        "SELECT * FROM review r WHERE r.i_id=?"
    );
    
    public final SQLStmt getTrust = new SQLStmt(
        "SELECT * FROM trust t WHERE t.source_u_id=?"
    );
    
    public void run(Connection conn, long iid, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getReview);
        stmt.setLong(1, iid);
        ResultSet r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
        stmt = this.getPreparedStatement(conn, getTrust);
        stmt.setLong(1, uid);
        r= stmt.executeQuery();
        while (r.next()) {
            continue;
        }
        r.close();
    }
    
}
