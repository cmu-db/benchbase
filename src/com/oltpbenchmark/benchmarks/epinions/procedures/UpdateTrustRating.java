package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateTrustRating extends Procedure {

    public final SQLStmt updateTrust = new SQLStmt(
        "UPDATE trust SET trust = ? WHERE source_u_id=? AND target_u_id=?"
    );
    
    public void run(Connection conn, long source_uid, long target_uid, int trust) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateTrust);
        stmt.setInt(1, trust);
        stmt.setLong(2, source_uid);
        stmt.setLong(3, target_uid);
        stmt.executeUpdate();
    }
}
