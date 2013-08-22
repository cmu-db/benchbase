package com.oltpbenchmark.benchmarks.sibench.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateRecord extends Procedure{
    public final SQLStmt updateStmt = new SQLStmt(
        "UPDATE sitest SET value = value + 1 WHERE id = ?"
    );

    public void run(Connection conn, int id) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateStmt);
        stmt.setInt(1, id);
        stmt.executeUpdate();
    }

}
