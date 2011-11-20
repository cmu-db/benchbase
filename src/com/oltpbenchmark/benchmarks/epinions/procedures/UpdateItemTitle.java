package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateItemTitle extends Procedure {

    public final SQLStmt updateItem = new SQLStmt(
        "UPDATE item SET title = ? WHERE i_id=?"
    );
    
    public void run(Connection conn, long iid, String title) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateItem);
        stmt.setString(1, title);
        stmt.setLong(2, iid);
        stmt.executeUpdate();
    }
}
