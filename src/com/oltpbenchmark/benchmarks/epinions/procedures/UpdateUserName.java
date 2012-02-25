package com.oltpbenchmark.benchmarks.epinions.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class UpdateUserName extends Procedure {

    public final SQLStmt updateUser = new SQLStmt("UPDATE user SET name = ? WHERE u_id=?");
    
    public void run(Connection conn, long uid, String name) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, updateUser);
        stmt.setString(1, name);
        stmt.setLong(2, uid);
        stmt.executeUpdate();
    }
}
