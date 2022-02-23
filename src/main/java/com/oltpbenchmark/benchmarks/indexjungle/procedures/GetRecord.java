package com.oltpbenchmark.benchmarks.indexjungle.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetRecord extends Procedure {

    public final SQLStmt GetRecord = new SQLStmt("SELECT * FROM jungle WHERE uuid_field = ?");

    public Object[] run(Connection conn, String uuid) throws SQLException {
        Object[] finalResult = null;
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetRecord)) {
            stmt.setString(1, uuid);

            // Bombs Away!
            try (ResultSet results = stmt.executeQuery()) {
                if (results.next()) {
                    int cols = results.getMetaData().getColumnCount();
                    finalResult = new Object[cols];
                    for (int i = 0; i < cols; i++) {
                        finalResult[i] = results.getObject(i + 1).toString();
                    }
                }
            }
        }
        return (finalResult);
    }

}
