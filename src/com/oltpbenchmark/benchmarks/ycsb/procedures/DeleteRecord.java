package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class DeleteRecord extends Procedure{
    public final SQLStmt deleteStmt = new SQLStmt(
        "DELETE FROM USERTABLE where YCSB_KEY=?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, deleteStmt);
        stmt.setInt(1, keyname);          
        stmt.executeUpdate();
    }

}
