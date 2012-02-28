package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class InsertRecord extends Procedure{
    public final SQLStmt insertStmt = new SQLStmt(
        "INSERT INTO USERTABLE VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname, Map<Integer,String> vals) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, insertStmt);
        stmt.setInt(1, keyname);
        for(Entry<Integer,String> s:vals.entrySet())
        {
        	stmt.setString(s.getKey()+1, s.getValue());
        }            
        stmt.executeUpdate();
    }

}
