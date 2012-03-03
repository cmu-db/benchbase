package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class ReadRecord extends Procedure{
    public final SQLStmt readStmt = new SQLStmt(
        "SELECT * FROM USERTABLE WHERE YCSB_KEY=?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int keyname, Map<Integer,String> results) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, readStmt);
        stmt.setInt(1, keyname);          
        ResultSet r=stmt.executeQuery();
        while(r.next())
        {
        	for(int i=1;i<11;i++)
        		results.put(i, r.getString(i));
        }
        r.close();
    }

}
