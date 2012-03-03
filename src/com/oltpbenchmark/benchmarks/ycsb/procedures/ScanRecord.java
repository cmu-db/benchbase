package com.oltpbenchmark.benchmarks.ycsb.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class ScanRecord extends Procedure{
    public final SQLStmt scanStmt = new SQLStmt(
        "SELECT * FROM USERTABLE WHERE YCSB_KEY>? AND YCSB_KEY<?"
    );
    
	//FIXME: The value in ysqb is a byteiterator
    public void run(Connection conn, int start, int count, List<Map<Integer,String>> results) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, scanStmt);
        stmt.setInt(1, start); 
        stmt.setInt(2, start+count); 
        ResultSet r=stmt.executeQuery();
        while(r.next())
        {
        	HashMap<Integer,String> m=new HashMap<Integer,String>();
        	for(int i=1;i<11;i++)
        		m.put(i, r.getString(i));
        	results.add(m);
        }
        r.close();
    }
}
