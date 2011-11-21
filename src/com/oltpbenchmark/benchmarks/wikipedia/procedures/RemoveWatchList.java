package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class RemoveWatchList extends Procedure {
	
	public SQLStmt removeWatchList = new SQLStmt("DELETE FROM `watchlist` WHERE " +
			"wl_user = ? AND wl_namespace = ? AND wl_title = ?");
    public SQLStmt setUserTouched = new SQLStmt("UPDATE  `user` SET user_touched = '" + getTimeStamp14char()
			+ "' WHERE user_id =  ? "); 

	public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {	        
		
		if (userId > 0) {	
			PreparedStatement ps =this.getPreparedStatement(conn, removeWatchList);
			ps.setInt(1, userId);
			ps.setInt(2, nameSpace);
			ps.setString(3, pageTitle);
			ps.executeUpdate();
			//ps.close();
	
			if (nameSpace == 0) 
			{ 
				// if regular page, also remove a line of
				// watchlist for the corresponding talk page
				ps =this.getPreparedStatement(conn, removeWatchList);
				ps.setInt(1, userId);
				ps.setInt(2, 1);
				ps.setString(3, pageTitle);
				ps.executeUpdate();
			}
			
			PreparedStatement stmt = this.getPreparedStatement(conn, setUserTouched);
			stmt.setInt(1, userId);
			stmt.executeUpdate();
			conn.commit();
		}
	 }
 
	private String getTimeStamp14char() {
		// TODO Auto-generated method stub
		java.util.Date d = Calendar.getInstance().getTime();
		return "" + d.getYear() + d.getMonth() + d.getDay() + d.getHours()
				+ d.getMinutes() + d.getSeconds();
	}	 
}
