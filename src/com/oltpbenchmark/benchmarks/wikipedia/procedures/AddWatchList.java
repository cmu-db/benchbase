package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class AddWatchList extends Procedure {

    public SQLStmt insertWatchList = new SQLStmt(
            "INSERT IGNORE INTO `watchlist` (wl_user,wl_namespace,wl_title,wl_notificationtimestamp) VALUES " +
            "('	?? ','?? ',?,NULL)");
   
    public SQLStmt setUserTouched = new SQLStmt("UPDATE  `user` SET user_touched = '" + getTimeStamp14char()
					+ "' WHERE user_id = ' ?? ';");    
	
    public ResultSet run(Connection conn, int userId, int nameSpace, String pageTitle)
	throws SQLException {
        
		if (userId > 0) {
			PreparedStatement ps =this.getPreparedStatement(conn, insertWatchList);
			ps.setString(1, pageTitle);
			ps.executeUpdate();
			ps.close();
		
			if (nameSpace == 0) { // if regular page, also add a line of
									// watchlist for the corresponding talk page
				ps =this.getPreparedStatement(conn, insertWatchList);
				ps.setString(1, pageTitle);
				ps.executeUpdate();
				ps.close();
			}

			PreparedStatement stmt = this.getPreparedStatement(conn, setUserTouched);
			stmt.execute();
			stmt.close();
			conn.commit();
		}
		return null;
	}
    
	private String getTimeStamp14char() {
		// TODO Auto-generated method stub
		java.util.Date d = Calendar.getInstance().getTime();
		return "" + d.getYear() + d.getMonth() + d.getDay() + d.getHours()
				+ d.getMinutes() + d.getSeconds();
	}
}
