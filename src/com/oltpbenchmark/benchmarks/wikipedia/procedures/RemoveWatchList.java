package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class RemoveWatchList extends Procedure {
	
	public SQLStmt removeWatchList = new SQLStmt(
        "DELETE FROM watchlist WHERE " +
		"wl_user = ? AND wl_namespace = ? AND wl_title = ?"
    );
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE  user SET user_touched = ? WHERE user_id =  ? "
    ); 

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
			
			ps= this.getPreparedStatement(conn, setUserTouched);
			ps.setString(1, LoaderUtil.getCurrentTime14());
			ps.setInt(2, userId);
			ps.executeUpdate();
			conn.commit();
		}
	 }
 
}
