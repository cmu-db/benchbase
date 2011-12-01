package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class AddWatchList extends Procedure {

    public SQLStmt insertWatchList = new SQLStmt(
            "INSERT IGNORE INTO `watchlist` (wl_usr,wl_namespace,wl_title,wl_notificationtimestamp) " +
            "VALUES (?,?,?,NULL)");
   
    public SQLStmt setUserTouched = new SQLStmt("UPDATE  `usr` SET usr_touched = '" + LoaderUtil.getCurrentTime14()
					+ "' WHERE usr_id =  ? ;");    
	
    public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {
        
		if (userId > 0) {
			PreparedStatement ps =this.getPreparedStatement(conn, insertWatchList);
			ps.setInt(1, userId);
			ps.setInt(2, nameSpace);
			ps.setString(3, pageTitle);
			ps.executeUpdate();
		
			if (nameSpace == 0) 
			{ 
				// if regular page, also add a line of
				// watchlist for the corresponding talk page
				ps =this.getPreparedStatement(conn, insertWatchList);
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
    
}
