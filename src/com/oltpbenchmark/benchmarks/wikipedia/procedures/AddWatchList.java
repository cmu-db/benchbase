package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class AddWatchList extends Procedure {

    public SQLStmt insertWatchList = new SQLStmt(
            "INSERT IGNORE INTO watchlist (wl_user,wl_namespace,wl_title,wl_notificationtimestamp) " +
            "VALUES (?,?,?,NULL)");
   
    public SQLStmt setUserTouched = new SQLStmt("UPDATE user SET user_touched = ? WHERE user_id =  ?");    
	
    public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {
        assert userId!=0;
		if (userId > 0) {
		    // TODO: find a way to by pass Unique constraints in SQL server (Replace, Merge ..?)
		    // Here I am simply catching the right excpetion and move on.
		    try
		    {
    			PreparedStatement ps =this.getPreparedStatement(conn, insertWatchList);
    			ps.setInt(1, userId);
    			ps.setInt(2, nameSpace);
    			ps.setString(3, pageTitle);
    			ps.executeUpdate();
		    }
		    catch (SQLServerException ex) {
                if (ex.getErrorCode() != 2627 || !ex.getSQLState().equals("23000"))
                    throw new RuntimeException("Unique Key Problem in this DBMS");
            }
		
			if (nameSpace == 0) 
			{ 
		        try
		        {
    				// if regular page, also add a line of
    				// watchlist for the corresponding talk page
    			    PreparedStatement ps =this.getPreparedStatement(conn, insertWatchList);
    				ps.setInt(1, userId);
    				ps.setInt(2, 1);
    				ps.setString(3, pageTitle);
    				ps.executeUpdate();
		        }
	            catch (SQLServerException ex) {
	                if (ex.getErrorCode() != 2627 || !ex.getSQLState().equals("23000"))
	                    throw new RuntimeException("Unique Key Problem in this DBMS");
	            }
			}

			PreparedStatement ps= this.getPreparedStatement(conn, setUserTouched);
			ps.setString(1,LoaderUtil.getCurrentTime14());
			ps.setInt(2, userId);
			ps.executeUpdate();
			conn.commit();
		}
	}
    
}
