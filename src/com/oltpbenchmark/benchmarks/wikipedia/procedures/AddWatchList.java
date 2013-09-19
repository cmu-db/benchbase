/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:    Carlo Curino <carlo.curino@gmail.com>
 *              Evan Jones <ej@evanjones.ca>
 *              DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 *              Andy Pavlo <pavlo@cs.brown.edu>
 *              CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *                  Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.wikipedia.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.wikipedia.WikipediaConstants;
import com.oltpbenchmark.util.TimeUtil;

public class AddWatchList extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public SQLStmt insertWatchList = new SQLStmt(
        "INSERT INTO " + WikipediaConstants.TABLENAME_WATCHLIST + " (" + 
        "wl_user, wl_namespace, wl_title, wl_notificationtimestamp" +
        ") VALUES (" +
        "?,?,?,NULL" +
        ")"
    );
   
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER + 
        "   SET user_touched = ? WHERE user_id = ?"
    );
    
    // -----------------------------------------------------------------
    // RUN
    // -----------------------------------------------------------------
	
    public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {
		if (userId > 0) {
		    // TODO: find a way to by pass Unique constraints in SQL server (Replace, Merge ..?)
		    // Here I am simply catching the right excpetion and move on.
		    try
		    {
    			PreparedStatement ps = this.getPreparedStatement(conn, insertWatchList);
    			ps.setInt(1, userId);
    			ps.setInt(2, nameSpace);
    			ps.setString(3, pageTitle);
    			ps.executeUpdate();
		    }
		    catch (SQLException ex) {
                if (ex.getErrorCode() != 2627 || !ex.getSQLState().equals("23000"))
                    throw new RuntimeException("Unique Key Problem in this DBMS");
            }
		
			if (nameSpace == 0) 
			{ 
		        try
		        {
    				// if regular page, also add a line of
    				// watchlist for the corresponding talk page
    			    PreparedStatement ps = this.getPreparedStatement(conn, insertWatchList);
    				ps.setInt(1, userId);
    				ps.setInt(2, 1);
    				ps.setString(3, pageTitle);
    				ps.executeUpdate();
		        }
	            catch (SQLException ex) {
	                if (ex.getErrorCode() != 2627 || !ex.getSQLState().equals("23000"))
	                    throw new RuntimeException("Unique Key Problem in this DBMS");
	            }
			}

			PreparedStatement ps = this.getPreparedStatement(conn, setUserTouched);
			ps.setString(1, TimeUtil.getCurrentTimeString14());
			ps.setInt(2, userId);
			ps.executeUpdate();
		}
	}
    
}
