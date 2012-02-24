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

public class RemoveWatchList extends Procedure {
	
	public SQLStmt removeWatchList = new SQLStmt(
        "DELETE FROM " + WikipediaConstants.TABLENAME_WATCHLIST +
        " WHERE wl_user = ? AND wl_namespace = ? AND wl_title = ?"
    );
    public SQLStmt setUserTouched = new SQLStmt(
        "UPDATE " + WikipediaConstants.TABLENAME_USER +
        "   SET user_touched = ? " +
        " WHERE user_id =  ? "
    ); 

    public void run(Connection conn, int userId, int nameSpace, String pageTitle) throws SQLException {

        if (userId > 0) {
            PreparedStatement ps = this.getPreparedStatement(conn, removeWatchList);
            ps.setInt(1, userId);
            ps.setInt(2, nameSpace);
            ps.setString(3, pageTitle);
            ps.executeUpdate();

            if (nameSpace == 0) {
                // if regular page, also remove a line of
                // watchlist for the corresponding talk page
                ps = this.getPreparedStatement(conn, removeWatchList);
                ps.setInt(1, userId);
                ps.setInt(2, 1);
                ps.setString(3, pageTitle);
                ps.executeUpdate();
            }

            ps = this.getPreparedStatement(conn, setUserTouched);
            ps.setString(1, TimeUtil.getCurrentTimeString14());
            ps.setInt(2, userId);
            ps.executeUpdate();
        }
    }
}
