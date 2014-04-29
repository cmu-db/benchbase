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
package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

public class GetTweetsFromFollowing extends Procedure {

    public final SQLStmt getFollowing = new SQLStmt(
        "SELECT f2 FROM " + TwitterConstants.TABLENAME_FOLLOWS +
        " WHERE f1 = ? LIMIT " + TwitterConstants.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid IN (??)", TwitterConstants.LIMIT_FOLLOWERS
    );
    
    public void run(Connection conn, int uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getFollowing);
        stmt.setLong(1, uid);
        ResultSet rs = stmt.executeQuery();
        
        stmt = this.getPreparedStatement(conn, getTweets);
        int ctr = 0;
        long last = -1;
        while (rs.next() && ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
            last = rs.getLong(1);
            stmt.setLong(ctr, last);
            
        } // WHILE
        rs.close();
        if (ctr > 0) {
            while (ctr++ < TwitterConstants.LIMIT_FOLLOWERS) {
                stmt.setLong(ctr, last);
            } // WHILE     
            rs = stmt.executeQuery();
            rs.close();
        }
        else 
        {
            // LOG.debug("No followers for user: "+uid); // so what .. ?
        }
    }
    
}
