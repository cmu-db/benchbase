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

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tatp.TATPLoader;
import com.oltpbenchmark.benchmarks.twitter.TwitterWorker;

public class GetTweetsFromFollowing extends Procedure {
	private static final Logger LOG = Logger.getLogger(GetTweetsFromFollowing.class);

    public final SQLStmt getFollowing = new SQLStmt(
        "SELECT f2 FROM follows WHERE f1 = ? LIMIT " + TwitterWorker.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM tweets WHERE uid IN (??)", TwitterWorker.LIMIT_FOLLOWERS
    );
    
    public ResultSet run(Connection conn, int uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getFollowing);
        stmt.setLong(1, uid);
        ResultSet rs = stmt.executeQuery();
        
        stmt = this.getPreparedStatement(conn, getTweets);
        int ctr = 0;
        long last = -1;
        while (rs.next() && ctr++ < TwitterWorker.LIMIT_FOLLOWERS) {
            last = rs.getLong(1);
            stmt.setLong(ctr, last);
            
        } // WHILE
        if (ctr > 0) {
            while (ctr++ < TwitterWorker.LIMIT_FOLLOWERS) {
                stmt.setLong(ctr, last);
            } // WHILE     
            return stmt.executeQuery();
        }
        else 
        {
            // LOG.debug("No followers for user: "+uid); // so what .. ?
            return (null);
        }
    }
    
}
