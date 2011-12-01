package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterWorker;

public class GetFollowers extends Procedure {
	
	private static final Logger LOG = Logger.getLogger(GetFollowers.class);
	
    public final SQLStmt getFollowers = new SQLStmt(
        "SELECT f2 FROM followers WHERE f1 = ? LIMIT " + TwitterWorker.LIMIT_FOLLOWERS
    );
    
    /** NOTE: The ?? is substituted into a string of repeated ?'s */
    public final SQLStmt getFollowerNames = new SQLStmt(
        "SELECT uid, name FROM user WHERE uid IN (??)", TwitterWorker.LIMIT_FOLLOWERS
    );
    
    public ResultSet run(Connection conn, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getFollowers);
        stmt.setLong(1, uid);
        ResultSet rs = stmt.executeQuery();
        
        stmt = this.getPreparedStatement(conn, getFollowerNames);
        int ctr = 0;
        long last = -1;
        while (rs.next()) {
            last = rs.getLong(1);
            stmt.setLong(ctr+1, last);
        } // WHILE
        if (ctr > 0) {
            while (ctr++ < TwitterWorker.LIMIT_FOLLOWERS) {
                stmt.setLong(ctr+1, last);
            } // WHILE
            return stmt.executeQuery();
        }
        LOG.info("No followers for user: "+uid);
        return (null);
    }

}
