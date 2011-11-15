package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterWorker;

public class GetTweetsFromFollowing extends Procedure {

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
        
        stmt = this.getPreparedStatement(conn, getFollowing);
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
        System.out.println("doesnt follow anyone");
        return (null);
    }
    
}
