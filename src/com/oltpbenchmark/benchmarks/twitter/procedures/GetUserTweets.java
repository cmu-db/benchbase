package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

public class GetUserTweets extends Procedure {

    public final SQLStmt getTweets = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS +
        " WHERE uid = ? LIMIT " + TwitterConstants.LIMIT_TWEETS_FOR_UID
    );
    
    public void run(Connection conn, long uid) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, getTweets);
        stmt.setLong(1, uid);
        ResultSet rs = stmt.executeQuery();
        rs.close();
    }
}
