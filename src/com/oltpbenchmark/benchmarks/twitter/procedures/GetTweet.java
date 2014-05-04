package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

public class GetTweet extends Procedure {

    public SQLStmt getTweet = new SQLStmt(
        "SELECT * FROM " + TwitterConstants.TABLENAME_TWEETS + " WHERE id = ?"
    );

    public void run(Connection conn, long tweet_id) throws SQLException{
        PreparedStatement stmt = this.getPreparedStatement(conn, getTweet);
        stmt.setLong(1, tweet_id);
        ResultSet rs = stmt.executeQuery();
        rs.close();
    }
}
