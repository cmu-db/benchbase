package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;

public class GetTweet extends Procedure {

    public SQLStmt getTweet = new SQLStmt(
        "SELECT * FROM tweets WHERE id = ?"
    );

    public ResultSet run(Connection conn, long tweet_id) throws SQLException{
        PreparedStatement stmt = this.getPreparedStatement(conn, getTweet);
        stmt.setLong(1, tweet_id);
        return (stmt.executeQuery());
    }
}
