package com.oltpbenchmark.benchmarks.twitter.procedures;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.twitter.TwitterConstants;

public class InsertTweet extends Procedure {

	//FIXME: Carlo is this correct? 1) added_tweets is empty initially 2) id is supposed to be not null
    public final SQLStmt insertTweet = new SQLStmt(
        "INSERT INTO " + TwitterConstants.TABLENAME_ADDED_TWEETS + 
        " (uid,text,createdate) VALUES (?, ?, ?)"
    );
    
    public boolean run(Connection conn, long uid, String text, Time time) throws SQLException {
        PreparedStatement stmt = this.getPreparedStatement(conn, insertTweet);
        stmt.setLong(1, uid);
        stmt.setString(2, text);
        stmt.setDate(3,new java.sql.Date(System.currentTimeMillis()));
        return (stmt.execute());
    }
}
