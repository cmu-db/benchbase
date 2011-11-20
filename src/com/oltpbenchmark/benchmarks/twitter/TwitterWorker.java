/*******************************************************************************
 * oltpbenchmark.com
 *  
 *  Project Info:  http://oltpbenchmark.com
 *  Project Members:  	Carlo Curino <carlo.curino@gmail.com>
 * 				Evan Jones <ej@evanjones.ca>
 * 				DIFALLAH Djellel Eddine <djelleleddine.difallah@unifr.ch>
 * 				Andy Pavlo <pavlo@cs.brown.edu>
 * 				CUDRE-MAUROUX Philippe <philippe.cudre-mauroux@unifr.ch>  
 *  				Yang Zhang <yaaang@gmail.com> 
 * 
 *  This library is free software; you can redistribute it and/or modify it under the terms
 *  of the GNU General Public License as published by the Free Software Foundation;
 *  either version 3.0 of the License, or (at your option) any later version.
 * 
 *  This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU Lesser General Public License for more details.
 ******************************************************************************/
package com.oltpbenchmark.benchmarks.twitter;

import java.sql.SQLException;
import java.sql.Time;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweet;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweetsFromFollowing;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetUserTweets;
import com.oltpbenchmark.benchmarks.twitter.procedures.InsertTweet;

public class TwitterWorker extends Worker {
    private TransactionGenerator<TwitterOperation> generator;

    // TODO: make the next parameters of WorkLoadConfiguration
    public static int LIMIT_TWEETS = 100;
    public static int LIMIT_TWEETS_FOR_UID = 10;
    public static int LIMIT_FOLLOWERS = 20;

    public TwitterWorker(int id, TwitterBenchmark benchmarkModule, TransactionGenerator<TwitterOperation> generator) {
        super(id, benchmarkModule);
        this.generator = generator;
    }

    @Override
    protected TransactionType doWork(boolean measure, Phase phase) {
        TwitterOperation t = generator.nextTransaction();

        // TODO FIXME THIS NEEDS TO BE FIXED.. checking with Aubrey how we
        // generated ids before...
        String text = "Blah blah new tweet...";

        TransactionType retTP = TransactionType.INVALID;
        TransactionType nextTrans = transactionTypes.getType(phase.chooseTransaction());

        try {
            if (nextTrans.getProcedureClass().equals(GetTweet.class)) {
                doSelect1Tweet(t.tweetid);
            } else if (nextTrans.getProcedureClass().equals(GetTweetsFromFollowing.class)) {
                doSelectTweetsFromPplIFollow(t.uid);
            } else if (nextTrans.getProcedureClass().equals(GetFollowers.class)) {
                doSelectNamesOfPplThatFollowMe(t.uid);
            } else if (nextTrans.getProcedureClass().equals(GetUserTweets.class)) {
                doSelectTweetsForUid(t.uid);
            } else if (nextTrans.getProcedureClass().equals(InsertTweet.class)) {
                doInsertTweet(t.uid, text);
            }
            conn.commit();
            retTP = nextTrans;

        } catch (MySQLTransactionRollbackException m) {
            System.err.println("Rollback:" + m.getMessage());
        } catch (SQLException e) {
            System.err.println("Timeout:" + e.getMessage());
        }
        return retTP;
    }

    public void doSelect1Tweet(int tweet_id) throws SQLException {
        GetTweet proc = this.getProcedure(GetTweet.class);
        assert (proc != null);
        proc.run(conn, tweet_id);
    }

    public void doSelectTweetsFromPplIFollow(int uid) throws SQLException {
        GetTweetsFromFollowing proc = this.getProcedure(GetTweetsFromFollowing.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doSelectNamesOfPplThatFollowMe(int uid) throws SQLException {
        GetFollowers proc = this.getProcedure(GetFollowers.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doSelectTweetsForUid(int uid) throws SQLException {
        GetUserTweets proc = this.getProcedure(GetUserTweets.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doInsertTweet(int uid, String text) throws SQLException {
        InsertTweet proc = this.getProcedure(InsertTweet.class);
        assert (proc != null);
        Time time = new Time(System.currentTimeMillis());
        proc.run(conn, uid, text, time);
    }
}
