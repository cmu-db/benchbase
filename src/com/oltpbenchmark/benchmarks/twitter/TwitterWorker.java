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
import java.sql.Statement;
import java.sql.Time;
import java.util.Random;

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
	private final Statement st;
	private final Random r;
    private TransactionGenerator<TwitterOperation> generator;
    private Random gen = new Random();
    
    //TODO: make the next parameters of WorkLoadConfiguration
    public static int LIMIT_TWEETS = 100;
    public static int LIMIT_TWEETS_FOR_UID = 10;
    public static int LIMIT_FOLLOWERS = 20;
    
	public TwitterWorker(int id, TwitterBenchmark benchmarkModule, TransactionGenerator<TwitterOperation> generator) {
		super(id, benchmarkModule);
		this.generator=generator;
		r = new Random();
	
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {

		
		TransactionType retTP = transactionTypes.getType("INVALID");
		TwitterOperation t= generator.nextTransaction();
		
		
		
		
		//TODO FIXME THIS NEEDS TO BE FIXED.. checking with Aubrey how we generated ids before...
		String text = "Blah blah new tweet..."; 
			
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				if(nextTrans == transactionTypes.getType("TWITTER_SELECT1_TWEET_BY_TWEETID").getId()){
					doSelect1Tweet(t.tweetid);
					retTP = transactionTypes.getType("TWITTER_SELECT1_TWEET_BY_TWEETID");
				}else
				if(nextTrans == transactionTypes.getType("TWITTER_SELECT_TWEETS_I_FOLLOW").getId()){
					doSelectTweetsFromPplIFollow(t.uid);
					retTP = transactionTypes.getType("TWITTER_SELECT_TWEETS_I_FOLLOW");
				}else
				if(nextTrans == transactionTypes.getType("TWITTER_SELECT_FOLLOWERS").getId()){
					doSelectNamesOfPplThatFollowMe(t.uid);
					retTP = transactionTypes.getType("TWITTER_SELECT_FOLLOWERS");
				}else
				if(nextTrans == transactionTypes.getType("TWITTER_SELECT_TWEETS_BY_USERID").getId()){
					doSelectTweetsForUid(t.uid);
					retTP = transactionTypes.getType("TWITTER_SELECT_TWEETS_BY_USERID");
				}else
				if(nextTrans == transactionTypes.getType("TWITTER_INSERT1_TWEET").getId()){
					doInsertTweet(t.uid,text);
					retTP = transactionTypes.getType("TWITTER_INSERT1_TWEET");
				}
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				System.err.println("Timeout:" + e.getMessage());			
			}
		}
		return retTP;
	}

	
	public void doSelect1Tweet(int tweet_id) throws SQLException {
	    GetTweet proc = (GetTweet)this.benchmarkModule.getProcedure("GetTweet");
	    assert(proc != null);
	    proc.run(conn, tweet_id);
	    conn.commit();
	}

	public void doSelectTweetsFromPplIFollow(int uid) throws SQLException{
	    GetTweetsFromFollowing proc = (GetTweetsFromFollowing)this.benchmarkModule.getProcedure("GetTweetsFromFollowing");
	    assert(proc != null);
        proc.run(conn, uid);
	    conn.commit();
	}
	  
	public void doSelectNamesOfPplThatFollowMe(int uid) throws SQLException{
	    GetFollowers proc = (GetFollowers)this.benchmarkModule.getProcedure("GetFollowers");
        assert(proc != null);
        proc.run(conn, uid);
	    conn.commit();
	}
	  
	public void doSelectTweetsForUid(int uid) throws SQLException{
	    GetUserTweets proc = (GetUserTweets)this.benchmarkModule.getProcedure("GetUserTweets");
        assert(proc != null);
        proc.run(conn, uid);
	    conn.commit();
    }

	public void doInsertTweet(int uid, String text) throws SQLException{
	    InsertTweet proc = (InsertTweet)this.benchmarkModule.getProcedure("InsertTweet");
        assert(proc != null);
        Time time = new Time(System.currentTimeMillis());
        proc.run(conn, uid, text, time);
        conn.commit();
	}
}
