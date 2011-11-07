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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;

public class TwitterWorker extends Worker {
	private final Statement st;
	private final Random r;
    private TransactionGenerator<TwitterOperation> generator;
    private Random gen = new Random();
    
    //TODO: make the next parameters of WorkLoadConfiguration
    public static int LIMIT_TWEETS = 100;
    public static int LIMIT_TWEETS_FOR_UID = 10;
    public static int LIMIT_FOLLOWERS = 20;
    
	public TwitterWorker(Connection conn, WorkLoadConfiguration wrkld, TransactionGenerator<TwitterOperation> generator) {
		super(conn, wrkld);
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

		
		TransactionType retTP = transTypes.getType("INVALID");
		TwitterOperation t= generator.nextTransaction();
		
		
		
		
		//TODO FIXME THIS NEEDS TO BE FIXED.. checking with Aubrey how we generated ids before...
		String text = "Blah blah new tweet..."; 
			
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				if(nextTrans == transTypes.getType("TWITTER_SELECT1_TWEET_BY_TWEETID").getId()){
					doSelect1Tweet(t.tweetid);
					retTP = transTypes.getType("TWITTER_SELECT1_TWEET_BY_TWEETID");
				}else
				if(nextTrans == transTypes.getType("TWITTER_SELECT_TWEETS_I_FOLLOW").getId()){
					doSelectTweetsFromPplIFollow(t.uid);
					retTP = transTypes.getType("TWITTER_SELECT_TWEETS_I_FOLLOW");
				}else
				if(nextTrans == transTypes.getType("TWITTER_SELECT_FOLLOWERS").getId()){
					doSelectNamesOfPplThatFollowMe(t.uid);
					retTP = transTypes.getType("TWITTER_SELECT_FOLLOWERS");
				}else
				if(nextTrans == transTypes.getType("TWITTER_SELECT_TWEETS_BY_USERID").getId()){
					doSelectTweetsForUid(t.uid);
					retTP = transTypes.getType("TWITTER_SELECT_TWEETS_BY_USERID");
				}else
				if(nextTrans == transTypes.getType("TWITTER_INSERT1_TWEET").getId()){
					doInsertTweet(t.uid,text);
					retTP = transTypes.getType("TWITTER_INSERT1_TWEET");
				}
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				System.err.println("Timeout:" + e.getMessage());			
			}
		}
		return retTP;
	
		
	
	}

	
	public void doSelect1Tweet(int tweet_id) throws SQLException{
	    // this is autocommit
	    String query = "select * from tweets where id = "+ tweet_id;
	    Statement st = conn.createStatement();
	    ResultSet rs = st.executeQuery(query);
	    conn.commit();
	  }


	  public void doSelectTweetsFromPplIFollow(int uid) throws SQLException{
	    String query1 = "select f2 from follows where f1 =" + uid;
	    System.out.println(query1);
	    Statement st = conn.createStatement();
	    ResultSet rs = st.executeQuery(query1);
	    String query2 = "select * from tweets where uid IN(";
	    ArrayList<String> ids = new ArrayList<String>();
	    while(rs.next()){
	      ids.add(rs.getString(1));
	    }
	    int sz = ids.size();
	    if(sz>0){
	      for(int i = 0; i < sz; i++){
	        query2+=ids.get(i);
	        if(i == (sz-1)){
	          query2+=") LIMIT " + LIMIT_TWEETS;
	        }else{
	          query2+= ", ";
	        }
	      }

	      System.out.println(query2);
	      rs = st.executeQuery(query2);
	    }else{
	      System.out.println("doesnt follow anyone");
	    }
	    
	    conn.commit();
	  }
	  
	  public void doSelectNamesOfPplThatFollowMe(int uid) throws SQLException{
	    String query1 = "select f2 from followers where f1 =" + uid;
	    System.out.println(query1);
	    Statement st = conn.createStatement();
	    ResultSet rs = st.executeQuery(query1);
//	    String query2 = "select * from user where uid IN(";
	    String query2 = "select name from user where uid IN(";

	    ArrayList<String> ids = new ArrayList<String>();
	    while(rs.next()){
	      ids.add(rs.getString(1));
	    }
	    int sz = ids.size();
	    if(sz>0){
	      for(int i = 0; i < sz; i++){
	        query2+=ids.get(i);
	        if(i == (sz-1)){
	          query2+=") LIMIT " + LIMIT_FOLLOWERS;
	        }else{
	          query2+= ", ";
	        }
	      }
	      System.out.println(query2);
	      rs = st.executeQuery(query2);
	    }else{
	      System.out.println("doesnt have followers");
	    }

	    conn.commit();
	  }
	  
	  public void doSelectTweetsForUid(int uid) throws SQLException{
	    String query = "select * from tweets where uid = "+ uid + " LIMIT " + LIMIT_TWEETS_FOR_UID;
	    Statement st = conn.createStatement();
	    ResultSet rs = st.executeQuery(query);
	    conn.commit();
	  }

	  public void doInsertTweet(int uid, String text) throws SQLException{
	    String query = "INSERT INTO added_tweets VALUES (null,"+uid+",'"+text+"',now())";
	    Statement st = conn.createStatement();
	    int rs = st.executeUpdate(query);
	    conn.commit();
	  }
	

}
