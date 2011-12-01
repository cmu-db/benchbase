package com.oltpbenchmark.benchmarks.twitter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.api.ZipFianDistribution;
import com.oltpbenchmark.catalog.Table;

public class TwitterLoader extends Loader{
	
	private static final int USERS = 500; //Number of user baseline
	private static final int TWEETS = 20000;//Number of tweets baseline
	private static final int FOLLOW= 100;//Max follow per user
	
	private static final int NAME = 5;//Name length
	private static final int EXP_U = 1;//Exponent in the Zipfian distribution of users tweeting/followup.
	
	public final static int configCommitCount = 1000;

	private String insertUserSql="INSERT INTO usr (uid,name,email,followers) VALUES (?, ?, ?, 0)";
	
	private String insertTweetSql="INSERT INTO tweets VALUES (?, ?, ?, '"+LoaderUtil.getCurrentTime()+"')";
	
	private String insertFollowsSql="INSERT INTO follows VALUES (?, ?)";
	private String insertFollowersSql="INSERT INTO followers VALUES (?, ?)";

	private int scale=1;

	public TwitterLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
    	this.scale = (int) workConf.getScaleFactor();
    	this.scale=10;
	}

	@Override
	public void load() throws SQLException {
		// TODO Auto-generated method stub
		PreparedStatement userInsert = this.conn.prepareStatement(insertUserSql);
		int k=1;
		for(int i=0;i<USERS*scale;i++)
		{
			String name= LoaderUtil.randomStr(NAME);
			userInsert.setInt(1, i);
			userInsert.setString(2, name);
			userInsert.setString(3,name+"@tweeter.com");
			userInsert.addBatch();
			if ((k % configCommitCount) == 0) {
				userInsert.executeBatch();
				conn.commit();
				userInsert.clearBatch();
				System.out.println("Users %"+k);
			}
			k++;
		}
		conn.commit();
		System.out.println("\t Users Loaded");
		
		///////
		PreparedStatement tweetInsert = this.conn.prepareStatement(insertTweetSql);
		k=1;
		ZipFianDistribution zipf = new ZipFianDistribution(USERS*scale,1);
		for(int i=0;i<TWEETS*scale;i++)
		{
			int uid = zipf.next();
			tweetInsert.setInt(1, i);
			tweetInsert.setInt(2, uid);
			tweetInsert.setString(3,"some random text from tweeter"+uid);
			tweetInsert.addBatch();
			if ((k % configCommitCount) == 0) {
				tweetInsert.executeBatch();
				conn.commit();
				tweetInsert.clearBatch();
				System.out.println("tweet % "+k);
			}
			k++;
		}
		tweetInsert.executeBatch();
		conn.commit();
		tweetInsert.clearBatch();
		System.out.println("\t Tweets Loaded");
		
		
		//////
		PreparedStatement followsInsert = this.conn.prepareStatement(insertFollowsSql);
		PreparedStatement followersInsert = this.conn.prepareStatement(insertFollowersSql);
		k=1;
		Random random = new Random();
		ZipFianDistribution zipfFollowee = new ZipFianDistribution(USERS*scale,EXP_U);
		for(int follower=0;follower<USERS*scale;follower++)
		{
			List<Integer> followees=new ArrayList<Integer>();
			int time= random.nextInt(FOLLOW);
			for(int f=0;f<time;f++)
			{
				int followee = zipfFollowee.next();			
				if(follower!=followee && !followees.contains(followee))
				{
					followsInsert.setInt(1, follower);
					followsInsert.setInt(2, followee);
					followsInsert.addBatch();

					followersInsert.setInt(1, followee);
					followersInsert.setInt(2, follower);
					followersInsert.addBatch();
					
					followees.add(followee);
					
					if ((k % configCommitCount) == 0) {
						followsInsert.executeBatch();
						followersInsert.executeBatch();
						conn.commit();
						followsInsert.clearBatch();
						followersInsert.clearBatch();
						System.out.println("Follows  % "+k);
					}
					k++;
				}
			}
		}
		followsInsert.executeBatch();
		followersInsert.executeBatch();
		followsInsert.clearBatch();
		followersInsert.clearBatch();
		conn.commit();
		System.out.println("\t Follows Loaded");
	}

}
