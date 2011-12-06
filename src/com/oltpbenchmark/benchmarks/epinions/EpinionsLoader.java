package com.oltpbenchmark.benchmarks.epinions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.yahoo.ycsb.generator.ZipfianGenerator;

public class EpinionsLoader extends Loader{
	
    public String insertUserSql = "INSERT INTO usr VALUES (?,?)";

	public String insertItemSql = "INSERT INTO item VALUES (?,?)";
	
	public String insertReviewSql = "INSERT INTO review VALUES (?,?,?,NULL,NULL)";
	
	public String insertTrustSql = "INSERT INTO trust VALUES (?,?,?,now())";
	
	private final int ITEMS=1000; // Number of baseline pages
	private static final long TITLE = 20;
	
	private final int USERS=2000; // Number of baseline Users
	private final int NAME=5; // Length of user's name
	
	private int scale=1; //Scale factor
	public final static int configCommitCount = 1000;

	private static final int REVIEW = 20; // this is the average .. expand to max

	private static final int TRUST = 10; // this is the average .. expand to max

	public EpinionsLoader(Connection c, WorkloadConfiguration workConf,
			Map<String, Table> tables) {
		super(c, workConf, tables);
    	this.scale = (int) workConf.getScaleFactor();
	}

	@Override
	public void load() {
		System.out.println(LoaderUtil.getCurrentTime14());
		try 
		{
			
			PreparedStatement userInsert = this.conn.prepareStatement(insertUserSql);
			int k=1;
			for(int i=0;i<USERS*scale;i++)
			{
				String name= LoaderUtil.randomStr(NAME);
				userInsert.setInt(1, i);
				userInsert.setString(2,name);
				userInsert.addBatch();
				if ((k % configCommitCount) == 0) {
					userInsert.executeBatch();
					conn.commit();
					userInsert.clearBatch();
					System.out.println("users"+k);
				}
				k++;
			}
			userInsert.executeBatch();
			conn.commit();
			userInsert.clearBatch();
			System.out.println("\t Users Loaded");
			
			PreparedStatement itemInsert = this.conn.prepareStatement(insertItemSql);
			k=1;
			for(int i=0;i<ITEMS*scale;i++)
			{
				String title=LoaderUtil.randomStr(TITLE);
				itemInsert.setInt(1, i);
				itemInsert.setString(2,title);
				itemInsert.addBatch();				
				if ((k % configCommitCount) == 0) {
					itemInsert.executeBatch();
					conn.commit();
					itemInsert.clearBatch();
					System.out.println("page "+k);
				}
				k++;
			}
			itemInsert.executeBatch();
			conn.commit();
			itemInsert.clearBatch();
			System.out.println("\t Items Loaded");
			
			PreparedStatement reviewInsert = this.conn.prepareStatement(insertReviewSql);
			ZipfianGenerator numReviews=new ZipfianGenerator(REVIEW*scale);
			ZipfianGenerator reviewer=new ZipfianGenerator(USERS*scale);
			k=1;
			for(int i=0;i<ITEMS*scale;i++)
			{
				List<Integer> reviewers=new ArrayList<Integer>();
				int time= numReviews.nextInt();
				for(int f=0;f<time;f++)
				{
					int u_id= reviewer.nextInt();
					if(!reviewers.contains(u_id))
					{
						reviewInsert.setInt(1, k);
						reviewInsert.setInt(2, u_id);
						reviewInsert.setInt(3, i);
						reviewInsert.addBatch();
						reviewers.add(u_id);
						if ((k % configCommitCount) == 0) {
							reviewInsert.executeBatch();
							conn.commit();
							reviewInsert.clearBatch();
							System.out.println("review "+k);
						}
						k++;
					}
				}
			}
			reviewInsert.executeBatch();
			conn.commit();
			reviewInsert.clearBatch();
			System.out.println("\t Reviews Loaded");
			
			PreparedStatement trustInsert = this.conn.prepareStatement(insertTrustSql);
			k=1;
			ZipfianGenerator numTrust=new ZipfianGenerator(TRUST*scale);
			for(int i=0;i<USERS*scale;i++)
			{
				List<Integer> trustee=new ArrayList<Integer>();
				int time= numTrust.nextInt();
				for(int f=0;f<time;f++)
				{
					int u_id= reviewer.nextInt();
					if(!trustee.contains(u_id))
					{
						trustInsert.setInt(1, i);
						trustInsert.setInt(2, u_id);
						trustInsert.setInt(3, 1);
						trustInsert.addBatch();
						trustee.add(u_id);
						if ((k % configCommitCount) == 0) {
							trustInsert.executeBatch();
							conn.commit();
							trustInsert.clearBatch();
							System.out.println("trust "+k);
						}
						k++;
					}
				}
			}
			trustInsert.executeBatch();
			conn.commit();
			trustInsert.clearBatch();
			System.out.println("\t Trust Loaded");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 		
	}
}
