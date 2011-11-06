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
package com.oltpbenchmark.epinions;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.TransactionType;
import com.oltpbenchmark.TransactionTypes;
import com.oltpbenchmark.WorkLoadConfiguration;
import com.oltpbenchmark.Worker;
import com.oltpbenchmark.tpcc.jTPCCConfig;

public class EpinionsWorker extends Worker {
	private final Connection conn;
	private final Statement st;
	private final Random r;
    private final TransactionTypes transTypes;


    // CPU-bound Txn
    private PreparedStatement cpu1PS = null;
    private PreparedStatement cpu2PS = null;
    
    // IO-bound Txn
    private PreparedStatement io1PS = null;
    private PreparedStatement io2PS = null;
    
    // Contention-bound Txn
    private PreparedStatement lock1PSupdate = null;
    private PreparedStatement lock1PSselect = null;
    private PreparedStatement lock2PSupdate = null;
    private PreparedStatement lock2PSselect = null;
    
    private final Random gen = new Random(1); // I change the random seed every time!

    private int result = 0;
    private ResultSet rs = null;
    private int terminalUniqueId = -1;
    private WorkLoadConfiguration wrkld;
    private ArrayList<String> user_ids;
    private ArrayList<String> item_ids;
    Random rand = new Random();

    
	public EpinionsWorker(Connection conn, int terminalUniqueId, WorkLoadConfiguration wrkld,ArrayList<String> user_ids,ArrayList<String> item_ids) {
		
		this.user_ids=user_ids;
		this.item_ids=item_ids;
		this.wrkld = wrkld;
		this.terminalUniqueId =terminalUniqueId;
		this.transTypes=wrkld.getTransTypes();
		this.conn = conn;
		r = new Random();
	
		try {
			st = conn.createStatement();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {

		transTypes.getType("INVALID");
		TransactionType retTP = transTypes.getType("INVALID");
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				
				if(nextTrans == transTypes.getType("ITEM_BY_ID").getId()){
					reviewItemByID();
					retTP = transTypes.getType("ITEM_BY_ID");
				}else
				if(nextTrans == transTypes.getType("ALL_REVIEWS_OF_A_USER").getId()){
					reviewsByUser();
					retTP = transTypes.getType("ALL_REVIEWS_OF_A_USER");
				}else
				if(nextTrans == transTypes.getType("AVG_RATING_BY_TRUSTED_REVIEWERS").getId()){
					averageRatingByTrustedUser();
					retTP = transTypes.getType("AVG_RATING_BY_TRUSTED_REVIEWERS");
				}else
				if(nextTrans == transTypes.getType("AVG_RATING_OF_ITEM").getId()){
					averageRatingOfItem();
					retTP = transTypes.getType("AVG_RATING_OF_ITEM");
				}else
				if(nextTrans == transTypes.getType("REVIEWS_BY_TRUSTED_USERS").getId()){
					itemReviewsByTrustedUser();
					retTP = transTypes.getType("REVIEWS_BY_TRUSTED_USERS");
				}else
				if(nextTrans == transTypes.getType("UPDATE_USER_NAME").getId()){
					updateUserName();
					retTP = transTypes.getType("UPDATE_USER_NAME");
				}else
				if(nextTrans == transTypes.getType("UPDATE_ITEM_TITLE").getId()){
					updateItemTitle();
						retTP = transTypes.getType("UPDATE_ITEM_TITLE");
				}else
				if(nextTrans == transTypes.getType("UPDATE_REVIEW_RATING").getId()){
					updateReviewRating();
						retTP = transTypes.getType("UPDATE_REVIEW_RATING");
				}
				if(nextTrans == transTypes.getType("UPDATE_TRUST_RATING").getId()){
					updateTrustRating();
						retTP = transTypes.getType("UPDATE_TRUST_RATING");
				}
				
				
				
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				System.err.println("Timeout:" + e.getMessage());			
			}
		}
		return retTP;
	
		
	
	}

	
	public void reviewItemByID() throws SQLException{
	      String iid = item_ids.get(rand.nextInt(item_ids.size()));
	      String sql = ("SELECT * FROM review r, item i WHERE i.i_id = r.i_id and r.i_id="
	          + iid + " ORDER BY rating LIMIT 10;");
	      st.execute(sql);
	      conn.commit();
	}
	

	public void reviewsByUser() throws SQLException{
		  String uid = user_ids.get(rand.nextInt(user_ids.size()));
	      String sql = ("SELECT * FROM review r, user u WHERE u.u_id = r.u_id AND r.u_id="
	          + uid + " ORDER BY rating LIMIT 10;");
	      st.execute(sql);
	      conn.commit();
    }

	
	public void averageRatingByTrustedUser() throws SQLException{
	      String uid = user_ids.get(rand.nextInt(user_ids.size()));
	      String iid = item_ids.get(rand.nextInt(item_ids.size()));
	      String sql = ("SELECT avg(rating) FROM review r, trust t WHERE r.i_id=" + iid
	          + " AND r.u_id=t.target_u_id AND t.source_u_id=" + uid + ";");
	      st.execute(sql);
	      conn.commit();
    }

	
	public void averageRatingOfItem() throws SQLException{
	      String iid = item_ids.get(rand.nextInt(item_ids.size()));
	      String sql = ("SELECT avg(rating) FROM review r WHERE r.i_id=" + iid + ";");
	      st.execute(sql);
	      conn.commit();
    }

	public void itemReviewsByTrustedUser() throws SQLException{
	      String uid = user_ids.get(rand.nextInt(user_ids.size()));
	      String iid = item_ids.get(rand.nextInt(item_ids.size()));
	      String sql = ("SELECT * FROM review r WHERE r.i_id=" + iid + ";");
	      st.execute(sql);
	      sql = ("SELECT * FROM trust t WHERE t.source_u_id=" + uid + ";");
	      st.execute(sql);
	      conn.commit();
    }

    // ===================================== UPDATES
    // ===================================================

	public void updateUserName() throws SQLException{
	      String uid = user_ids.get(rand.nextInt(user_ids.size()));
	      String sql = ("UPDATE user SET name = name WHERE u_id=" + uid + ";"); //FIXME this has no effect on DB... need to change
	      st.execute(sql);
	      conn.commit();
    }

	
	public void updateItemTitle() throws SQLException{
	      String iid = item_ids.get(rand.nextInt(item_ids.size()));
	      String sql = ("UPDATE item SET title = title WHERE i_id=" + iid + ";");
	      st.execute(sql);
	      conn.commit();
    }

	
	public void updateReviewRating() throws SQLException{
      String iid = item_ids.get(rand.nextInt(item_ids.size()));
      String uid = user_ids.get(rand.nextInt(user_ids.size()));
      String sql = ("UPDATE review SET rating = rating WHERE i_id=" + iid
          + " AND u_id=" + uid + ";");
      st.execute(sql);
      conn.commit();
    }

	
	public void updateTrustRating() throws SQLException{
      String uid = user_ids.get(rand.nextInt(user_ids.size()));
      String uid2 = user_ids.get(rand.nextInt(user_ids.size()));
      String sql = ("UPDATE trust SET trust = trust WHERE source_u_id=" + uid
          + " AND target_u_id=" + uid2 + ";");
      st.execute(sql);
      conn.commit();
	}
	


}
