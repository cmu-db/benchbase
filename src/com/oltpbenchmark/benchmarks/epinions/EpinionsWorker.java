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
package com.oltpbenchmark.benchmarks.epinions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import com.mysql.jdbc.exceptions.jdbc4.MySQLTransactionRollbackException;
import com.oltpbenchmark.Phase;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetAverageRatingByTrustedUser;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetItemAverageRating;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetItemReviewsByTrustedUser;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetReviewItemById;
import com.oltpbenchmark.benchmarks.epinions.procedures.GetReviewsByUser;
import com.oltpbenchmark.benchmarks.epinions.procedures.UpdateItemTitle;
import com.oltpbenchmark.benchmarks.epinions.procedures.UpdateReviewRating;
import com.oltpbenchmark.benchmarks.epinions.procedures.UpdateTrustRating;
import com.oltpbenchmark.benchmarks.epinions.procedures.UpdateUserName;

public class EpinionsWorker extends Worker {
    
    private final Random gen = new Random(1); // I change the random seed every time!

    private ResultSet rs = null;
    private ArrayList<String> user_ids;
    private ArrayList<String> item_ids;
    Random rand = new Random();

    
	public EpinionsWorker(int id, EpinionsBenchmark benchmarkModule, ArrayList<String> user_ids,ArrayList<String> item_ids) {
		super(id, benchmarkModule);
		this.user_ids=user_ids;
		this.item_ids=item_ids;
	}

	@Override
	protected TransactionType doWork(boolean measure, Phase phase) {

		//transactionTypes.getType("INVALID");
		TransactionType retTP = transactionTypes.getType("INVALID");
		
		if(phase!=null){
			int nextTrans = phase.chooseTransaction();
			
			try {
				
				if(nextTrans == transactionTypes.getType("ITEM_BY_ID").getId()){
					reviewItemByID();
					retTP = transactionTypes.getType("ITEM_BY_ID");
				}else
				if(nextTrans == transactionTypes.getType("ALL_REVIEWS_OF_A_USER").getId()){
					reviewsByUser();
					retTP = transactionTypes.getType("ALL_REVIEWS_OF_A_USER");
				}else
				if(nextTrans == transactionTypes.getType("AVG_RATING_BY_TRUSTED_REVIEWERS").getId()){
					averageRatingByTrustedUser();
					retTP = transactionTypes.getType("AVG_RATING_BY_TRUSTED_REVIEWERS");
				}else
				if(nextTrans == transactionTypes.getType("AVG_RATING_OF_ITEM").getId()){
					averageRatingOfItem();
					retTP = transactionTypes.getType("AVG_RATING_OF_ITEM");
				}else
				if(nextTrans == transactionTypes.getType("REVIEWS_BY_TRUSTED_USERS").getId()){
					itemReviewsByTrustedUser();
					retTP = transactionTypes.getType("REVIEWS_BY_TRUSTED_USERS");
				}else
				if(nextTrans == transactionTypes.getType("UPDATE_USER_NAME").getId()){
					updateUserName();
					retTP = transactionTypes.getType("UPDATE_USER_NAME");
				}else
				if(nextTrans == transactionTypes.getType("UPDATE_ITEM_TITLE").getId()){
					updateItemTitle();
						retTP = transactionTypes.getType("UPDATE_ITEM_TITLE");
				}else
				if(nextTrans == transactionTypes.getType("UPDATE_REVIEW_RATING").getId()){
					updateReviewRating();
						retTP = transactionTypes.getType("UPDATE_REVIEW_RATING");
				}
				if(nextTrans == transactionTypes.getType("UPDATE_TRUST_RATING").getId()){
					updateTrustRating();
						retTP = transactionTypes.getType("UPDATE_TRUST_RATING");
				}
				
				
				
				
			} catch (MySQLTransactionRollbackException m){
				System.err.println("Rollback:" + m.getMessage());
			} catch (SQLException e) {
				System.err.println("Timeout:" + e.getMessage());			
			}
		}
		return retTP;
	
		
	
	}

    public void reviewItemByID() throws SQLException {
        GetReviewItemById proc = (GetReviewItemById) this.getProcedure("GetReviewItemById");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        proc.run(conn, iid);
        conn.commit();
    }

    public void reviewsByUser() throws SQLException {
        GetReviewsByUser proc = (GetReviewsByUser) this.getProcedure("GetReviewsByUser");
        assert (proc != null);
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, uid);
        conn.commit();
    }

    public void averageRatingByTrustedUser() throws SQLException {
        GetAverageRatingByTrustedUser proc = (GetAverageRatingByTrustedUser) this.getProcedure("GetAverageRatingByTrustedUser");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
        conn.commit();
    }

    public void averageRatingOfItem() throws SQLException {
        GetItemAverageRating proc = (GetItemAverageRating) this.getProcedure("GetItemAverageRating");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        proc.run(conn, iid);
        conn.commit();
    }

    public void itemReviewsByTrustedUser() throws SQLException {
        GetItemReviewsByTrustedUser proc = (GetItemReviewsByTrustedUser) this.getProcedure("GetItemReviewsByTrustedUser");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
        conn.commit();
    }

    // ===================================== UPDATES
    // ===================================================

    public void updateUserName() throws SQLException {
        UpdateUserName proc = (UpdateUserName) this.getProcedure("UpdateUserName");
        assert (proc != null);
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        String name = "XXXXXXXXXXX"; // FIXME
        proc.run(conn, uid, name);
        conn.commit();
    }

    public void updateItemTitle() throws SQLException {
        UpdateItemTitle proc = (UpdateItemTitle) this.getProcedure("UpdateItemTitle");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        String title = "XXXXXXXXXXX"; // FIXME
        proc.run(conn, iid, title);
        conn.commit();
    }

    public void updateReviewRating() throws SQLException {
        UpdateReviewRating proc = (UpdateReviewRating) this.getProcedure("UpdateReviewRating");
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        int rating = rand.nextInt(1000); // ???
        proc.run(conn, iid, uid, rating);
        conn.commit();
    }

    public void updateTrustRating() throws SQLException {
        UpdateTrustRating proc = (UpdateTrustRating) this.getProcedure("UpdateTrustRating");
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        long uid2 = uid;
        while (uid2 == uid) {
            uid2 = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        } // WHILE
        int trust = rand.nextInt(100); // ???
        proc.run(conn, uid, uid2, trust);
        conn.commit();
    }

}
