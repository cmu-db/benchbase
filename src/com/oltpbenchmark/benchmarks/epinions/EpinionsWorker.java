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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import com.oltpbenchmark.api.Procedure.UserAbortException;
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
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;

public class EpinionsWorker extends Worker {

    private ArrayList<String> user_ids;
    private ArrayList<String> item_ids;
    private final Random rand = new Random(System.currentTimeMillis());

    public EpinionsWorker(int id, EpinionsBenchmark benchmarkModule, ArrayList<String> user_ids, ArrayList<String> item_ids) {
        super(benchmarkModule, id);
        this.user_ids = user_ids;
        this.item_ids = item_ids;
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(GetReviewItemById.class)) {
            reviewItemByID();
        } else if (nextTrans.getProcedureClass().equals(GetReviewsByUser.class)) {
            reviewsByUser();
        } else if (nextTrans.getProcedureClass().equals(GetAverageRatingByTrustedUser.class)) {
            averageRatingByTrustedUser();
        } else if (nextTrans.getProcedureClass().equals(GetItemAverageRating.class)) {
            averageRatingOfItem();
        } else if (nextTrans.getProcedureClass().equals(GetItemReviewsByTrustedUser.class)) {
            itemReviewsByTrustedUser();
        } else if (nextTrans.getProcedureClass().equals(UpdateUserName.class)) {
            updateUserName();
        } else if (nextTrans.getProcedureClass().equals(UpdateItemTitle.class)) {
            updateItemTitle();
        } else if (nextTrans.getProcedureClass().equals(UpdateReviewRating.class)) {
            updateReviewRating();
        } else if (nextTrans.getProcedureClass().equals(UpdateTrustRating.class)) {
            updateTrustRating();
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    public void reviewItemByID() throws SQLException {
        GetReviewItemById proc = this.getProcedure(GetReviewItemById.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        proc.run(conn, iid);
    }

    public void reviewsByUser() throws SQLException {
        GetReviewsByUser proc = this.getProcedure(GetReviewsByUser.class);
        assert (proc != null);
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, uid);
    }

    public void averageRatingByTrustedUser() throws SQLException {
        GetAverageRatingByTrustedUser proc = this.getProcedure(GetAverageRatingByTrustedUser.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
    }

    public void averageRatingOfItem() throws SQLException {
        GetItemAverageRating proc = this.getProcedure(GetItemAverageRating.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        proc.run(conn, iid);
    }

    public void itemReviewsByTrustedUser() throws SQLException {
        GetItemReviewsByTrustedUser proc = this.getProcedure(GetItemReviewsByTrustedUser.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
    }

    public void updateUserName() throws SQLException {
        UpdateUserName proc = this.getProcedure(UpdateUserName.class);
        assert (proc != null);
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        String name = TextGenerator.randomStr(rng(), EpinionsConstants.NAME_LENGTH); // FIXME
        proc.run(conn, uid, name);
    }

    public void updateItemTitle() throws SQLException {
        UpdateItemTitle proc = this.getProcedure(UpdateItemTitle.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        String title = TextGenerator.randomStr(rng(), EpinionsConstants.TITLE_LENGTH); // FIXME
        proc.run(conn, iid, title);
    }

    public void updateReviewRating() throws SQLException {
        UpdateReviewRating proc = this.getProcedure(UpdateReviewRating.class);
        assert (proc != null);
        long iid = Long.valueOf(item_ids.get(rand.nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rand.nextInt(user_ids.size())));
        int rating = rand.nextInt(1000); // ???
        proc.run(conn, iid, uid, rating);
    }

    public void updateTrustRating() throws SQLException {
        UpdateTrustRating proc = this.getProcedure(UpdateTrustRating.class);
        int uix= rand.nextInt(user_ids.size());
        int uix2= rand.nextInt(user_ids.size());
        long uid = Long.valueOf(user_ids.get(uix));
        long uid2 = Long.valueOf(user_ids.get(uix2));
        int trust = rand.nextInt(2);
        proc.run(conn, uid, uid2, trust);
    }

}
