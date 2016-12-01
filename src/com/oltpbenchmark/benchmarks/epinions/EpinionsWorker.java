/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/


package com.oltpbenchmark.benchmarks.epinions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.log4j.Logger;
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

public class EpinionsWorker extends Worker<EpinionsBenchmark> {
	
	private static final Logger LOG = Logger.getLogger(EpinionsWorker.class);

    private ArrayList<String> user_ids;
    private ArrayList<String> item_ids;
    private final Random rand = new Random(System.currentTimeMillis());

    public EpinionsWorker(EpinionsBenchmark benchmarkModule, int id, ArrayList<String> user_ids, ArrayList<String> item_ids) {
        super(benchmarkModule, id);
        this.user_ids = user_ids;
        this.item_ids = item_ids;
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        
    	boolean successful = false;
		while (!successful) {
			try {
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
				successful = true;
			} catch (SQLException esql) {
				int error_code = esql.getErrorCode();
				if (error_code == 8177) {
					conn.rollback();
				} else {
					LOG.error("caught sql error in Epinions Benchmark for the procedure "
							+ nextTrans.getName() + ":" + esql);
				}
			} catch (Exception e) {
				LOG.error("caught Exceptions in Epinions for the procedure "
						+ nextTrans.getName() + ":" + e);
			}
		}

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
