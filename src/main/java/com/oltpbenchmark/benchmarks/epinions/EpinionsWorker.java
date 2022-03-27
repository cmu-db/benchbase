/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.epinions;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.epinions.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.TextGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

public class EpinionsWorker extends Worker<EpinionsBenchmark> {

    private static final Logger LOG = LoggerFactory.getLogger(EpinionsWorker.class);

    private final ArrayList<String> user_ids;
    private final ArrayList<String> item_ids;
    private final Random rand = new Random(System.currentTimeMillis());

    public EpinionsWorker(EpinionsBenchmark benchmarkModule, int id, ArrayList<String> user_ids, ArrayList<String> item_ids) {
        super(benchmarkModule, id);
        this.user_ids = user_ids;
        this.item_ids = item_ids;
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        if (nextTrans.getProcedureClass().equals(GetReviewItemById.class)) {
            reviewItemByID(conn);
        } else if (nextTrans.getProcedureClass().equals(GetReviewsByUser.class)) {
            reviewsByUser(conn);
        } else if (nextTrans.getProcedureClass().equals(GetAverageRatingByTrustedUser.class)) {
            averageRatingByTrustedUser(conn);
        } else if (nextTrans.getProcedureClass().equals(GetItemAverageRating.class)) {
            averageRatingOfItem(conn);
        } else if (nextTrans.getProcedureClass().equals(GetItemReviewsByTrustedUser.class)) {
            itemReviewsByTrustedUser(conn);
        } else if (nextTrans.getProcedureClass().equals(UpdateUserName.class)) {
            updateUserName(conn);
        } else if (nextTrans.getProcedureClass().equals(UpdateItemTitle.class)) {
            updateItemTitle(conn);
        } else if (nextTrans.getProcedureClass().equals(UpdateReviewRating.class)) {
            updateReviewRating(conn);
        } else if (nextTrans.getProcedureClass().equals(UpdateTrustRating.class)) {
            updateTrustRating(conn);
        }
        return (TransactionStatus.SUCCESS);
    }

    public void reviewItemByID(Connection conn) throws SQLException {
        GetReviewItemById proc = this.getProcedure(GetReviewItemById.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        proc.run(conn, iid);
    }

    public void reviewsByUser(Connection conn) throws SQLException {
        GetReviewsByUser proc = this.getProcedure(GetReviewsByUser.class);

        long uid = Long.valueOf(user_ids.get(rng().nextInt(user_ids.size())));
        proc.run(conn, uid);
    }

    public void averageRatingByTrustedUser(Connection conn) throws SQLException {
        GetAverageRatingByTrustedUser proc = this.getProcedure(GetAverageRatingByTrustedUser.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rng().nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
    }

    public void averageRatingOfItem(Connection conn) throws SQLException {
        GetItemAverageRating proc = this.getProcedure(GetItemAverageRating.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        proc.run(conn, iid);
    }

    public void itemReviewsByTrustedUser(Connection conn) throws SQLException {
        GetItemReviewsByTrustedUser proc = this.getProcedure(GetItemReviewsByTrustedUser.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rng().nextInt(user_ids.size())));
        proc.run(conn, iid, uid);
    }

    public void updateUserName(Connection conn) throws SQLException {
        UpdateUserName proc = this.getProcedure(UpdateUserName.class);

        long uid = Long.valueOf(user_ids.get(rng().nextInt(user_ids.size())));
        String name = TextGenerator.randomStr(rng(), EpinionsConstants.NAME_LENGTH); // FIXME
        proc.run(conn, uid, name);
    }

    public void updateItemTitle(Connection conn) throws SQLException {
        UpdateItemTitle proc = this.getProcedure(UpdateItemTitle.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        String title = TextGenerator.randomStr(rng(), EpinionsConstants.TITLE_LENGTH); // FIXME
        proc.run(conn, iid, title);
    }

    public void updateReviewRating(Connection conn) throws SQLException {
        UpdateReviewRating proc = this.getProcedure(UpdateReviewRating.class);

        long iid = Long.valueOf(item_ids.get(rng().nextInt(item_ids.size())));
        long uid = Long.valueOf(user_ids.get(rng().nextInt(user_ids.size())));
        int rating = rng().nextInt(1000); // ???
        proc.run(conn, iid, uid, rating);
    }

    public void updateTrustRating(Connection conn) throws SQLException {
        UpdateTrustRating proc = this.getProcedure(UpdateTrustRating.class);
        int uix = rng().nextInt(user_ids.size());
        int uix2 = rng().nextInt(user_ids.size());
        long uid = Long.valueOf(user_ids.get(uix));
        long uid2 = Long.valueOf(user_ids.get(uix2));
        int trust = rng().nextInt(2);
        proc.run(conn, uid, uid2, trust);
    }

}
