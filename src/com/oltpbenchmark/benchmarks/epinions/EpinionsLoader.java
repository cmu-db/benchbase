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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

public class EpinionsLoader extends Loader<EpinionsBenchmark> {

    private static final Logger LOG = Logger.getLogger(EpinionsLoader.class);

    private final int num_users;
    private final int num_items;
    private final long num_reviews;
    private final int num_trust;

    public EpinionsLoader(EpinionsBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_users = (int) Math.round(EpinionsConstants.NUM_USERS * this.scaleFactor);
        this.num_items = (int) Math.round(EpinionsConstants.NUM_ITEMS * this.scaleFactor);
        this.num_reviews = (int) Math.round(EpinionsConstants.REVIEW * this.scaleFactor);
        this.num_trust = (int) Math.round(EpinionsConstants.TRUST * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# USERS:  " + this.num_users);
            LOG.debug("# ITEMS: " + this.num_items);
            LOG.debug("# Max of REVIEWS per item: " + this.num_reviews);
            LOG.debug("# Max of TRUSTS per user: " + this.num_trust);
        }
    }

    @Override
    public void load() throws SQLException {
        this.loadUsers();
        this.loadItems();
        this.loadReviews();
        this.loadTrust();
    }

    /**
     * @author Djellel Load num_users users.
     * @throws SQLException
     */
    private void loadUsers() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog("user");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);

        //
        int total = 0;
        int batch = 0;
        for (int i = 0; i < num_users; i++) {
            String name = TextGenerator.randomStr(rng(), EpinionsConstants.NAME_LENGTH);
            userInsert.setInt(1, i);
            userInsert.setString(2, name);
            userInsert.addBatch();
            total++;

            if ((++batch % EpinionsConstants.BATCH_SIZE) == 0) {
                userInsert.executeBatch();
                conn.commit();
                batch = 0;
                userInsert.clearBatch();
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
            }
        }
        if (batch > 0) {
            userInsert.executeBatch();
            conn.commit();
            userInsert.clearBatch();
        }
        userInsert.close();
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Users Loaded [%d]", total));
    }

    /**
     * @author Djellel Load num_items items.
     * @throws SQLException
     */
    private void loadItems() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog("item");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement itemInsert = this.conn.prepareStatement(sql);
        
        //
        int total = 0;
        int batch = 0;
        for (int i = 0; i < num_items; i++) {
            String title = TextGenerator.randomStr(rng(), EpinionsConstants.TITLE_LENGTH);
            itemInsert.setInt(1, i);
            itemInsert.setString(2, title);
            itemInsert.addBatch();
            total++;
            
            if ((++batch % EpinionsConstants.BATCH_SIZE) == 0) {
                itemInsert.executeBatch();
                conn.commit();
                batch = 0;
                itemInsert.clearBatch();
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Items %d / %d", total, num_items));
            }
        }
        if (batch > 0) {
            itemInsert.executeBatch();
            conn.commit();
            itemInsert.clearBatch();
        }
        itemInsert.close();
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Items Loaded [%d]", total));
    }

    /**
     * @author Djellel What's going on here?: For each item we Loaded, we are
     *         going to generate reviews The number of reviews per Item selected
     *         from num_reviews. Who gives the reviews is selected from
     *         num_users and added to reviewers list. Note: the selection is
     *         based on Zipfian distribution.
     * @throws SQLException
     */
    private void loadReviews() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog("review");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement reviewInsert = this.conn.prepareStatement(sql);
        
        //
        ZipfianGenerator numReviews = new ZipfianGenerator(num_reviews, 1.8);
        ZipfianGenerator reviewer = new ZipfianGenerator(num_users);
        int total = 0;
        int batch = 0;
        for (int i = 0; i < num_items; i++) {
            List<Integer> reviewers = new ArrayList<Integer>();
            int review_count = numReviews.nextInt();
            if (review_count == 0)
                review_count = 1; // make sure at least each item has a review
            for (int rc = 0; rc < review_count;) {
                int u_id = reviewer.nextInt();
                if (!reviewers.contains(u_id)) {
                    rc++;
                    reviewInsert.setInt(1, total);
                    reviewInsert.setInt(2, u_id);
                    reviewInsert.setInt(3, i);
                    reviewInsert.setInt(4, new Random().nextInt(5));// rating
                    reviewInsert.setNull(5, java.sql.Types.INTEGER);
                    reviewInsert.addBatch();
                    reviewers.add(u_id);
                    total++;
                    
                    if ((++batch % EpinionsConstants.BATCH_SIZE) == 0) {
                        reviewInsert.executeBatch();
                        conn.commit();
                        batch = 0;
                        reviewInsert.clearBatch();
                        if (LOG.isDebugEnabled())
                            if (LOG.isDebugEnabled())
                                LOG.debug("Reviewed items  % " + (int) (((double) i / (double) this.num_items) * 100));
                    }
                }
            }
        } // FOR
        if (batch > 0) {
            reviewInsert.executeBatch();
            conn.commit();
            reviewInsert.clearBatch();
        }
        reviewInsert.close();
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Reviews Loaded [%d]", total));
    }

    /**
     * @author Djellel What's going on here?: For each user, select a number
     *         num_trust of trust-feedbacks (given by others users). Then we
     *         select the users who are part of that list. The actual feedback
     *         can be 1/0 with uniform distribution. Note: Select is based on
     *         Zipfian distribution Trusted users are not correlated to heavy
     *         reviewers (drawn using a scrambled distribution)
     * @throws SQLException
     */
    public void loadTrust() throws SQLException {
        Table catalog_tbl = this.benchmark.getTableCatalog("trust");
        assert (catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement trustInsert = this.conn.prepareStatement(sql);
        
        //
        int total = 0;
        int batch = 0;
        ZipfianGenerator numTrust = new ZipfianGenerator(num_trust, 1.95);
        ScrambledZipfianGenerator reviewed = new ScrambledZipfianGenerator(num_users);
        Random isTrusted = new Random(System.currentTimeMillis());
        for (int i = 0; i < num_users; i++) {
            List<Integer> trusted = new ArrayList<Integer>();
            int trust_count = numTrust.nextInt();
            for (int tc = 0; tc < trust_count;) {
                int u_id = reviewed.nextInt();
                if (!trusted.contains(u_id)) {
                    tc++;
                    trustInsert.setInt(1, i);
                    trustInsert.setInt(2, u_id);
                    trustInsert.setInt(3, isTrusted.nextInt(2));
                    trustInsert.setDate(4, new java.sql.Date(System.currentTimeMillis()));
                    trustInsert.addBatch();
                    trusted.add(u_id);
                    total++;
                    
                    if ((++batch % EpinionsConstants.BATCH_SIZE) == 0) {
                        trustInsert.executeBatch();
                        conn.commit();
                        batch = 0;
                        trustInsert.clearBatch();
                        if (LOG.isDebugEnabled())
                            LOG.debug("Rated users  % " + (int) (((double) i / (double) this.num_users) * 100));

                    }
                }
            }
        } // FOR
        if (batch > 0) {
            trustInsert.executeBatch();
            conn.commit();
            trustInsert.clearBatch();
        }
        trustInsert.close();
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Trust Loaded [%d]", total));
    }
}
