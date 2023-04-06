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

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class EpinionsLoader extends Loader<EpinionsBenchmark> {

    private final int num_users;
    private final int num_items;
    private final long num_reviews;
    private final int num_trust;

    public EpinionsLoader(EpinionsBenchmark benchmark) {
        super(benchmark);
        this.num_users = (int) Math.round(EpinionsConstants.NUM_USERS * this.scaleFactor);
        this.num_items = (int) Math.round(EpinionsConstants.NUM_ITEMS * this.scaleFactor);
        this.num_reviews = (int) Math.round(EpinionsConstants.REVIEW * this.scaleFactor);
        this.num_trust = (int) Math.round(EpinionsConstants.TRUST * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# USERS:  {}", this.num_users);
            LOG.debug("# ITEMS: {}", this.num_items);
            LOG.debug("# Max of REVIEWS per item: {}", this.num_reviews);
            LOG.debug("# Max of TRUSTS per user: {}", this.num_trust);
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        final int numToLoad = this.num_items + this.num_users;
        final int loadPerThread = Math.max(numToLoad / numLoaders, 1);
        final int numUserThreads = (int) Math.ceil((double) this.num_users / loadPerThread);
        final int numItemThreads = (int) Math.ceil((double) this.num_items / loadPerThread);

        final CountDownLatch userLatch = new CountDownLatch(numUserThreads);
        final CountDownLatch itemLatch = new CountDownLatch(numItemThreads);

        // USERACCT
        for (int i = 0; i < numUserThreads; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.num_users, (i + 1) * loadPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadUsers(conn, lo, hi);

                }

                @Override
                public void afterLoad() {
                    userLatch.countDown();
                }
            });
        }

        // ITEM
        for (int i = 0; i < numItemThreads; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.num_items, (i + 1) * loadPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadItems(conn, lo, hi);

                }

                @Override
                public void afterLoad() {
                    itemLatch.countDown();
                }
            });
        }

        // TRUST depends on USERACCT
        for (int i = 0; i < numItemThreads; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.num_users, (i + 1) * loadPerThread);

            // To ensure that we don't wind up with an empty trust table for
            // small scales we have the first loader thread ensure that at
            // least a single trust record is inserted.
            final boolean firstLoader = (i == 0);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadTrust(conn, lo, hi, firstLoader);
                }

                @Override
                public void beforeLoad() {
                    try {
                        userLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        // REVIEWS depends on USERACCT, ITEM
        for (int i = 0; i < numItemThreads; i++) {
            final int lo = i * loadPerThread;
            final int hi = Math.min(this.num_items, (i + 1) * loadPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {
                    loadReviews(conn, lo, hi);
                }

                @Override
                public void beforeLoad() {
                    try {
                        userLatch.await();
                        itemLatch.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        return threads;
    }

    /**
     * @throws SQLException
     * @author Djellel Load num_users users.
     */
    private void loadUsers(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable("useracct");
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;
        try (PreparedStatement userInsert = conn.prepareStatement(sql)) {
            long timestamp = System.currentTimeMillis();
            for (int i = lo; i < hi; i++) {
                String name = TextGenerator.randomStr(rng(), EpinionsConstants.NAME_LENGTH);
                String email = TextGenerator.randomStr(rng(), EpinionsConstants.EMAIL_LENGTH);

                userInsert.setInt(1, i);
                userInsert.setString(2, name);
                userInsert.setString(3, email);
                userInsert.setTimestamp(4, new Timestamp(timestamp));
                userInsert.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    userInsert.executeBatch();
                    batch = 0;
                    userInsert.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Users %d / %d", total, num_users));
                    }
                }
            }
            if (batch > 0) {
                userInsert.executeBatch();
                userInsert.clearBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Users Loaded [%d]", total));
        }
    }

    /**
     * @throws SQLException
     * @author Djellel Load num_items items.
     */
    private void loadItems(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable("item");
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;
        try (PreparedStatement itemInsert = conn.prepareStatement(sql)) {
            ZipfianGenerator descLength = new ZipfianGenerator(rng(), EpinionsConstants.DESCRIPTION_LENGTH);
            long timestamp = System.currentTimeMillis();

            for (int i = lo; i < hi; i++) {
                String title = TextGenerator.randomStr(rng(), EpinionsConstants.TITLE_LENGTH);
                String desc = TextGenerator.randomStr(rng(), descLength.nextInt());

                itemInsert.setInt(1, i);
                itemInsert.setString(2, title);
                itemInsert.setString(3, desc);
                itemInsert.setTimestamp(4, new Timestamp(timestamp));
                itemInsert.addBatch();
                total++;

                if ((++batch % workConf.getBatchSize()) == 0) {
                    itemInsert.executeBatch();
                    batch = 0;
                    itemInsert.clearBatch();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Items %d / %d", total, num_items));
                    }
                }
            }
            if (batch > 0) {
                itemInsert.executeBatch();
                itemInsert.clearBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Items Loaded [%d]", total));
        }
    }

    /**
     * @throws SQLException
     * @author Djellel What's going on here?: For each item we Loaded, we are
     * going to generate reviews The number of reviews per Item selected
     * from num_reviews. Who gives the reviews is selected from
     * num_users and added to reviewers list. Note: the selection is
     * based on Zipfian distribution.
     */
    private void loadReviews(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable("review");
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;

        try (PreparedStatement reviewInsert = conn.prepareStatement(sql)) {
            ZipfianGenerator numReviews = new ZipfianGenerator(rng(), num_reviews, 1.8);
            ZipfianGenerator reviewer = new ZipfianGenerator(rng(), num_users);
            ZipfianGenerator commentLength = new ZipfianGenerator(rng(), EpinionsConstants.COMMENT_LENGTH - EpinionsConstants.COMMENT_MIN_LENGTH);
            Set<Integer> reviewers = new HashSet<>();

            for (int i = lo; i < hi; i++) {
                // make sure each item has at least one review
                int review_count = numReviews.nextInt() + 1;
                long timestamp = System.currentTimeMillis();

                for (int rc = 0; rc < review_count; ) {
                    int u_id = reviewer.nextInt();
                    if (!reviewers.contains(u_id)) {
                        String comment = TextGenerator.randomStr(rng(), commentLength.nextInt() + EpinionsConstants.COMMENT_MIN_LENGTH);
                        reviewInsert.setInt(1, total);
                        reviewInsert.setInt(2, u_id);
                        reviewInsert.setInt(3, i);
                        reviewInsert.setInt(4, rng().nextInt(5));// rating
                        reviewInsert.setNull(5, java.sql.Types.INTEGER);
                        reviewInsert.setString(6, comment);
                        reviewInsert.setTimestamp(7, new Timestamp(timestamp));
                        reviewInsert.addBatch();
                        reviewers.add(u_id);
                        total++;
                        rc++;
                        if ((++batch % workConf.getBatchSize()) == 0) {
                            reviewInsert.executeBatch();
                            batch = 0;
                            reviewInsert.clearBatch();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Reviewed items  % {}", (int) (((double) i / (double) this.num_items) * 100));
                            }
                        }
                    }
                }
                reviewers.clear();
            }
            if (batch > 0) {
                reviewInsert.executeBatch();
                reviewInsert.clearBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Reviews Loaded [%d]", total));
        }
    }

    /**
     * @throws SQLException
     * @author Djellel What's going on here?: For each user, select a number
     * num_trust of trust-feedbacks (given by others users). Then we
     * select the users who are part of that list. The actual feedback
     * can be 1/0 with uniform distribution. Note: Select is based on
     * Zipfian distribution Trusted users are not correlated to heavy
     * reviewers (drawn using a scrambled distribution)
     */
    public void loadTrust(Connection conn, int lo, int hi, boolean firstLoader) throws SQLException {
        Table catalog_tbl = this.benchmark.getCatalog().getTable("trust");
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;
        int batch = 0;

        try (PreparedStatement trustInsert = conn.prepareStatement(sql)) {
            ZipfianGenerator numTrust = new ZipfianGenerator(rng(), num_trust, 1.95);
            ScrambledZipfianGenerator reviewed = new ScrambledZipfianGenerator(num_users);
            Set<Integer> trusted = new HashSet<>();

            for (int i = lo; i < hi; i++) {
                long timestamp = System.currentTimeMillis();
                int trust_count = numTrust.nextInt();
                // To avoid having an empty trust table during small scale
                // tests, we make sure that at least one trust record is
                // inserted to the table.
                if (firstLoader && i == lo) {
                    trust_count = Math.max(trust_count, 1);
                }
                for (int tc = 0; tc < trust_count; ) {
                    int u_id = reviewed.nextInt();
                    if (!trusted.contains(u_id)) {
                        tc++;
                        trustInsert.setInt(1, i);
                        trustInsert.setInt(2, u_id);
                        trustInsert.setInt(3, rng().nextInt(2));
                        trustInsert.setDate(4, new java.sql.Date(timestamp));
                        trustInsert.addBatch();
                        trusted.add(u_id);
                        total++;

                        if ((++batch % workConf.getBatchSize()) == 0) {
                            trustInsert.executeBatch();
                            batch = 0;
                            trustInsert.clearBatch();
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Rated users  % {}", (int) (((double) i / (double) this.num_users) * 100));
                            }

                        }
                    }
                }
                trusted.clear();
            }
            if (batch > 0) {
                trustInsert.executeBatch();
                trustInsert.clearBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Trust Loaded [%d]", total));
        }
    }
}
