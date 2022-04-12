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

package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.twitter.util.NameHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TwitterLoader extends Loader<TwitterBenchmark> {
    private final int num_users;
    private final long num_tweets;
    private final int num_follows;

    public TwitterLoader(TwitterBenchmark benchmark) {
        super(benchmark);
        this.num_users = (int) Math.round(TwitterConstants.NUM_USERS * this.scaleFactor);
        this.num_tweets = (int) Math.round(TwitterConstants.NUM_TWEETS * this.scaleFactor);
        this.num_follows = (int) Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  {}", this.num_users);
            LOG.debug("# of TWEETS: {}", this.num_tweets);
            LOG.debug("# of FOLLOWS: {}", this.num_follows);
        }
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final int numLoaders = this.benchmark.getWorkloadConfiguration().getLoaderThreads();
        // first we load USERS
        final int itemsPerThread = Math.max(this.num_users / numLoaders, 1);
        final int numUserThreads = (int) Math.ceil((double) this.num_users / itemsPerThread);
        // then we load FOLLOWS and TWEETS
        final long tweetsPerThread = Math.max(this.num_tweets / numLoaders, 1);
        final int numTweetThreads = (int) Math.ceil((double) this.num_tweets / tweetsPerThread);

        final CountDownLatch userLatch = new CountDownLatch(numUserThreads);

        // USERS
        for (int i = 0; i < numUserThreads; i++) {
            final int lo = i * itemsPerThread + 1;
            final int hi = Math.min(this.num_users, (i + 1) * itemsPerThread);

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

        // FOLLOW_DATA depends on USERS
        for (int i = 0; i < numUserThreads; i++) {
            final int lo = i * itemsPerThread + 1;
            final int hi = Math.min(this.num_users, (i + 1) * itemsPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {

                    loadFollowData(conn, lo, hi);
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

        // TWEETS depends on USERS
        for (int i = 0; i < numTweetThreads; i++) {
            final long lo = i * tweetsPerThread + 1;
            final long hi = Math.min(this.num_tweets, (i + 1) * tweetsPerThread);

            threads.add(new LoaderThread(this.benchmark) {
                @Override
                public void load(Connection conn) throws SQLException {


                    loadTweets(conn, lo, hi);
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

        return threads;
    }

    /**
     * @throws SQLException
     * @author Djellel Load num_users users.
     */
    protected void loadUsers(Connection conn, int lo, int hi) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(TwitterConstants.TABLENAME_USER);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;

        try (PreparedStatement userInsert = conn.prepareStatement(sql)) {
            int batchSize = 0;

            NameHistogram name_h = new NameHistogram();
            FlatHistogram<Integer> name_len_rng = new FlatHistogram<>(this.rng(), name_h);


            for (int i = lo; i <= hi; i++) {
                // Generate a random username for this user
                int name_length = name_len_rng.nextValue();
                String name = TextGenerator.randomStr(this.rng(), name_length);

                userInsert.setInt(1, i); // ID
                userInsert.setString(2, name); // NAME
                userInsert.setString(3, name + "@tweeter.com"); // EMAIL
                userInsert.setNull(4, java.sql.Types.INTEGER);
                userInsert.setNull(5, java.sql.Types.INTEGER);
                userInsert.setNull(6, java.sql.Types.INTEGER);
                userInsert.addBatch();

                batchSize++;
                total++;
                if ((batchSize % workConf.getBatchSize()) == 0) {
                    int[] result = userInsert.executeBatch();

                    userInsert.clearBatch();
                    batchSize = 0;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("Users %d / %d", total, this.num_users));
                    }
                }
            }
            if (batchSize > 0) {
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
     * @author Djellel What's going on here?: The number of tweets is fixed to
     * num_tweets We simply select using the distribution who issued the
     * tweet
     */
    protected void loadTweets(Connection conn, long lo, long hi) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(TwitterConstants.TABLENAME_TWEETS);

        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());

        int total = 0;

        try (PreparedStatement tweetInsert = conn.prepareStatement(sql)) {


            int batchSize = 0;
            ScrambledZipfianGenerator zy = new ScrambledZipfianGenerator(1, this.num_users);

            TweetHistogram tweet_h = new TweetHistogram();
            FlatHistogram<Integer> tweet_len_rng = new FlatHistogram<>(this.rng(), tweet_h);

            for (long i = lo; i <= hi; i++) {
                int uid = zy.nextInt();
                tweetInsert.setLong(1, i);
                tweetInsert.setInt(2, uid);
                tweetInsert.setString(3, TextGenerator.randomStr(this.rng(), tweet_len_rng.nextValue()));
                tweetInsert.setNull(4, java.sql.Types.DATE);
                tweetInsert.addBatch();
                batchSize++;
                total++;

                if ((batchSize % workConf.getBatchSize()) == 0) {
                    tweetInsert.executeBatch();
                    tweetInsert.clearBatch();
                    batchSize = 0;
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("tweet % {}/{}", total, this.num_tweets);
                    }
                }
            }
            if (batchSize > 0) {
                tweetInsert.executeBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("[Tweets Loaded] {}", this.num_tweets);
        }
    }

    /**
     * @throws SQLException
     * @author Djellel What's going on here?: For each user (follower) we select
     * how many users he is following (followees List) then select users
     * to fill up that list. Selecting is based on the distribution.
     * NOTE: We are using two different distribution to avoid
     * correlation: ZipfianGenerator (describes the followed most)
     * ScrambledZipfianGenerator (describes the heavy tweeters)
     */
    protected void loadFollowData(Connection conn, int lo, int hi) throws SQLException {

        int total = 1;

        Table followsTable = benchmark.getCatalog().getTable(TwitterConstants.TABLENAME_FOLLOWS);
        Table followersTable = benchmark.getCatalog().getTable(TwitterConstants.TABLENAME_FOLLOWERS);

        String followsTableSql = SQLUtil.getInsertSQL(followsTable, this.getDatabaseType());
        String followersTableSql = SQLUtil.getInsertSQL(followersTable, this.getDatabaseType());

        try (PreparedStatement followsInsert = conn.prepareStatement(followsTableSql);
             PreparedStatement followersInsert = conn.prepareStatement(followersTableSql)) {


            int batchSize = 0;

            ZipfianGenerator zipfFollowee = new ZipfianGenerator(rng(),1, this.num_users, 1.75);
            ZipfianGenerator zipfFollows = new ZipfianGenerator(rng(), this.num_follows, 1.75);
            List<Integer> followees = new ArrayList<>();
            for (int follower = lo; follower <= hi; follower++) {
                followees.clear();
                int time = zipfFollows.nextInt();
                if (time == 0) {
                    time = 1; // At least this follower will follow 1 user
                }
                for (int f = 0; f < time; ) {
                    int followee = zipfFollowee.nextInt();
                    if (follower != followee && !followees.contains(followee)) {
                        followsInsert.setInt(1, follower);
                        followsInsert.setInt(2, followee);
                        followsInsert.addBatch();

                        followersInsert.setInt(1, followee);
                        followersInsert.setInt(2, follower);
                        followersInsert.addBatch();

                        followees.add(followee);

                        total++;
                        batchSize++;


                        if ((batchSize % workConf.getBatchSize()) == 0) {
                            followsInsert.executeBatch();
                            followersInsert.executeBatch();
                            followsInsert.clearBatch();
                            followersInsert.clearBatch();
                            batchSize = 0;
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Follows  % {}", (int) (((double) follower / (double) this.num_users) * 100));
                            }
                        }
                    }

                    f++;
                }
            }
            if (batchSize > 0) {
                followsInsert.executeBatch();
                followersInsert.executeBatch();
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("[Follows Loaded] {}", total);
        }
    }
}
