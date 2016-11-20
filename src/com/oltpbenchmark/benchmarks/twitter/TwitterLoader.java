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

package com.oltpbenchmark.benchmarks.twitter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.benchmarks.twitter.util.NameHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;
import com.oltpbenchmark.distributions.ZipfianGenerator;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.util.TextGenerator;

public class TwitterLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(TwitterLoader.class);

    public final static int configCommitCount = 1000;

    private final int num_users;
    private final long num_tweets;
    private final int num_follows;

    public TwitterLoader(TwitterBenchmark benchmark, Connection c) {
        super(benchmark, c);
        this.num_users = (int)Math.round(TwitterConstants.NUM_USERS * this.scaleFactor);
        this.num_tweets = (int)Math.round(TwitterConstants.NUM_TWEETS * this.scaleFactor);
        this.num_follows = (int)Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * this.scaleFactor);
        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  " + this.num_users);
            LOG.debug("# of TWEETS: " + this.num_tweets);
            LOG.debug("# of FOLLOWS: " + this.num_follows);
        }
    }
    
    /**
     * @author Djellel
     * Load num_users users.
     * @throws SQLException
     */
    protected void loadUsers() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(TwitterConstants.TABLENAME_USER);
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);
        
        NameHistogram name_h = new NameHistogram();
        FlatHistogram<Integer> name_len_rng = new FlatHistogram<Integer>(this.rng(), name_h);
        
        int total = 0;
        int batchSize = 0;
        
        for (int i = 0; i <= this.num_users; i++) {
        	// Generate a random username for this user
        	int name_length = name_len_rng.nextValue().intValue();
            String name = TextGenerator.randomStr(rng(), name_length);
            
            userInsert.setInt(1, i); // ID
            userInsert.setString(2, name); // NAME
            userInsert.setString(3, name + "@tweeter.com"); // EMAIL
            userInsert.setNull(4, java.sql.Types.INTEGER);
            userInsert.setNull(5, java.sql.Types.INTEGER);
            userInsert.setNull(6, java.sql.Types.INTEGER);
            userInsert.addBatch();
            
            batchSize++;
            total++;
            if ((batchSize % configCommitCount) == 0) {
                int result[] = userInsert.executeBatch();
                assert(result != null);
                conn.commit();
                userInsert.clearBatch();
                batchSize = 0;
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
            }
        } // FOR
        if (batchSize > 0) {
            userInsert.executeBatch();
            conn.commit();
            userInsert.clearBatch();
        }
        userInsert.close();
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Users Loaded [%d]", total));
    }
    
    /**
     * @author Djellel
     * What's going on here?: 
     * The number of tweets is fixed to num_tweets
     * We simply select using the distribution who issued the tweet
     * @throws SQLException
     */
    protected void loadTweets() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(TwitterConstants.TABLENAME_TWEETS);
        assert(catalog_tbl != null);
        String sql = SQLUtil.getInsertSQL(catalog_tbl);
        PreparedStatement tweetInsert = this.conn.prepareStatement(sql);
        
        int total = 0;
        int batchSize = 0;
        ScrambledZipfianGenerator zy = new ScrambledZipfianGenerator(this.num_users);
        
        TweetHistogram tweet_h = new TweetHistogram();
        FlatHistogram<Integer> tweet_len_rng = new FlatHistogram<Integer>(this.rng(), tweet_h);
        
        for (long i = 0; i < this.num_tweets; i++) {
            int uid = zy.nextInt();
            tweetInsert.setLong(1, i);
            tweetInsert.setInt(2, uid);
            tweetInsert.setString(3, TextGenerator.randomStr(rng(), tweet_len_rng.nextValue()));
            tweetInsert.setNull(4, java.sql.Types.DATE);
            tweetInsert.addBatch();
            batchSize++;
            total++;

            if ((batchSize % configCommitCount) == 0) {
                tweetInsert.executeBatch();
                conn.commit();
                tweetInsert.clearBatch();            
                batchSize = 0;
                if (LOG.isDebugEnabled()) 
                    LOG.debug("tweet % " + total + "/"+this.num_tweets);
            }
        }
        if (batchSize > 0) {
            tweetInsert.executeBatch();
            conn.commit();
        }
        tweetInsert.close();
        if (LOG.isDebugEnabled()) 
            LOG.debug("[Tweets Loaded] "+ this.num_tweets);
    }
    
    /**
     * @author Djellel
     * What's going on here?: 
     * For each user (follower) we select how many users he is following (followees List)
     * then select users to fill up that list.
     * Selecting is based on the distribution.
     * NOTE: We are using two different distribution to avoid correlation:
     * ZipfianGenerator (describes the followed most) 
     * ScrambledZipfianGenerator (describes the heavy tweeters)
     * @throws SQLException
     */
    protected void loadFollowData() throws SQLException {
        Table catalog_tbl = this.getTableCatalog(TwitterConstants.TABLENAME_FOLLOWS);
        assert(catalog_tbl != null);
        final PreparedStatement followsInsert = this.conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl));

        catalog_tbl = this.getTableCatalog(TwitterConstants.TABLENAME_FOLLOWERS);
        assert(catalog_tbl != null);
        final PreparedStatement followersInsert = this.conn.prepareStatement(SQLUtil.getInsertSQL(catalog_tbl));

        int total = 1;
        int batchSize = 0;
        
        ZipfianGenerator zipfFollowee = new ZipfianGenerator(this.num_users,1.75);
        ZipfianGenerator zipfFollows = new ZipfianGenerator(this.num_follows,1.75);
        List<Integer> followees = new ArrayList<Integer>();
        for (int follower = 0; follower < this.num_users; follower++) {
            followees.clear();
            int time = zipfFollows.nextInt();
            if(time==0) time=1; // At least this follower will follow 1 user 
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
                    f++;

                    if ((batchSize % configCommitCount) == 0) {
                        followsInsert.executeBatch();
                        followersInsert.executeBatch();
                        conn.commit();
                        followsInsert.clearBatch();
                        followersInsert.clearBatch();
                        batchSize = 0;
                        if (LOG.isDebugEnabled()) 
                            LOG.debug("Follows  % " + (int)(((double)follower/(double)this.num_users)*100));
                    }
                }
            } // FOR
        } // FOR
        if (batchSize > 0) {
            followsInsert.executeBatch();
            followersInsert.executeBatch();
            conn.commit();
        }
        followsInsert.close();
        followersInsert.close();
        if (LOG.isDebugEnabled()) LOG.debug("[Follows Loaded] "+total);
    }

    @Override
    public void load() throws SQLException {
        this.loadUsers();
        this.loadTweets();
        this.loadFollowData();
    }
}
