package com.oltpbenchmark.benchmarks.twitter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderUtil;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.distributions.ScrambledZipfianGenerator;

public class TwitterLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(TwitterLoader.class);

    private static final int USERS = 500; // Number of user baseline
    private static final int TWEETS = 20000;// Number of tweets baseline
    private static final int FOLLOW = 100;// Max follow per user baseline

    private static final int NAME = 5;// Name length

    public final static int configCommitCount = 1000;

    private final int num_users;
    private final long num_tweets;
    private final int num_follows;

    public TwitterLoader(Connection c, WorkloadConfiguration workConf, Map<String, Table> tables) {
        super(c, workConf, tables);
        this.num_users = (int)Math.round(USERS * this.scaleFactor);
        this.num_tweets = (int)Math.round(TWEETS * this.scaleFactor);
        this.num_follows= (int)Math.round(FOLLOW * this.scaleFactor);
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
        Table catalog_tbl = this.getTableCatalog("user");
        assert(catalog_tbl != null);
        String sql = catalog_tbl.getInsertSQL(1);
        System.out.println("Executing: "+sql);
        PreparedStatement userInsert = this.conn.prepareStatement(sql);
        long total = 0;
        for (int i = 0; i < num_users; i++) {
            String name = LoaderUtil.randomStr(NAME);
            userInsert.setInt(1, i); // ID
            userInsert.setString(2, name); // NAME
            userInsert.setString(3, name + "@tweeter.com"); // EMAIL
            userInsert.setNull(4, java.sql.Types.INTEGER);
            userInsert.setNull(5, java.sql.Types.INTEGER);
            userInsert.setNull(6, java.sql.Types.INTEGER);
            userInsert.addBatch();
            if ((++total % configCommitCount) == 0) {
                int result[] = userInsert.executeBatch();
                assert(result != null);
                conn.commit();
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Users %d / %d", total, num_users));
            }
        } // FOR
        userInsert.executeBatch();
        conn.commit();
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
        Table catalog_tbl = this.getTableCatalog("tweets");
        assert(catalog_tbl != null);
        String sql = catalog_tbl.getInsertSQL(1);
        PreparedStatement tweetInsert = this.conn.prepareStatement(sql);
        //
        int total = 0;
        ScrambledZipfianGenerator zy=new ScrambledZipfianGenerator(this.num_users);
        for (long i = 0; i < this.num_tweets; i++) {
            int uid = zy.nextInt();
            tweetInsert.setLong(1, i);
            tweetInsert.setInt(2, uid);
            tweetInsert.setString(3, "some random text from tweeter" + uid);
            tweetInsert.setNull(4, java.sql.Types.DATE);
            tweetInsert.addBatch();
            if ((++total % configCommitCount) == 0) {
                tweetInsert.executeBatch();
                conn.commit();
                tweetInsert.clearBatch();
                if (LOG.isDebugEnabled()) LOG.debug("tweet % " + total);
            }
        }
        tweetInsert.executeBatch();
        conn.commit();
        if (LOG.isDebugEnabled()) LOG.debug("Tweets Loaded");
    }
    
    /**
     * @author Djellel
     * What's going on here?: 
     * For each user (follower) we select how many users he is following (followees List)
     * then select users to fill up that list.
     * Selecting is based on the distribution.
     * @throws SQLException
     */
    protected void loadFollowData() throws SQLException {
        Table catalog_tbl = this.getTableCatalog("follows");
        assert(catalog_tbl != null);
        final PreparedStatement followsInsert = this.conn.prepareStatement(catalog_tbl.getInsertSQL(1));

        catalog_tbl = this.getTableCatalog("followers");
        assert(catalog_tbl != null);
        final PreparedStatement followersInsert = this.conn.prepareStatement(catalog_tbl.getInsertSQL(1));
        //
        int total = 1;
        ScrambledZipfianGenerator zipfFollowee = new ScrambledZipfianGenerator(this.num_users);
        ScrambledZipfianGenerator zipfFollows = new ScrambledZipfianGenerator(this.num_follows);
        List<Integer> followees = new ArrayList<Integer>();
        for (int follower = 0; follower < this.num_users; follower++) {
            followees.clear();
            int time = zipfFollows.nextInt();
            for (int f = 0; f < time; f++) {
                int followee = zipfFollowee.nextInt();
                if (follower != followee && !followees.contains(followee)) {
                    followsInsert.setInt(1, follower);
                    followsInsert.setInt(2, followee);
                    followsInsert.addBatch();

                    followersInsert.setInt(1, followee);
                    followersInsert.setInt(2, follower);
                    followersInsert.addBatch();

                    followees.add(followee);

                    if ((++total % configCommitCount) == 0) {
                        followsInsert.executeBatch();
                        followersInsert.executeBatch();
                        conn.commit();
                        followsInsert.clearBatch();
                        followersInsert.clearBatch();
                        if (LOG.isDebugEnabled()) LOG.debug("Follows  % " + total);
                    }
                }
            }
        }
        followsInsert.executeBatch();
        followersInsert.executeBatch();
        conn.commit();
        if (LOG.isDebugEnabled()) LOG.debug("Follows Loaded");
    }

    @Override
    public void load() throws SQLException {
        this.loadUsers();
        this.loadTweets();
        this.loadFollowData();
    }

}
