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

import java.sql.SQLException;
import java.sql.Time;

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweet;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetTweetsFromFollowing;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetUserTweets;
import com.oltpbenchmark.benchmarks.twitter.procedures.InsertTweet;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterOperation;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.TextGenerator;

public class TwitterWorker extends Worker<TwitterBenchmark> {
    private TransactionGenerator<TwitterOperation> generator;

    private final FlatHistogram<Integer> tweet_len_rng;
    private final int num_users;
    
    public TwitterWorker(TwitterBenchmark benchmarkModule, int id, TransactionGenerator<TwitterOperation> generator) {
        super(benchmarkModule, id);
        this.generator = generator;
        this.num_users = (int)Math.round(TwitterConstants.NUM_USERS * this.getWorkloadConfiguration().getScaleFactor());
        
        TweetHistogram tweet_h = new TweetHistogram();
        this.tweet_len_rng = new FlatHistogram<Integer>(this.rng(), tweet_h);
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        TwitterOperation t = generator.nextTransaction();
        t.uid = this.rng().nextInt(this.num_users); // HACK
        
        if (nextTrans.getProcedureClass().equals(GetTweet.class)) {
            doSelect1Tweet(t.tweetid);
        } else if (nextTrans.getProcedureClass().equals(GetTweetsFromFollowing.class)) {
            doSelectTweetsFromPplIFollow(t.uid);
        } else if (nextTrans.getProcedureClass().equals(GetFollowers.class)) {
            doSelectNamesOfPplThatFollowMe(t.uid);
        } else if (nextTrans.getProcedureClass().equals(GetUserTweets.class)) {
            doSelectTweetsForUid(t.uid);
        } else if (nextTrans.getProcedureClass().equals(InsertTweet.class)) {
            int len = this.tweet_len_rng.nextValue().intValue();
            String text = TextGenerator.randomStr(this.rng(), len);
            doInsertTweet(t.uid, text);
        }
        conn.commit();
        return (TransactionStatus.SUCCESS);
    }

    public void doSelect1Tweet(int tweet_id) throws SQLException {
        GetTweet proc = this.getProcedure(GetTweet.class);
        assert (proc != null);
        proc.run(conn, tweet_id);
    }

    public void doSelectTweetsFromPplIFollow(int uid) throws SQLException {
        GetTweetsFromFollowing proc = this.getProcedure(GetTweetsFromFollowing.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doSelectNamesOfPplThatFollowMe(int uid) throws SQLException {
        GetFollowers proc = this.getProcedure(GetFollowers.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doSelectTweetsForUid(int uid) throws SQLException {
        GetUserTweets proc = this.getProcedure(GetUserTweets.class);
        assert (proc != null);
        proc.run(conn, uid);
    }

    public void doInsertTweet(int uid, String text) throws SQLException {
        InsertTweet proc = this.getProcedure(InsertTweet.class);
        assert (proc != null);
        Time time = new Time(System.currentTimeMillis());
        try {
            proc.run(conn, uid, text, time);
        } catch (SQLException ex) {
            System.err.println("uid=" + uid);
            System.err.println("text=" + text);
            System.err.println("time=" + time);
            throw ex;
        }
    }
}
