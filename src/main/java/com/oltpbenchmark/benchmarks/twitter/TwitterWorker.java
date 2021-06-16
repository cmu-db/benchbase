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

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.*;
import com.oltpbenchmark.benchmarks.twitter.util.TweetHistogram;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterOperation;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;

public class TwitterWorker extends Worker<TwitterBenchmark> {
    private final TransactionGenerator<TwitterOperation> generator;

    private final FlatHistogram<Integer> tweet_len_rng;
    private final int num_users;

    public TwitterWorker(TwitterBenchmark benchmarkModule, int id, TransactionGenerator<TwitterOperation> generator) {
        super(benchmarkModule, id);
        this.generator = generator;
        this.num_users = (int) Math.round(TwitterConstants.NUM_USERS * this.getWorkloadConfiguration().getScaleFactor());

        TweetHistogram tweet_h = new TweetHistogram();
        this.tweet_len_rng = new FlatHistogram<>(this.rng(), tweet_h);
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType nextTrans) throws UserAbortException, SQLException {
        TwitterOperation t = generator.nextTransaction();
        // zero is an invalid id, so fixing random here to be atleast 1
        t.uid = this.rng().nextInt(this.num_users - 1 ) + 1;

        if (nextTrans.getProcedureClass().equals(GetTweet.class)) {
            doSelect1Tweet(conn, t.tweetid);
        } else if (nextTrans.getProcedureClass().equals(GetTweetsFromFollowing.class)) {
            doSelectTweetsFromPplIFollow(conn, t.uid);
        } else if (nextTrans.getProcedureClass().equals(GetFollowers.class)) {
            doSelectNamesOfPplThatFollowMe(conn, t.uid);
        } else if (nextTrans.getProcedureClass().equals(GetUserTweets.class)) {
            doSelectTweetsForUid(conn, t.uid);
        } else if (nextTrans.getProcedureClass().equals(InsertTweet.class)) {
            int len = this.tweet_len_rng.nextValue();
            String text = TextGenerator.randomStr(this.rng(), len);
            doInsertTweet(conn, t.uid, text);
        }
        return (TransactionStatus.SUCCESS);
    }

    public void doSelect1Tweet(Connection conn, int tweet_id) throws SQLException {
        GetTweet proc = this.getProcedure(GetTweet.class);

        proc.run(conn, tweet_id);
    }

    public void doSelectTweetsFromPplIFollow(Connection conn, int uid) throws SQLException {
        GetTweetsFromFollowing proc = this.getProcedure(GetTweetsFromFollowing.class);

        proc.run(conn, uid);
    }

    public void doSelectNamesOfPplThatFollowMe(Connection conn, int uid) throws SQLException {
        GetFollowers proc = this.getProcedure(GetFollowers.class);

        proc.run(conn, uid);
    }

    public void doSelectTweetsForUid(Connection conn, int uid) throws SQLException {
        GetUserTweets proc = this.getProcedure(GetUserTweets.class);

        proc.run(conn, uid);
    }

    public void doInsertTweet(Connection conn, int uid, String text) throws SQLException {
        InsertTweet proc = this.getProcedure(InsertTweet.class);

        Time time = new Time(System.currentTimeMillis());
        proc.run(conn, uid, text, time);

    }
}
