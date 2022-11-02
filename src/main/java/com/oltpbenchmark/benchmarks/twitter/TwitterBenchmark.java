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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TwitterBenchmark extends BenchmarkModule {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterBenchmark.class);

    public final long numUsers;
    public final long numTweets;
    public final long numFollows;

    public TwitterBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
        // this.twitterConf = new TwitterConfiguration(workConf);

        this.numUsers = Math.round(TwitterConstants.NUM_USERS * workConf.getScaleFactor());
        this.numTweets = Math.round(TwitterConstants.NUM_TWEETS * workConf.getScaleFactor());
        this.numFollows = Math.round(TwitterConstants.MAX_FOLLOW_PER_USER * workConf.getScaleFactor());

        if (LOG.isDebugEnabled()) {
            LOG.debug("# of USERS:  {}", this.numUsers);
            LOG.debug("# of TWEETS: {}", this.numTweets);
            LOG.debug("# of FOLLOWS: {}", this.numFollows);
        }
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetFollowers.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            workers.add(new TwitterWorker(this, i));
        }
        return workers;
    }

    @Override
    protected Loader<TwitterBenchmark> makeLoaderImpl() {
        return new TwitterLoader(this);
    }
}
