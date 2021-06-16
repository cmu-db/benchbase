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
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.twitter.procedures.GetFollowers;
import com.oltpbenchmark.benchmarks.twitter.util.TraceTransactionGenerator;
import com.oltpbenchmark.benchmarks.twitter.util.TwitterOperation;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class TwitterBenchmark extends BenchmarkModule {

    private final TwitterConfiguration twitterConf;

    public TwitterBenchmark(WorkloadConfiguration workConf) {
        super(workConf);
        this.twitterConf = new TwitterConfiguration(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetFollowers.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<String> tweetIds = FileUtils.readLines(new File(twitterConf.getTracefile()), Charset.defaultCharset());
        List<String> userIds = FileUtils.readLines(new File(twitterConf.getTracefile2()), Charset.defaultCharset());

        if (tweetIds.size() != userIds.size()) {
            throw new RuntimeException(String.format("there was a problem reading files, sizes don't match.  tweets %d, userids %d", tweetIds.size(), userIds.size()));
        }

        List<TwitterOperation> trace = new ArrayList<>();
        for (int i = 0; i < tweetIds.size(); i++) {
            trace.add(new TwitterOperation(Integer.parseInt(tweetIds.get(i)), Integer.parseInt(userIds.get(i))));
        }

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 0; i < workConf.getTerminals(); ++i) {
            TransactionGenerator<TwitterOperation> generator = new TraceTransactionGenerator(trace);
            workers.add(new TwitterWorker(this, i, generator));
        }
        return workers;
    }

    @Override
    protected Loader<TwitterBenchmark> makeLoaderImpl() {
        return new TwitterLoader(this);
    }
}
