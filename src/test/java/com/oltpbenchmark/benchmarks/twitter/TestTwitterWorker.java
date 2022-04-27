package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

//@Ignore("the testcase is under development")
public class TestTwitterWorker extends AbstractTestWorker<TwitterBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTwitterBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TwitterBenchmark> benchmarkClass() {
        return TwitterBenchmark.class;
    }

    @Override
    protected String traceFile1() {
        return "data/twitter/twitter_tweetids.txt";
    }

    @Override
    protected String traceFile2() {
        return "data/twitter/twitter_user_ids.txt";
    }
}
