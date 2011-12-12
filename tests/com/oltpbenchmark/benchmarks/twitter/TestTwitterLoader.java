package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestTwitterLoader extends AbstractTestLoader<TwitterBenchmark> {

    private final String IGNORED_TABLES[] = {
        "added_tweets",
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(TwitterBenchmark.class, IGNORED_TABLES, TestTwitterBenchmark.PROC_CLASSES);
    }

}
