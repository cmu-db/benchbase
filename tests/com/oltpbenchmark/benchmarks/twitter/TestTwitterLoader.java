package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.catalog.Catalog;

public class TestTwitterLoader extends AbstractTestLoader<TwitterBenchmark> {

    private final String IGNORED_TABLES[] = {
        TwitterConstants.TABLENAME_ADDED_TWEETS
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(TwitterBenchmark.class, IGNORED_TABLES, TestTwitterBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.1);
        Catalog.setSeparator("");
    }

}
