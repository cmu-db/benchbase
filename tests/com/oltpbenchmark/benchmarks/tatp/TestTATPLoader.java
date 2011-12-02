package com.oltpbenchmark.benchmarks.tatp;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestTATPLoader extends AbstractTestLoader<TATPBenchmark> {

    @Override
    protected void setUp() throws Exception {
        super.setUp(TATPBenchmark.class, null, TestTATPBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.0001);
    }

}
