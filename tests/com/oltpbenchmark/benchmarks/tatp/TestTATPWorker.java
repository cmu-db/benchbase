package com.oltpbenchmark.benchmarks.tatp;

import com.oltpbenchmark.api.AbstractTestWorker;

public class TestTATPWorker extends AbstractTestWorker<TATPBenchmark> {
   
    @Override
    protected void setUp() throws Exception {
        super.setUp(TATPBenchmark.class, TestTATPBenchmark.PROC_CLASSES);
    }
}
