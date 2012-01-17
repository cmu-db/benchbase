package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.api.AbstractTestWorker;

public class TestSEATSWorker extends AbstractTestWorker<SEATSBenchmark> {
   
    @Override
    protected void setUp() throws Exception {
        super.setUp(SEATSBenchmark.class, TestSEATSBenchmark.PROC_CLASSES);
    }
}
