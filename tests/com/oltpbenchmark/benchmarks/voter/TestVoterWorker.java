package com.oltpbenchmark.benchmarks.voter;

import com.oltpbenchmark.api.AbstractTestWorker;

public class TestVoterWorker extends AbstractTestWorker<VoterBenchmark> {
   
    @Override
    protected void setUp() throws Exception {
        super.setUp(VoterBenchmark.class, TestVoterBenchmark.PROC_CLASSES);
    }
}
