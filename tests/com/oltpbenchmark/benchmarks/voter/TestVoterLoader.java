package com.oltpbenchmark.benchmarks.voter;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestVoterLoader extends AbstractTestLoader<VoterBenchmark> {

    @Override
    protected void setUp() throws Exception {
        super.setUp(VoterBenchmark.class, null, TestVoterBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.0001);
    }

}
