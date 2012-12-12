package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestTPCCLoader extends AbstractTestLoader<TPCCBenchmark> {

    @Override
    protected void setUp() throws Exception {
        super.setUp(TPCCBenchmark.class, null, TestTPCCBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.0001);
    }

}
