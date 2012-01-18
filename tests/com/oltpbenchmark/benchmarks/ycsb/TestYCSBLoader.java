package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestYCSBLoader extends AbstractTestLoader<YCSBBenchmark> {

    @Override
    protected void setUp() throws Exception {
        super.setUp(YCSBBenchmark.class, null, TestYCSBBenchmark.PROC_CLASSES);
    }

}
