package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestYCSBLoader extends AbstractTestLoader<YCSBBenchmark> {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(YCSBBenchmark.class, null, TestYCSBBenchmark.PROC_CLASSES);
    }

}
