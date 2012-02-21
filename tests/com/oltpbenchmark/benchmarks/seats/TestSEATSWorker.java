package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.api.AbstractTestWorker;

public class TestSEATSWorker extends AbstractTestWorker<SEATSBenchmark> {
   
    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(SEATSBenchmark.class, TestSEATSBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.01);
        SEATSProfile.clearCachedProfile();
    }
    
}
