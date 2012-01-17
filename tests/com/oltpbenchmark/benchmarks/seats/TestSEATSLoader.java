package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.api.AbstractTestLoader;

public class TestSEATSLoader extends AbstractTestLoader<SEATSBenchmark> {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(SEATSBenchmark.class, null, TestSEATSBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.01);
    }

}
