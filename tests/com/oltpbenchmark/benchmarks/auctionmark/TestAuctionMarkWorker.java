package com.oltpbenchmark.benchmarks.auctionmark;

import com.oltpbenchmark.api.AbstractTestWorker;

public class TestAuctionMarkWorker extends AbstractTestWorker<AuctionMarkBenchmark> {
   
    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(AuctionMarkBenchmark.class, TestAuctionMarkBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.01);
    }
    
}
