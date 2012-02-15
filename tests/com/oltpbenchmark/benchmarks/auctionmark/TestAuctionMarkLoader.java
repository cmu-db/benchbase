package com.oltpbenchmark.benchmarks.auctionmark;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.util.RandomGenerator;

public class TestAuctionMarkLoader extends AbstractTestLoader<AuctionMarkBenchmark> {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    private static String IGNORE[] = {
//        AuctionMarkConstants.TABLENAME_CONFIG_PROFILE,
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(AuctionMarkBenchmark.class, IGNORE, TestAuctionMarkBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.1);
    }
    
}
