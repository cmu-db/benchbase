package com.oltpbenchmark.benchmarks.auctionmark;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.util.RandomGenerator;

public class TestAuctionMarkLoader extends AbstractTestLoader<AuctionMarkBenchmark> {

//    static {
//        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
//    }
    
    private static String IGNORE[] = {
//        AuctionMarkConstants.TABLENAME_CONFIG_PROFILE,
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(AuctionMarkBenchmark.class, IGNORE, TestAuctionMarkBenchmark.PROC_CLASSES);
        this.workConf.setScaleFactor(0.01);
    }
    
    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        AuctionMarkLoader loader = (AuctionMarkLoader)this.benchmark.makeLoaderImpl(conn);
        assertNotNull(loader);
        loader.load();
        
        AuctionMarkProfile orig = loader.profile;
        assertNotNull(orig);
        assertFalse(orig.users_per_item_count.isEmpty());
        
        AuctionMarkProfile copy = new AuctionMarkProfile(this.benchmark, new RandomGenerator(0));
        assertTrue(copy.users_per_item_count.isEmpty());
        copy.loadProfile(this.conn);
        
        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.benchmarkStartTime.toString(), copy.benchmarkStartTime.toString());
        assertEquals(orig.users_per_item_count, copy.users_per_item_count);
    }

}
