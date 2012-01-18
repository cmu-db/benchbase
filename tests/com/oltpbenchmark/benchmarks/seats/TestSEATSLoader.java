package com.oltpbenchmark.benchmarks.seats;

import com.oltpbenchmark.api.AbstractTestLoader;
import com.oltpbenchmark.util.RandomGenerator;

public class TestSEATSLoader extends AbstractTestLoader<SEATSBenchmark> {

    static {
        org.apache.log4j.PropertyConfigurator.configure("/home/pavlo/Documents/OLTPBenchmark/OLTPBenchmark/log4j.properties");
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(SEATSBenchmark.class, null, TestSEATSBenchmark.PROC_CLASSES);
    }
    
    /**
     * testSaveLoadProfile
     */
    public void testSaveLoadProfile() throws Exception {
        SEATSLoader loader = (SEATSLoader)this.benchmark.makeLoaderImpl(conn);
        assertNotNull(loader);
        loader.load();
        
        SEATSProfile orig = loader.profile;
        assertNotNull(orig);
        
        SEATSProfile copy = new SEATSProfile(this.benchmark, new RandomGenerator(0));
        assert(copy.airport_histograms.isEmpty());
        copy.loadProfile(this.conn);
        
        assertEquals(orig.scale_factor, copy.scale_factor);
        assertEquals(orig.airport_max_customer_id, copy.airport_max_customer_id);
        assertEquals(orig.flight_start_date.toString(), copy.flight_start_date.toString());
        assertEquals(orig.flight_upcoming_date.toString(), copy.flight_upcoming_date.toString());
        assertEquals(orig.flight_past_days, copy.flight_past_days);
        assertEquals(orig.flight_future_days, copy.flight_future_days);
        assertEquals(orig.flight_upcoming_offset, copy.flight_upcoming_offset);
        assertEquals(orig.reservation_upcoming_offset, copy.reservation_upcoming_offset);
        assertEquals(orig.num_records, copy.num_records);
        assertEquals(orig.histograms, copy.histograms);
        assertEquals(orig.airport_histograms, copy.airport_histograms);
//        assertEquals(orig.code_id_xref, copy.code_id_xref);
    }

}
