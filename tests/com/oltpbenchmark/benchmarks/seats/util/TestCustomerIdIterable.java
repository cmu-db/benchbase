package com.oltpbenchmark.benchmarks.seats.util;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import com.oltpbenchmark.util.Histogram;

import junit.framework.TestCase;

public class TestCustomerIdIterable extends TestCase {

    final Random rand = new Random();
    final Histogram<Long> airport_max_customer_id = new Histogram<Long>();
    CustomerIdIterable customer_id_iterable;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        for (long airport = 0; airport <= 285; airport++) {
            this.airport_max_customer_id.put(airport, rand.nextInt(100));
        } // FOR
        this.customer_id_iterable = new CustomerIdIterable(this.airport_max_customer_id);
    }
    
    
    /**
     * testIterator
     */
    public void testIterator() throws Exception {
        Set<Long> seen_ids = new HashSet<Long>();
        Histogram<Long> airport_ids = new Histogram<Long>();
        for (CustomerId c_id : this.customer_id_iterable) {
            assertNotNull(c_id);
            long encoded = c_id.encode();
            assertFalse(seen_ids.contains(encoded));
            seen_ids.add(encoded);
            airport_ids.put(c_id.getDepartAirportId());
        } // FOR
        assertEquals(this.airport_max_customer_id.getSampleCount(), seen_ids.size());
        assertEquals(this.airport_max_customer_id, airport_ids);
    }
    
    
}
