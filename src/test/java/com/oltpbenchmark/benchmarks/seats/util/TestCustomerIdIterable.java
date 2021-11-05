/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.seats.util;

import com.oltpbenchmark.util.Histogram;
import junit.framework.TestCase;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

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
        Set<String> seen_ids = new HashSet<>();
        Histogram<Long> airport_ids = new Histogram<>();
        for (CustomerId c_id : this.customer_id_iterable) {
            assertNotNull(c_id);
            String encoded = c_id.encode();
            assertFalse(seen_ids.contains(encoded));
            seen_ids.add(encoded);
            airport_ids.put(c_id.getDepartAirportId());
        } // FOR
        assertEquals(this.airport_max_customer_id.getSampleCount(), seen_ids.size());
        assertEquals(this.airport_max_customer_id, airport_ids);
    }


}
