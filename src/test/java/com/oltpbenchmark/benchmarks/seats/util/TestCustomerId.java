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

import junit.framework.TestCase;

public class TestCustomerId extends TestCase {

    private final long[] base_ids = {66666, 77777, 88888};
    private final long[] airport_ids = {123, 1234, 12345};

    /**
     * testCustomerId
     */
    public void testCustomerId() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                CustomerId customer_id = new CustomerId((int) base_id, airport_id);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }

    /**
     * testCustomerIdEncode
     */
    public void testCustomerIdEncode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                String encoded = new CustomerId((int) base_id, airport_id).encode();
//                System.err.println("base_id=" + base_id);
//                System.err.println("airport_id=" + airport_id);
//                System.err.println("encodd=" + encoded);
//                System.exit(1);

                CustomerId customer_id = new CustomerId(encoded);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }

    /**
     * testCustomerIdDecode
     */
    public void testCustomerIdDecode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                String[] values = {String.valueOf(base_id), String.valueOf(airport_id)};
                String encoded = new CustomerId((int) base_id, airport_id).encode();

                String[] new_values = new CustomerId(encoded).toArray();
                assertEquals(values.length, new_values.length);
                for (int i = 0; i < new_values.length; i++) {
                    assertEquals(values[i], new_values[i]);
                } // FOR
            } // FOR
        } // FOR
    }
}