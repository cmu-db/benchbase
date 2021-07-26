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

import java.sql.Timestamp;

public class TestReturnFlight extends TestCase {

    private final int customer_base_id = 1000;
    private final long depart_airport_id = 9999;
    private final int[] return_days = {1, 5, 14};

    private Timestamp flight_date;
    private CustomerId customer_id;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.customer_id = new CustomerId(this.customer_base_id, this.depart_airport_id);
        assertNotNull(this.customer_id);
        this.flight_date = new Timestamp(System.currentTimeMillis());
        assertNotNull(this.flight_date);
    }

    /**
     * testReturnFlight
     */
    public void testReturnFlight() {
        for (int return_day : this.return_days) {
            ReturnFlight return_flight = new ReturnFlight(this.customer_id, this.depart_airport_id, this.flight_date, return_day);
            assertNotNull(return_flight);
            assertEquals(this.customer_id, return_flight.getCustomerId());
            assertEquals(this.depart_airport_id, return_flight.getReturnAirportId());
            assertTrue(this.flight_date.getTime() < return_flight.getReturnDate().getTime());
        } // FOR
    }

    /**
     * testCalculateReturnDate
     */
    public void testCalculateReturnDate() {
        for (int return_day : this.return_days) {
            Timestamp return_flight_date = ReturnFlight.calculateReturnDate(this.flight_date, return_day);
            assertNotNull(return_flight_date);
            assertTrue(this.flight_date.getTime() < return_flight_date.getTime());
        } // FOR
    }
}
