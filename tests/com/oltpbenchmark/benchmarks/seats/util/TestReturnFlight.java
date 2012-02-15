/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package com.oltpbenchmark.benchmarks.seats.util;

import java.sql.Timestamp;

import junit.framework.TestCase;

public class TestReturnFlight extends TestCase {
    
    private final int customer_base_id  = 1000;
    private final long depart_airport_id = 9999;
    private final int return_days[]      = { 1, 5, 14 };
    
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
