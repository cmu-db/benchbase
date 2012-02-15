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
import java.util.Calendar;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

import junit.framework.TestCase;

public class TestFlightId extends TestCase {

    private final long base_ids[]               = { 111, 222, 333 };  
    private final long depart_airport_ids[]     = { 444, 555, 666 };
    private final long arrive_airport_ids[]     = { 777, 888, 999 };
    private final int flight_offset_days[]      = { 1, 2, 4, 8 };
    private final Timestamp flight_dates[]  = new Timestamp[this.flight_offset_days.length];
    private Timestamp start_date;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.start_date = new Timestamp(Calendar.getInstance().getTime().getTime());
        for (int i = 0; i < this.flight_dates.length; i++) {
            int day = this.flight_offset_days[i];
            this.flight_dates[i] = new Timestamp(this.start_date.getTime() + (day * SEATSConstants.MILLISECONDS_PER_DAY));
        } // FOR
    }
    
    /**
     * testFlightId
     */
    public void testFlightId() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Timestamp flight_date : this.flight_dates) {
                        FlightId flight_id = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date);
                        assertNotNull(flight_id);
                        assertEquals(base_id, flight_id.getAirlineId());
                        assertEquals(depart_airport_id, flight_id.getDepartAirportId());
                        assertEquals(arrive_airport_id, flight_id.getArriveAirportId());
                        assertEquals(flight_date, flight_id.getDepartDate(this.start_date));
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
    
    /**
     * testFlightIdEncode
     */
    public void testFlightIdEncode() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Timestamp flight_date : this.flight_dates) {
                        long encoded = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date).encode();
                        assert(encoded >= 0) : "Invalid encoded value '" + encoded + "'";
                
                        FlightId flight_id = new FlightId(encoded);
                        assertNotNull(flight_id);
                        assertEquals(base_id, flight_id.getAirlineId());
                        assertEquals(depart_airport_id, flight_id.getDepartAirportId());
                        assertEquals(arrive_airport_id, flight_id.getArriveAirportId());
                        assertEquals(flight_date, flight_id.getDepartDate(this.start_date));
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
    
    /**
     * testFlightIdDecode
     */
    public void testFlightIdDecode() {
        for (long base_id : this.base_ids) {
            for (long depart_airport_id : this.depart_airport_ids) {
                for (long arrive_airport_id : this.arrive_airport_ids) {
                    for (Timestamp flight_date : this.flight_dates) {
                        long values[] = { base_id, depart_airport_id, arrive_airport_id, FlightId.calculateFlightDate(this.start_date, flight_date) };
                        long encoded = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date).encode();
                        assert(encoded >= 0) : "Invalid encoded value '" + encoded + "'";

                        long new_values[] = new FlightId(encoded).toArray();
                        assertEquals(values.length, new_values.length);
                        for (int i = 0; i < new_values.length; i++) {
                            assertEquals(values[i], new_values[i]);
                        } // FOR
                    } // FOR (time_code)
                } // FOR (arrive_airport_id)
            } // FOR (depart_airport_id)
        } // FOR (base_ids)
    }
}