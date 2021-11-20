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

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;
import junit.framework.TestCase;

import java.sql.Timestamp;
import java.util.Calendar;

public class TestFlightId extends TestCase {

    private final long[] base_ids = {111, 222, 333};
    private final long[] depart_airport_ids = {444, 555, 666};
    private final long[] arrive_airport_ids = {777, 888, 999};
    private final int[] flight_offset_days = {1, 2, 4, 8};
    private final Timestamp[] flight_dates = new Timestamp[this.flight_offset_days.length];
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
                        assertEquals(flight_date, flight_id.getDepartDateAsTimestamp(this.start_date));
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
                        String encoded = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date).encode();

                        FlightId flight_id = new FlightId(encoded);
                        assertNotNull(flight_id);
                        assertEquals(base_id, flight_id.getAirlineId());
                        assertEquals(depart_airport_id, flight_id.getDepartAirportId());
                        assertEquals(arrive_airport_id, flight_id.getArriveAirportId());
                        assertEquals(flight_date, flight_id.getDepartDateAsTimestamp(this.start_date));
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
                        String[] values = {String.valueOf(base_id), String.valueOf(depart_airport_id), String.valueOf(arrive_airport_id), String.valueOf(FlightId.calculateFlightDate(this.start_date, flight_date))};
                        String encoded = new FlightId(base_id, depart_airport_id, arrive_airport_id, this.start_date, flight_date).encode();

                        String[] new_values = new FlightId(encoded).toArray();
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