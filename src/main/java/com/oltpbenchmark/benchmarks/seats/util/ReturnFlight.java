/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.benchmarks.seats.util;

import com.oltpbenchmark.benchmarks.seats.SEATSConstants;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Objects;

public class ReturnFlight implements Comparable<ReturnFlight> {

    private final CustomerId customer_id;
    private final long return_airport_id;
    private final Timestamp return_date;

    public ReturnFlight(CustomerId customer_id, long return_airport_id, Timestamp flight_date, int return_days) {
        this.customer_id = customer_id;
        this.return_airport_id = return_airport_id;
        this.return_date = ReturnFlight.calculateReturnDate(flight_date, return_days);
    }

    /**
     * @param flight_date
     * @param return_days
     * @return
     */
    protected static Timestamp calculateReturnDate(Timestamp flight_date, int return_days) {

        // Round this to the start of the day
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(flight_date.getTime() + (return_days * SEATSConstants.MILLISECONDS_PER_DAY));

        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH);
        int day = cal.get(Calendar.DAY_OF_MONTH);

        cal.clear();
        cal.set(year, month, day);
        return (new Timestamp(cal.getTime().getTime()));
    }

    /**
     * @return the customer_id
     */
    public CustomerId getCustomerId() {
        return customer_id;
    }

    /**
     * @return the return_airport_id
     */
    public long getReturnAirportId() {
        return return_airport_id;
    }

    /**
     * @return the return_time
     */
    public Timestamp getReturnDate() {
        return return_date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReturnFlight that = (ReturnFlight) o;
        return return_airport_id == that.return_airport_id && Objects.equals(customer_id, that.customer_id) && Objects.equals(return_date, that.return_date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customer_id, return_airport_id, return_date);
    }

    @Override
    public int compareTo(ReturnFlight o) {
        if (this.customer_id.equals(o.customer_id) &&
                this.return_airport_id == o.return_airport_id &&
                this.return_date.equals(o.return_date)) {
            return (0);
        }
        // Otherwise order by time
        return (this.return_date.compareTo(o.return_date));
    }

    @Override
    public String toString() {
        return String.format("ReturnFlight{%s,airport=%s,date=%s}",
                this.customer_id, this.return_airport_id, this.return_date);
    }

}
