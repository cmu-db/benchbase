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

import com.oltpbenchmark.util.CompositeId;

import java.util.Comparator;
import java.util.Objects;

public class CustomerId extends CompositeId implements Comparable<CustomerId> {

    private static final int[] COMPOSITE_BITS = {
            INT_MAX_DIGITS, // ID
            LONG_MAX_DIGITS // AIRPORT_ID
    };

    private int id;
    private long depart_airport_id;

    public CustomerId(int id, long depart_airport_id) {
        this.id = id;
        this.depart_airport_id = depart_airport_id;
    }

    public CustomerId(String composite_id) {
        this.decode(composite_id);
    }

    @Override
    public String encode() {
        return (this.encode(COMPOSITE_BITS));
    }

    @Override
    public void decode(String composite_id) {
        String[] values = super.decode(composite_id, COMPOSITE_BITS);
        this.id = Integer.parseInt(values[0]);
        this.depart_airport_id = Long.parseLong(values[1]);
    }

    @Override
    public String[] toArray() {
        return (new String[]{Integer.toString(this.id), Long.toString(this.depart_airport_id)});
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @return the depart_airport_id
     */
    public long getDepartAirportId() {
        return depart_airport_id;
    }

    @Override
    public String toString() {
        return String.format("CustomerId{airport=%d,id=%d}", this.depart_airport_id, this.id);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomerId that = (CustomerId) o;
        return id == that.id && depart_airport_id == that.depart_airport_id;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, depart_airport_id);
    }

    @Override
    public int compareTo(CustomerId o) {
        return Comparator.comparingInt(CustomerId::getId)
                .thenComparingLong(CustomerId::getDepartAirportId)
                .compare(this, o);
    }
}