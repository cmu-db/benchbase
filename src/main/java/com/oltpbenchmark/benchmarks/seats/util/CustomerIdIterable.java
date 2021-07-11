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

import com.oltpbenchmark.util.Histogram;
import org.apache.commons.collections4.set.ListOrderedSet;

import java.util.Iterator;

public class CustomerIdIterable implements Iterable<CustomerId> {
    private final Histogram<Long> airport_max_customer_id;
    private final ListOrderedSet<Long> airport_ids = new ListOrderedSet<>();
    private Long last_airport_id = null;
    private int last_id = -1;
    private long last_max_id = -1;

    public CustomerIdIterable(Histogram<Long> airport_max_customer_id) {
        this.airport_max_customer_id = airport_max_customer_id;
        this.airport_ids.addAll(airport_max_customer_id.values());
    }

    @Override
    public Iterator<CustomerId> iterator() {
        return new Iterator<CustomerId>() {
            @Override
            public boolean hasNext() {
                return (!CustomerIdIterable.this.airport_ids.isEmpty() || (last_id != -1 && last_id < last_max_id));
            }

            @Override
            public CustomerId next() {
                if (last_airport_id == null) {
                    last_airport_id = airport_ids.remove(0);
                    last_id = 0;
                    last_max_id = airport_max_customer_id.get(last_airport_id);
                }
                CustomerId next_id = new CustomerId(last_id, last_airport_id);
                if (++last_id == last_max_id) {
                    last_airport_id = null;
                }
                return next_id;
            }

            @Override
            public void remove() {
                // Not implemented
            }
        };
    }
}
