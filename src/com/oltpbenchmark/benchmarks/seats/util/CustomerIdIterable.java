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

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.collections15.set.ListOrderedSet;

import com.oltpbenchmark.util.Histogram;

public class CustomerIdIterable implements Iterable<CustomerId> {
    private final Histogram<Long> airport_max_customer_id;
    private final ListOrderedSet<Long> airport_ids = new ListOrderedSet<Long>();
    private Long last_airport_id = null;
    private int last_id = -1;
    private long last_max_id = -1;
    
    public CustomerIdIterable(Histogram<Long> airport_max_customer_id, long...airport_ids) {
        this.airport_max_customer_id = airport_max_customer_id;
        for (long id : airport_ids) {
            this.airport_ids.add(id);
        } // FOR
    }
    
    public CustomerIdIterable(Histogram<Long> airport_max_customer_id) {
        this.airport_max_customer_id = airport_max_customer_id;
        this.airport_ids.addAll(airport_max_customer_id.values());
    }
    
    public CustomerIdIterable(Histogram<Long> airport_max_customer_id, Collection<Long> airport_ids) {
        this.airport_max_customer_id = airport_max_customer_id;
        this.airport_ids.addAll(airport_ids);
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
                if (++last_id == last_max_id) last_airport_id = null;
                return next_id;
            }
            @Override
            public void remove() {
                // Not implemented
            }
        };
    }
} // END CLASS
