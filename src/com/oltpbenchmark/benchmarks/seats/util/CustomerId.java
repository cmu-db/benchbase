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

public class CustomerId {

    private static final long BASE_ID_MASK = 281474976710655l; // (2^48)-1
    private static final int AIRPORT_ID_OFFSET = 48;
    
    private final long id;
    private final long depart_airport_id;
    private final Long encoded;
    
    public CustomerId(long id, long depart_airport_id) {
        this.id = id;
        this.depart_airport_id = depart_airport_id;
        this.encoded = CustomerId.encode(new long[]{this.id, this.depart_airport_id});
    }
    
    public CustomerId(long composite_id) {
        long values[] = CustomerId.decode(composite_id);
        this.id = values[0];
        this.depart_airport_id = values[1];
        this.encoded = composite_id;
    }
    
    /**
     * @return the id
     */
    public long getId() {
        return id;
    }

    /**
     * @return the depart_airport_id
     */
    public long getDepartAirportId() {
        return depart_airport_id;
    }
    
    public long encode() {
        return (this.encoded);
    }

    public static long encode(long...values) {
        assert(values.length == 2);
        return (values[0] | values[1]<<AIRPORT_ID_OFFSET);
    }
    
    public static long[] decode(long composite_id) {
        long values[] = { composite_id & BASE_ID_MASK,
                          composite_id>>AIRPORT_ID_OFFSET };
        return (values);
    }
    
    @Override
    public String toString() {
        return String.format("CustomerId{airport=%d,id=%d}", this.depart_airport_id, this.id);
    }
    
    @Override
    public int hashCode() {
        return (this.encoded.hashCode());
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CustomerId) {
            CustomerId o = (CustomerId)obj;
            return (this.id == o.id &&
                    this.depart_airport_id == o.depart_airport_id);
        }
        return (false);
    }
}