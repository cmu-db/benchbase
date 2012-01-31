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

import com.oltpbenchmark.util.CompositeId;

public class CustomerId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        48, // ID
        16, // AIRPORT_ID
    };
    private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);
    
    private int id;
    private long depart_airport_id;
    
    public CustomerId(int id, long depart_airport_id) {
        this.id = id;
        this.depart_airport_id = depart_airport_id;
    }
    
    public CustomerId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (this.encode(COMPOSITE_BITS, COMPOSITE_POWS));
    }

    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.id = (int)values[0];
        this.depart_airport_id = values[1];
    }

    @Override
    public long[] toArray() {
        return (new long[]{ this.id, this.depart_airport_id });
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

}