/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
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
package com.oltpbenchmark.benchmarks.auctionmark.util;

import com.oltpbenchmark.util.CompositeId;

public class GlobalAttributeGroupId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        16, // CATEGORY
        8,  // ID
        8   // COUNT
    };
    private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);
    
    private int category_id;
    private int id;
    private int count;
    
    public GlobalAttributeGroupId(int category_id, int id, int count) {
        this.category_id = category_id;
        this.id = id;
        this.count = count;
    }
    
    public GlobalAttributeGroupId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (this.encode(COMPOSITE_BITS, COMPOSITE_POWS));
    }

    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.category_id = (int)values[0];
        this.id = (int)values[1];
        this.count = (int)values[2];
    }

    @Override
    public long[] toArray() {
        return (new long[]{ this.category_id, this.id, this.count });
    }
    
    public int getCategoryId() {
        return (this.category_id);
    }
    protected int getId() {
        return (this.id);
    }
    public int getCount() {
        return (this.count);
    }
}
