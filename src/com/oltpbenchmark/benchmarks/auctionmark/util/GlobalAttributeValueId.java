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

public class GlobalAttributeValueId extends CompositeId {

    private static final int COMPOSITE_BITS[] = {
        32, // GROUP_ATTRIBUTE_ID
        8,  // ID
    };
    private static final long COMPOSITE_POWS[] = compositeBitsPreCompute(COMPOSITE_BITS);
    
    private long group_attribute_id;
    private int id;
    
    public GlobalAttributeValueId(long group_attribute_id, int id) {
        this.group_attribute_id = group_attribute_id;
        this.id = id;
    }
    
    public GlobalAttributeValueId(GlobalAttributeGroupId group_attribute_id, int id) {
        this(group_attribute_id.encode(), id);
    }
    
    public GlobalAttributeValueId(long composite_id) {
        this.decode(composite_id);
    }
    
    @Override
    public long encode() {
        return (super.encode(COMPOSITE_BITS, COMPOSITE_POWS));
    }

    @Override
    public void decode(long composite_id) {
        long values[] = super.decode(composite_id, COMPOSITE_BITS, COMPOSITE_POWS);
        this.group_attribute_id = (int)values[0];
        this.id = (int)values[1];
    }

    @Override
    public long[] toArray() {
        return (new long[]{ this.group_attribute_id, this.id });
    }
    
    public GlobalAttributeGroupId getGlobalAttributeGroup() {
        return new GlobalAttributeGroupId(this.group_attribute_id);
    }
    
    protected int getId() {
        return (this.id);
    }
}
