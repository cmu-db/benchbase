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
