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
