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

import java.io.IOException;
import java.sql.Timestamp;

import com.oltpbenchmark.util.JSONSerializable;
import com.oltpbenchmark.util.JSONUtil;
import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONObject;
import com.oltpbenchmark.util.json.JSONStringer;

public class ItemInfo implements JSONSerializable, Comparable<ItemInfo> {
    public ItemId itemId;
    public Float currentPrice;
    public Timestamp endDate;
    public long numBids = 0;
    public ItemStatus status = null;
    
    public ItemInfo(ItemId id, Double currentPrice, Timestamp endDate, int numBids) {
        this.itemId = id;
        this.currentPrice = (currentPrice != null ? currentPrice.floatValue() : null);
        this.endDate = endDate;
        this.numBids = numBids;
    }

    public ItemInfo() {
        // For serialization
    }
    
    public ItemId getItemId() {
        return (this.itemId);
    }
    public UserId getSellerId() {
        return (this.itemId.getSellerId());
    }
    public boolean hasCurrentPrice() {
        return (this.currentPrice != null);
    }
    public Float getCurrentPrice() {
        return currentPrice;
    }
    public boolean hasEndDate() {
        return (this.endDate != null);
    }
    public Timestamp getEndDate() {
        return endDate;
    }
    
    @Override
    public int compareTo(ItemInfo o) {
        return this.itemId.compareTo(o.itemId);
    }
    @Override
    public String toString() {
        return this.itemId.toString();
    }
    @Override
    public int hashCode() {
        return this.itemId.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj) 
            return true;
        
        if (!(obj instanceof ItemInfo) || obj == null)
            return false;
        
        return (this.itemId == obj || this.equals(((ItemInfo)obj).itemId));
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path) throws IOException {
        
    }
    @Override
    public void save(String output_path) throws IOException {
        
    }
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, ItemInfo.class, JSONUtil.getSerializableFields(ItemInfo.class));
    }
    @Override
    public void fromJSON(JSONObject json_object) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, this, ItemInfo.class, true, JSONUtil.getSerializableFields(ItemInfo.class));
    }
}