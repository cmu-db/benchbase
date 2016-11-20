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