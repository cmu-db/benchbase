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


package com.oltpbenchmark.util;

import java.io.IOException;
import java.util.Arrays;

import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONObject;
import com.oltpbenchmark.util.json.JSONStringer;

/**
 * Pack multiple values into a single long using bit-shifting
 * @author pavlo
 */
public abstract class CompositeId implements Comparable<CompositeId>, JSONSerializable {
    
    private transient int hashCode = -1;
    
    protected static final long[] compositeBitsPreCompute(int offset_bits[]) {
        long pows[] = new long[offset_bits.length];
        for (int i = 0; i < offset_bits.length; i++) {
            pows[i] = (long)(Math.pow(2, offset_bits[i]) - 1l);
        } // FOR
        return (pows);
    }
    
    protected final long encode(int offset_bits[], long offset_pows[]) {
        long values[] = this.toArray();
        assert(values.length == offset_bits.length);
        long id = 0;
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            long max_value = offset_pows[i];

            assert(values[i] >= 0) :
                String.format("%s value at position %d is %d %s",
                              this.getClass().getSimpleName(), i, values[i], Arrays.toString(values));
            assert(values[i] < max_value) :
                String.format("%s value at position %d is %d. Max value is %d\n%s",
                              this.getClass().getSimpleName(), i, values[i], max_value, this);
            
            id = (i == 0 ? values[i] : id | values[i]<<offset);
            offset += offset_bits[i];
        } // FOR
        this.hashCode = (int)(id ^ (id >>> 32)); // From Long.hashCode()
        return (id);
    }
    
    protected final long[] decode(long composite_id, int offset_bits[], long offset_pows[]) {
        long values[] = new long[offset_bits.length];
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = (composite_id>>offset & offset_pows[i]);
            offset += offset_bits[i];
        } // FOR
        return (values);
    }
    
    public abstract long encode();
    public abstract void decode(long composite_id);
    public abstract long[] toArray();
    
    @Override
    public int compareTo(CompositeId o) {
        return Math.abs(this.hashCode()) - Math.abs(o.hashCode());
    }
    
    @Override
    public int hashCode() {
        if (this.hashCode == -1) {
            this.encode();
            assert(this.hashCode != -1);
        }
        return (this.hashCode);
    }
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(String input_path) throws IOException {
        JSONUtil.load(this, input_path);
    }
    @Override
    public void save(String output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        stringer.key("ID").value(this.encode());
    }
    @Override
    public void fromJSON(JSONObject json_object) throws JSONException {
        this.decode(json_object.getLong("ID"));
    }
}
