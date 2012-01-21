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
package com.oltpbenchmark.util;

import java.io.IOException;

import com.oltpbenchmark.util.json.JSONException;
import com.oltpbenchmark.util.json.JSONObject;
import com.oltpbenchmark.util.json.JSONStringer;

/**
 * Pack multiple values into a single long using bit-shifting
 * @author pavlo
 */
public abstract class CompositeId implements Comparable<CompositeId>, JSONSerializable {
    
    private transient int hashCode = -1;
    
    protected final long encode(int...offset_bits) {
        long values[] = this.toArray();
        assert(values.length == offset_bits.length);
        long id = 0;
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            long max_value = (long)(Math.pow(2, offset_bits[i]) - 1l);

            assert(values[i] >= 0) :
                String.format("%s value at position %d is %d",
                              this.getClass().getSimpleName(), i, values[i]);
            assert(values[i] < max_value) :
                String.format("%s value at position %d is %d. Max value is %d",
                              this.getClass().getSimpleName(), i, values[i], max_value);
            
            id = (i == 0 ? values[i] : id | values[i]<<offset);
            offset += offset_bits[i];
        } // FOR
        this.hashCode = new Long(id).hashCode();
        return (id);
    }
    
    protected final long[] decode(long composite_id, int...offset_bits) {
        long values[] = new long[offset_bits.length];
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            long max_value = (long)(Math.pow(2, offset_bits[i]) - 1l);
            values[i] = (composite_id>>offset & max_value);
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
