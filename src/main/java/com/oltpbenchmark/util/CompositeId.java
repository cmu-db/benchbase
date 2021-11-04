/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */


package com.oltpbenchmark.util;


import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.io.IOException;
import java.util.Arrays;

/**
 * Pack multiple values into a single long using bit-shifting
 *
 * @author pavlo
 */
public abstract class CompositeId {

    private transient int hashCode = -1;

    protected static long[] compositeBitsPreCompute(int[] offset_bits) {
        long[] pows = new long[offset_bits.length];
        for (int i = 0; i < offset_bits.length; i++) {
            pows[i] = (long) (Math.pow(2, offset_bits[i]) - 1L);
        }
        return (pows);
    }

    protected final long encode(int[] offset_bits, long[] offset_pows) {
        long[] values = this.toArray();

        long id = 0;
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            long max_value = offset_pows[i];

            if (values[i] < 0) {
                throw new IllegalArgumentException(String.format("%s value at position %d is %d %s",
                        this.getClass().getSimpleName(), i,
                        values[i], Arrays.toString(values)));
            }
            if (values[i] >= max_value) {
                throw new IllegalArgumentException(String.format("%s value at position %d is %d. Max value is %d\n",
                        this.getClass().getSimpleName(), i,
                        values[i], max_value));
            }

            id = (i == 0 ? values[i] : id | values[i] << offset);
            offset += offset_bits[i];
        }
        this.hashCode = (int) (id ^ (id >>> 32)); // From Long.hashCode()
        return (id);
    }

    protected final long[] decode(long composite_id, int[] offset_bits, long[] offset_pows) {
        long[] values = new long[offset_bits.length];
        int offset = 0;
        for (int i = 0; i < values.length; i++) {
            values[i] = (composite_id >> offset & offset_pows[i]);
            offset += offset_bits[i];
        }
        return (values);
    }

    public abstract long encode();

    public abstract void decode(long composite_id);

    public abstract long[] toArray();

    @Override
    public int hashCode() {
        if (this.hashCode == -1) {
            this.encode();

        }
        return (this.hashCode);
    }

}
