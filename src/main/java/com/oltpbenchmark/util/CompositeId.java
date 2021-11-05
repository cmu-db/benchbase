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


import org.apache.commons.lang3.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;

/**
 * Pack multiple values into a single long using bit-shifting
 *
 * @author pavlo
 */
public abstract class CompositeId {

    public static final String PAD_STRING = "0";

    protected static long[] compositeBitsPreCompute(int[] offset_bits) {
        long[] pows = new long[offset_bits.length];
        for (int i = 0; i < offset_bits.length; i++) {
            pows[i] = (long) (Math.pow(2, offset_bits[i]) - 1L);
        }
        return (pows);
    }

    protected final String encode(int[] offset_bits, long[] offset_pows) {
        int encodedStringSize = IntStream.of(offset_bits).sum();
        StringBuilder compositeBuilder = new StringBuilder(encodedStringSize);

        String[] decodedValues = this.toArray();
        for (int i = 0; i < decodedValues.length; i++) {
            String value = decodedValues[i];
            int valueLength = offset_bits[i];
            String encodedValue = StringUtils.leftPad(value, valueLength, PAD_STRING);
            compositeBuilder.append(encodedValue);
        }

        return compositeBuilder.toString();
    }

    protected final String[] decode(String composite_id, int[] offset_bits, long[] offset_pows) {
        String[] decodedValues = new String[offset_bits.length];

        int start = 0;
        for (int i = 0; i < decodedValues.length; i++) {
            int valueLength = offset_bits[i];
            int end = start + valueLength;
            decodedValues[i] = StringUtils.substring(composite_id, start, end);
            start = end;
        }
        return decodedValues;
    }

    public abstract String encode();

    public abstract void decode(String composite_id);

    public abstract String[] toArray();


}
