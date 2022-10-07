/*
package com.oltpbenchmark.benchmarks.featurebench.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.stream.IntStream;

*/
/**
 * Pack multiple values into a single long using bit-shifting
 *
 * @author pavlo
 *//*

public abstract class CompositeId {

    private static final String PAD_STRING = "0";
    public static final int INT_MAX_DIGITS = 10;
    public static final int LONG_MAX_DIGITS = 19;

    protected final String encode(int[] offset_bits) {
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

    protected final String[] decode(String composite_id, int[] offset_bits) {
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

*/
