package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

/*
     Description :- returns a random alphabetic string with length in range [minimumlength,maximumlength].
     Params:-
     1. int: minimumLength(values[0]) :- minimum length of random alphabetic string.
     2. int: maximumLength(values[1]):- maximum length of random alphabetic string.
     Return type:- String (Alphabetic)
*/

public class RandomAString extends Random implements BaseUtil {

    private final int minimumLength;
    private final int maximumLength;

    public RandomAString(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimumLength = ((Number) values.get(0)).intValue();
        this.maximumLength = ((Number) values.get(1)).intValue();
        if (minimumLength > maximumLength || minimumLength == 0 && maximumLength == 0 || minimumLength < 0)
            throw new RuntimeException("Please enter correct bounds for max and min length");
    }

    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     * maximum_length].
     */
    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        return randomString(minimumLength, maximumLength, 'a', 26);
    }

    private String randomString(int minimumLength, int maximumLength, char base, int numCharacters) {
        int length = number(minimumLength, maximumLength);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    public int number(int minimum, int maximum) {

        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;

        return value;
    }

}
