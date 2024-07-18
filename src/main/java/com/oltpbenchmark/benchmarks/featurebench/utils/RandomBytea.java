package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

/*
     Description :- returns a random hexadecimal string with length in range [minimumlength,maximumlength].
     Params:-
     1. int: minimumLength(values[0]) :- minimum length of random hexadecimal string.
     2. int: maximumLength(values[1]):- maximum length of random hexadecimal string.
     Return type:- String (Hexadecimal)
*/

public class RandomBytea extends Random implements BaseUtil {

    private final int minimumLength;
    private final int maximumLength;

    public RandomBytea(List<Object> values) {
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
    public RandomBytea(List<Object> values, int workerId, int totalWorkers) {
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
     * @returns a random byte array with length in range [minimumLength, maximumLength].
     */
    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        return randomByteArray(minimumLength, maximumLength);
    }

    private byte[] randomByteArray(int minimumLength, int maximumLength) {
        int length = number(minimumLength, maximumLength);
        byte[] bytes = new byte[length];
        this.nextBytes(bytes);
        return bytes;
    }

    public int number(int minimum, int maximum) {
        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;
        return value;
    }

}
