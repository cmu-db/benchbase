package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

public class RandomString extends Random implements BaseUtil {

    private int minimumLength;
    private int maximumLength;
    private char base;
    private int numCharacters;

    public RandomString(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 4) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimumLength = ((Number) values.get(0)).intValue();
        this.maximumLength = ((Number) values.get(1)).intValue();
        this.base = (char) values.get(2);
        this.numCharacters = ((Number) values.get(3)).intValue();
        if (maximumLength < minimumLength || numCharacters <= 0)
            throw new RuntimeException("Please enter correct min, max and no. of characters for random string");
    }

    public int number(int minimum, int maximum) {

        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;

        return value;
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        int length = number(minimumLength, maximumLength);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }
}
