package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

public class RandomString extends Random implements BaseUtil {

    private int minimum_length;
    private int maximum_length;
    private char base;
    private int numCharacters;

    public RandomString(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 4) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.minimum_length = ((Number) (int) values.get(0)).intValue();
        this.maximum_length = ((Number) (int) values.get(1)).intValue();
        this.base = (char) values.get(2);
        this.numCharacters = ((Number) (int) values.get(3)).intValue();
        if (maximum_length < minimum_length || numCharacters <= 0)
            throw new RuntimeException("Please enter correct min, max and no. of characters for random string");
    }

    public int number(int minimum, int maximum) {

        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;

        return value;
    }

    /**
     * @param minimum_length
     * @param maximum_length
     * @param base
     * @param numCharacters
     * @return
     */
    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        int length = number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }
}
