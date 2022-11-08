package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class RandomStringTextArrayGen implements BaseUtil {

    private String[] textArray;

    private int minLengthOfString;
    private int maxLengthOfString;

    private int arraySize;


    public RandomStringTextArrayGen(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());

        }
        this.arraySize = ((Number) values.get(0)).intValue();
        if (arraySize <= 0)
            throw new RuntimeException("Please enter positive text array length");
        textArray = new String[arraySize];
        this.minLengthOfString = ((Number) values.get(1)).intValue();
        this.maxLengthOfString = ((Number) values.get(2)).intValue();
        if (minLengthOfString > maxLengthOfString || minLengthOfString < 0)
            throw new RuntimeException("Please enter correct bounds for max and min length");

    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < arraySize; i++) {
            String rd = (String) new RandomAString(List.of(minLengthOfString, maxLengthOfString)).run();
            textArray[i] = rd;
        }
        return textArray;
    }
}