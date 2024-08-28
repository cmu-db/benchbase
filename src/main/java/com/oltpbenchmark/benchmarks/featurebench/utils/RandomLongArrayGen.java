package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomLongArrayGen implements BaseUtil {

    private List<Long> longArray;

    private Long minValue;
    private Long maxValue;
    private int arraySize;

    public RandomLongArrayGen(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.arraySize = ((Number) values.get(0)).intValue();
        if (arraySize <= 0)
            throw new RuntimeException("Please enter positive integer array length");
        this.minValue = ((Number) values.get(1)).longValue();
        this.maxValue = ((Number) values.get(2)).longValue();
        if (minValue > maxValue)
            throw new RuntimeException("Please enter correct bounds for max and min value");
    }

    public RandomLongArrayGen(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.arraySize = ((Number) values.get(0)).intValue();
        if (arraySize <= 0)
            throw new RuntimeException("Please enter positive integer array length");
        this.minValue = ((Number) values.get(1)).longValue();
        this.maxValue = ((Number) values.get(2)).longValue();
        if (minValue > maxValue)
            throw new RuntimeException("Please enter correct bounds for max and min value");
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        Random random = new Random();
        longArray = new ArrayList<>();
        for (int i = 0; i < arraySize; i++) {
            Long rd = random.nextLong((maxValue - minValue) + 1) + minValue;
            longArray.add(rd);
        }
        return longArray.toString().replaceFirst("\\[", "{").replace("]", "}");
    }
}

