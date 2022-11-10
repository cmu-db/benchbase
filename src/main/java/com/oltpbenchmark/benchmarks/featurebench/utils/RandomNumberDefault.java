package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomNumberDefault implements BaseUtil {

    private final int lowerBound = Integer.MIN_VALUE;
    private final int upperBound = Integer.MAX_VALUE - 1;


    public RandomNumberDefault(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

    }

    public RandomNumberDefault(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        return ThreadLocalRandom.current().nextInt(lowerBound, upperBound + 1);
    }
}