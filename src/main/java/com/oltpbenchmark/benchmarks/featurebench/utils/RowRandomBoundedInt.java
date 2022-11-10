package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;


public class RowRandomBoundedInt implements BaseUtil {
    private final int lowValue;
    private final int highValue;

    public RowRandomBoundedInt(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowValue = ((Number) values.get(0)).intValue();
        this.highValue = ((Number) values.get(1)).intValue();
        if (lowValue > highValue)
            throw new RuntimeException("Please enter correct value for max and min value");

    }

    public RowRandomBoundedInt(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowValue = ((Number) values.get(0)).intValue();
        this.highValue = ((Number) values.get(1)).intValue();
        if (lowValue > highValue)
            throw new RuntimeException("Please enter correct value for max and min value");

    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        return ThreadLocalRandom.current().nextInt(lowValue, highValue + 1);
    }
}

