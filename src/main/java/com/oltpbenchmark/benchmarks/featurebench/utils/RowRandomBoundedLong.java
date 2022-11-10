package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.util.List;


public class RowRandomBoundedLong implements BaseUtil {
    private final long lowValue;
    private final long highValue;

    public RowRandomBoundedLong(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowValue = ((Number) values.get(0)).longValue();
        this.highValue = ((Number) values.get(1)).longValue();
        if (lowValue > highValue)
            throw new RuntimeException("Please enter correct value for max and min value");
    }

    public RowRandomBoundedLong(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowValue = ((Number) values.get(0)).longValue();
        this.highValue = ((Number) values.get(1)).longValue();
        if (lowValue > highValue)
            throw new RuntimeException("Please enter correct value for max and min value");
    }

    @Override
    public Object run() {
        return lowValue + (long) (Math.random() * (highValue - lowValue));
    }
}
