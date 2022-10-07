package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;

public class RandomNumber implements BaseUtil {

    final private int minimum;
    final private int maximum;

    public RandomNumber(List<Object> values) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.minimum = (int) values.get(0);
        this.maximum = (int) values.get(0);
    }

    @Override
    public Object run() {
        int range_size = maximum - minimum + 1;
        int value = minimum;
        return value;
    }
}
