package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomNumber implements BaseUtil {

    final private int minimum;
    final private int maximum;

    public RandomNumber(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimum = ((Number) values.get(0)).intValue();
        this.maximum = ((Number) values.get(1)).intValue();
    }

    @Override
    public Object run() {
        return ThreadLocalRandom.current().nextInt(minimum, maximum + 1);
    }
}
