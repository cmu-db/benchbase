package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomExpoFloat extends Random implements BaseUtil {
    private final double center;
    private final double deviation;

    public RandomExpoFloat(List<Object> values) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.center = ((Number) values.get(0)).doubleValue();
        this.deviation = ((Number) values.get(1)).doubleValue();
    }

    /**
     * Returns a random exponential distribution float value with average equal to center
     */
    @Override
    public Object run() {
        double r = Math.log(1 - this.nextDouble()) / (-deviation) + center;
        return r;
    }
}
