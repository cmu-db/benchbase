package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomExpoInt extends Random implements BaseUtil {
    private final int center;
    private final int deviation;

    public RandomExpoInt(List<Object> values) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.center = ((Number) values.get(0)).intValue();
        this.deviation = ((Number) values.get(1)).intValue();
    }

    public RandomExpoInt(List<Object> values, int workerId, int totalWorkers) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.center = ((Number) values.get(0)).intValue();
        this.deviation = ((Number) values.get(1)).intValue();
    }

    /**
     * Returns a random exponential distribution int value with average equal to center
     */
    @Override
    public Object run() {
        double r = Math.log(1 - this.nextDouble()) / (-deviation) + center;
        return (int) Math.round(r);
    }
}
