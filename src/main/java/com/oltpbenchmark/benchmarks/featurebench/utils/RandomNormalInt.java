package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomNormalInt extends Random implements BaseUtil {

    private final int center;
    private final int deviation;

    public RandomNormalInt(List<Object> values) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.center = ((Number) values.get(0)).intValue();
        this.deviation = ((Number) values.get(1)).intValue();
    }

    /**
     * Returns a random normal distribution int value with average equal to center
     *
     * @param center
     * @param deviation
     */
    @Override
    public Object run() {
        double r = this.nextGaussian() * deviation + center;
        return (int) Math.round(r);
    }
}


