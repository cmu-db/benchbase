package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomExpoLong extends Random implements BaseUtil {
    private final long center;
    private final long deviation;

    public RandomExpoLong(List<Object> values) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.center = ((Number) values.get(0)).longValue();
        this.deviation = ((Number) values.get(1)).longValue();
    }

    public RandomExpoLong(List<Object> values, int workerId, int totalWorkers) {
        super((int) System.nanoTime());

        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.center = ((Number) values.get(0)).longValue();
        this.deviation = ((Number) values.get(1)).longValue();
    }

    /**
     * Returns a random exponential distribution long value with average equal to center
     */
    @Override
    public Object run() {
        double r = Math.log(1 - this.nextDouble()) / (-deviation) + center;
        return (long) Math.round(r);
    }

}
