package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.util.List;


/**
 * Easily step from one value to the next according to a modified
 * logarithmic sequence that makes it easy to pick useful testing
 * boundaries.
 * <p>
 * With levels per magnitude at 1, the progression goes in powers
 * of 10. With any higher value than 1, each magnitude is divided
 * into equal parts. For example, starting at 10 with 2 levels per magnitude,
 * you get 50, 100, 500, 1000, 5000, and so on when you ask for
 * the next higher bound.
 */

public class Bounds implements BaseUtil {

    private final int levelsPerMagnitude;
    private long currentValue;


    public Bounds(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.currentValue = ((Number) values.get(0)).longValue();
        this.levelsPerMagnitude = ((Number) values.get(1)).intValue();
    }
    public Bounds(List<Object> values,int workerId,int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.currentValue = ((Number) values.get(0)).longValue();
        this.levelsPerMagnitude = ((Number) values.get(1)).intValue();
    }

    @Override
    public Object run() {
        long nextValue = findNextHigherValue();
        currentValue = nextValue;
        return currentValue;
    }

    private long findNextHigherValue() {
        int pow10 = (int) Math.log10(currentValue);
        if (levelsPerMagnitude == 1) {
            return (long) Math.pow(10, pow10 + 1);
        }
        double baseMagnitude = Math.pow(10, pow10);
        double increment = baseMagnitude / levelsPerMagnitude;

        long newValue = (long) (currentValue + increment);
        return newValue;
    }
}

