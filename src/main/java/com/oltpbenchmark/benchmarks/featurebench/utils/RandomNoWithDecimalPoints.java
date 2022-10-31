package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomNoWithDecimalPoints implements BaseUtil {

    private final int lowerBound;
    private final int upperBound;
    private final int decimalPoints;

    public RandomNoWithDecimalPoints(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowerBound = ((Number) values.get(0)).intValue();
        this.upperBound = ((Number) values.get(1)).intValue();
        this.decimalPoints = ((Number) values.get(2)).intValue();
        if (lowerBound < 0 || upperBound < lowerBound || decimalPoints < 0) {
            throw new RuntimeException("Incorrect parameters for random no with decimal points");
        }
    }

    @Override
    public Object run() {
        Random rnd = new Random();
        double randomNo = lowerBound + (upperBound - lowerBound) * rnd.nextDouble();
        return (double) Math.round(randomNo * (Math.pow(10, decimalPoints))) / (Math.pow(10, decimalPoints));
    }
}
