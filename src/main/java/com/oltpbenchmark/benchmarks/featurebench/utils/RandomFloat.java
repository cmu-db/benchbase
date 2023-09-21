package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

public class RandomFloat  extends Random implements BaseUtil{

    private int lowerBound;
    private final int upperBound;
    private final double denominator ;



    public RandomFloat(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.lowerBound = ((Number) values.get(0)).intValue();
        this.upperBound = ((Number) values.get(1)).intValue();
        int decimalPoints = ((Number) values.get(2)).intValue();
        this.denominator = Math.pow(10, decimalPoints);

        if (lowerBound < 0 || upperBound < lowerBound || decimalPoints < 0) {
            throw new RuntimeException("Incorrect parameters for random no with decimal points");
        }
    }

    public RandomFloat(List<Object> values,int workerId, int totalWorkers) {
        super((int) System.nanoTime());
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        int divide = (((Number) values.get(1)).intValue() - ((Number) values.get(0)).intValue()) / totalWorkers;
        this.lowerBound = ((Number) values.get(0)).intValue()  + divide * workerId;
        int upperRangeTemp = (((Number) values.get(0)).intValue() + (divide) * (workerId + 1) + (workerId == 0 ? 0 : 1));
        this.upperBound = Math.min(upperRangeTemp, ((Number) values.get(1)).intValue());
        int decimalPoints = ((Number) values.get(2)).intValue();
        this.denominator = Math.pow(10, decimalPoints);

        if (lowerBound < 0 || upperBound < lowerBound || decimalPoints < 0) {
            throw new RuntimeException("Incorrect parameters for random no with decimal points");
        }
    }


    @Override
    public Object run() {
        int range_size = upperBound - lowerBound + 1;
        int value = this.nextInt(range_size);
        value += lowerBound;
        return  Math.round( Math.PI * value * denominator)/(denominator);
    }
}
