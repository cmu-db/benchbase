package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;

public class PrimaryFloatGen  implements BaseUtil{

    private int lowerBound;
    private final int upperBound;
    private final double denominator ;



    public PrimaryFloatGen(List<Object> values) {
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

    public PrimaryFloatGen(List<Object> values,int workerId, int totalWorkers) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        int divide = (((Number) values.get(1)).intValue() - ((Number) values.get(0)).intValue()) / totalWorkers;
        this.lowerBound = ((Number) values.get(0)).intValue() + divide * workerId;
        int upperRangeTemp = (((Number) values.get(0)).intValue() + (divide) * (workerId + 1) + (workerId == 0 ? 0 : 1));
        this.upperBound = Math.min(upperRangeTemp, ((Number) values.get(1)).intValue());
        int decimalPoints = ((Number) values.get(2)).intValue();
        this.denominator = Math.pow(10, decimalPoints);

        if (lowerBound < 0 || upperBound < lowerBound || decimalPoints < 0) {
            throw new RuntimeException("Incorrect parameters for random no with decimal points");
        }
    }

    private int findNextHigherValue() {
        return lowerBound++;
    }

    @Override
    public Object run() {
        int curr = findNextHigherValue();
        return  Math.round( Math.PI * curr * denominator)/(denominator);
    }
}
