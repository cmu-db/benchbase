package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;

public class RandomStringNumeric implements BaseUtil {
    protected int desiredLength;

    public RandomStringNumeric(List<Object> values) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.desiredLength = ((Number) values.get(0)).intValue();
        if (desiredLength <= 0)
            throw new RuntimeException("Please enter positive string length");
    }

    public RandomStringNumeric(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.desiredLength = ((Number) values.get(0)).intValue();
        if (desiredLength <= 0)
            throw new RuntimeException("Please enter positive string length");
    }


    @Override
    public Object run() {
        String NumericString = "0123456789";

        StringBuilder sb = new StringBuilder(desiredLength);
        for (int i = 0; i < desiredLength; i++) {
            int index = (int) (NumericString.length() * Math.random());
            sb.append(NumericString.charAt(index));
        }
        return sb.toString();
    }
}
