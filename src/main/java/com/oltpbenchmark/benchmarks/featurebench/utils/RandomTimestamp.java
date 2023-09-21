package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.sql.Timestamp;
import java.util.List;
import java.util.Random;

public class RandomTimestamp implements BaseUtil{

    private final int numberofTimestamp;

    long startepoch = 1672511400000L;


    private final Random rd = new Random(System.currentTimeMillis());

    public RandomTimestamp(List<Object> values) {
        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberofTimestamp = ((Number) values.get(0)).intValue();

        if (numberofTimestamp <0)
            throw new RuntimeException("Please enter positive number of days");

    }

    public RandomTimestamp(List<Object> values, int workerId, int totalWorkers) {

        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberofTimestamp = values.get(0) instanceof String? Integer.parseInt((String) values.get(0)):  ((Number) values.get(0)).intValue();

        if (numberofTimestamp <0)
            throw new RuntimeException("Please enter positive number of days");

    }
    @Override
    public Object run() {
        int offset = rd.nextInt(numberofTimestamp);
        return new Timestamp(startepoch + offset* 10000000L);
    }
}
