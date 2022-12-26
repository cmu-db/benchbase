package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.UUID;

public class RandomUUID implements BaseUtil {

    public RandomUUID(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
    }

    public RandomUUID(List<Object> values, int workerId, int totalWorkers) {
        this(values);
    }

    @Override
    public Object run() {
        return UUID.randomUUID();
    }
}

