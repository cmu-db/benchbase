package com.oltpbenchmark.benchmarks.featurebench.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class PrimaryIntRandomForExecutePhase implements BaseUtil{
    private static final Logger LOG = LoggerFactory.getLogger(PrimaryIntGen.class);
    private final int upperRange;
    private final int lowerRange;
    private int currentValue;

    public PrimaryIntRandomForExecutePhase(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        int low = (int) values.get(0);
        int high = (int) values.get(1);

        int diff = (high - low) + 1 == high ? Math.floorDiv(high, totalWorkers) : Math.floorDiv((high-low), totalWorkers);

        this.lowerRange = workerId == 0 ? low : low + workerId * diff + 1;
        this.upperRange = Math.min(low + diff* (workerId+1), high);
        this.currentValue = this.lowerRange;
        LOG.info("lowerRange: {} upperRange: {} currentVal: {} totalWorkers: {} workerID: {}", lowerRange, upperRange, currentValue, totalWorkers, workerId);
        if (upperRange < lowerRange) {
            throw new RuntimeException("Upper bound less than lower bound");
        }

    }

    @Override
    public Object run() {
        return ThreadLocalRandom.current().nextInt(lowerRange, upperRange);
    }
}
