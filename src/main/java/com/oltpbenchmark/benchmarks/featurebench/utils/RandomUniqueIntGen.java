package com.oltpbenchmark.benchmarks.featurebench.utils;

import com.oltpbenchmark.DBWorkload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/*
Description :- Random Integer Primary key generator between a range with no duplicates.
Params :
1.int: lowerRange (values[0]) :- Lower Range for Integer Primary key.
2.int: upperRange (values[1]) :- Upper Range for Integer Primary key.

Supports worker-based partitioning:
Each worker receives a unique sub-range with randomized, non-repeating values.
*/

public class RandomUniqueIntGen implements BaseUtil {
    private final List<Integer> shuffledValues;
    private int index;
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkload.class);
    public RandomUniqueIntGen(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function " + this.getClass());
        }

        int lowerRange = ((Number) values.get(0)).intValue();
        int upperRange = ((Number) values.get(1)).intValue();

        if (upperRange < lowerRange) {
            throw new RuntimeException("Upper bound less than lower bound");
        }

        this.shuffledValues = new ArrayList<>();
        for (int i = lowerRange; i <= upperRange; i++) {
            this.shuffledValues.add(i);
        }

        Collections.shuffle(this.shuffledValues, new Random());
        this.index = 0;
    }

    public RandomUniqueIntGen(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function " + this.getClass());
        }

        int lower = ((Number) values.get(0)).intValue();
        int upper = ((Number) values.get(1)).intValue();

        if (upper < lower) {
            throw new RuntimeException("Upper bound less than lower bound");
        }

        int totalValues = upper - lower + 1;
        int baseChunkSize = totalValues / totalWorkers;
        int remainder = totalValues % totalWorkers;

        int startIndex = workerId * baseChunkSize + Math.min(workerId, remainder);
        int chunkSize = baseChunkSize + (workerId < remainder ? 1 : 0);

        int startValue = lower + startIndex;
        int endValue = startValue + chunkSize - 1;

        if (startValue > upper || chunkSize <= 0) {
            throw new RuntimeException("Worker " + workerId + " has no range to work on.");
        }

        this.shuffledValues = new ArrayList<>();
        for (int i = startValue; i <= endValue; i++) {
            this.shuffledValues.add(i);
        }

        Collections.shuffle(this.shuffledValues, new Random());
        this.index = 0;
    }

    @Override
    public Object run() {
        if (index >= shuffledValues.size()) {
            throw new RuntimeException("No more unique values to generate in the given range.");
        }
        return shuffledValues.get(index++);
    }
}
