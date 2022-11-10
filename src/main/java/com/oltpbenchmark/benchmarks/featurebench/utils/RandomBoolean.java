package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

/*
 * Description :- Returns a random boolean value.
 * Params:- Empty list
 * Return type:- (boolean) True OR false
 * */
public class RandomBoolean implements BaseUtil {
    private Random rd;

    public RandomBoolean(List<Object> values) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        Random rd = new Random();
    }
    public RandomBoolean(List<Object> values,int workerId,int totalWorkers) {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        Random rd = new Random();
    }

    @Override
    public Object run() {
        if (this.rd == null) {
            this.rd = new Random();
        }
        return rd.nextBoolean();
    }
}

