package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

import java.util.Random;

public class RandomBoolean {
    protected Random rd;

    public RandomBoolean() {
        Random rd = new Random();
    }

    public boolean getRandomBoolean() {
        if (this.rd == null) {
            this.rd = new Random();
        }
        return rd.nextBoolean();
    }
}

