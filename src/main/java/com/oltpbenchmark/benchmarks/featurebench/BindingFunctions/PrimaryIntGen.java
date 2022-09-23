package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;


public class PrimaryIntGen {
    protected int currentValue;
    protected int upperRange;
    protected int lowerRange;

    public PrimaryIntGen(int lowerRange, int upperRange) {
        this.currentValue = lowerRange - 1;
        this.upperRange = upperRange;
        this.lowerRange = lowerRange;
    }

    public long getValue() {
        return currentValue;
    }

    public int getNextValue() {
        if (currentValue == upperRange) {
            throw new RuntimeException("Out of bounds primary key access");
        }
        currentValue = findNextHigherValue();
        return currentValue;
    }

    private int findNextHigherValue() {
        currentValue++;
        return currentValue;
    }

}
