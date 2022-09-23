package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

public class PrimaryStringGen {
    protected int currentValue;
    protected int desiredLength;

    protected String key;

    public PrimaryStringGen(int startNumber, int desiredLength) {
        this.currentValue = startNumber - 1;
        this.desiredLength = desiredLength;
    }

    public String getValue() {
        return key;
    }

    public String getNextValue() {
        currentValue++;
        key = numberToIdString(currentValue);
        return key;
    }

    public String numberToIdString(int number) {
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(currentValue));
        while (baseNumberStr.length() < desiredLength) {
            baseNumberStr.append('a');
        }
        return baseNumberStr.toString();
    }

}


