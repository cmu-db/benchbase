package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

public class RandomStringNumeric {
    protected int desiredLength;

    public RandomStringNumeric(int desiredLength) {
        this.desiredLength = desiredLength;
    }


    public String getNumericString() {
        String NumericString = "0123456789";

        StringBuilder sb = new StringBuilder(desiredLength);
        for (int i = 0; i < desiredLength; i++) {
            int index = (int) (NumericString.length() * Math.random());
            sb.append(NumericString.charAt(index));
        }
        return sb.toString();
    }
}
