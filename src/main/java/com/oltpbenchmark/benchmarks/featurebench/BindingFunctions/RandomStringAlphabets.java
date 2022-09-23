package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

public class RandomStringAlphabets {

    protected int desiredLength;

    public RandomStringAlphabets(int desiredLength) {
        this.desiredLength = desiredLength;
    }

    public String getAlphaString() {
        String AlphaString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(desiredLength);
        for (int i = 0; i < desiredLength; i++) {
            int index = (int) (AlphaString.length() * Math.random());
            sb.append(AlphaString.charAt(index));
        }
        return sb.toString();
    }

}
