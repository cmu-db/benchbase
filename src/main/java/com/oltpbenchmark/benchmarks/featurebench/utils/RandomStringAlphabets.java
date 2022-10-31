package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;

public class RandomStringAlphabets implements BaseUtil {

    protected int desiredLength;

    public RandomStringAlphabets(List<Object> values) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.desiredLength = ((Number) values.get(0)).intValue();
        if (desiredLength <= 0)
            throw new RuntimeException("Please enter positive string length");
    }

    @Override
    public Object run() {
        String AlphaString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvxyz";

        StringBuilder sb = new StringBuilder(desiredLength);
        for (int i = 0; i < desiredLength; i++) {
            int index = (int) (AlphaString.length() * Math.random());
            sb.append(AlphaString.charAt(index));
        }
        return sb.toString();
    }
}
