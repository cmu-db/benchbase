package com.oltpbenchmark.benchmarks.featurebench.helpers;

public abstract class UtilGenerators {
    private static int counter;
    private static int starterForStringCounter = 0;
    private static int upperRangeForPrimaryIntKeys;
    private static int lowerRangeForPrimaryIntKeys;
    private static int desiredLengthStringPkeys;

    private static int minLenString;
    private static int maxLenString;


    public static int getUpperRangeForPrimaryIntKeys() {
        return upperRangeForPrimaryIntKeys;
    }

    public static void setUpperRangeForPrimaryIntKeys(int upperRangeForPrimaryIntKeys) {
        UtilGenerators.upperRangeForPrimaryIntKeys = upperRangeForPrimaryIntKeys;
    }

    public static int getLowerRangeForPrimaryIntKeys() {
        return lowerRangeForPrimaryIntKeys;
    }

    public static void setLowerRangeForPrimaryIntKeys(int lowerRangeForPrimaryIntKeys) {
        UtilGenerators.lowerRangeForPrimaryIntKeys = lowerRangeForPrimaryIntKeys;
        counter = lowerRangeForPrimaryIntKeys;
    }

    public static int getDesiredLengthStringPkeys() {
        return desiredLengthStringPkeys;
    }

    public static void setDesiredLengthStringPkeys(int desiredLengthStringPkeys) {
        UtilGenerators.desiredLengthStringPkeys = desiredLengthStringPkeys;
    }

    public static int getIntPrimaryKey() {
        if (counter > upperRangeForPrimaryIntKeys) {
            throw new RuntimeException("Index out of range");
        } else {
            counter++;
            return counter;
        }
    }

    public static String numberToIdString() {
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(starterForStringCounter));
        while (baseNumberStr.length() < desiredLengthStringPkeys) {
            baseNumberStr.append('a');
        }
        starterForStringCounter++;
        return baseNumberStr.toString();
    }

    public static int getMinLenString() {
        return minLenString;
    }

    public static void setMinLenString(int minLenString) {
        UtilGenerators.minLenString = minLenString;
    }

    public static int getMaxLenString() {
        return maxLenString;
    }

    public static void setMaxLenString(int maxLenString) {
        UtilGenerators.maxLenString = maxLenString;
    }
}
