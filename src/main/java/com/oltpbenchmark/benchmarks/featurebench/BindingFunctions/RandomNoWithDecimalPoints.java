package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

import java.util.Random;

public class RandomNoWithDecimalPoints {

    protected int lowerBound;
    protected int upperBound;
    protected int decimalPoints;

    public RandomNoWithDecimalPoints(int lowerBound, int upperBound, int decimalPoints) {
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.decimalPoints = decimalPoints;
        if (lowerBound < 0 || upperBound <= lowerBound || decimalPoints < 0) {
            throw new IllegalArgumentException("error");
        }
    }

    public double getRandomNoWithDecimalPoints() {
        Random rnd = new Random();
        double randomNo = lowerBound + (upperBound - lowerBound) * rnd.nextDouble();
        double DecimalPointNumber = (double) Math.round(randomNo * (Math.pow(10, decimalPoints))) / (Math.pow(10, decimalPoints));
        return DecimalPointNumber;
    }

}
