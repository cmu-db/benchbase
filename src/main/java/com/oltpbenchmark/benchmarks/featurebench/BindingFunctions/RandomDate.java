package com.oltpbenchmark.benchmarks.featurebench.BindingFunctions;

import java.util.Random;


public class RandomDate {
    protected Random rd;
    protected int yearlowerBound;
    protected int yearupperBound;

    public RandomDate(int yearlowerBound, int yearupperBound) {
        Random rnd = new Random();
        this.yearlowerBound = yearlowerBound;
        this.yearupperBound = yearupperBound;

    }

    public String getRandomDate() {
        if (this.rd == null) {
            this.rd = new Random();
        }
        int year = rd.nextInt(yearupperBound - yearlowerBound) + yearlowerBound;
        int month = rd.nextInt(12 - 1) + 1;
        int day = rd.nextInt(30 - 1) + 1;
        String date = day + "-" + month + "-" + year;
        return date;
    }
}
