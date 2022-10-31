package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.Random;

/*
Description :- Returns a random date in range[yearlowerBound,yearupperBound]
Params :
1.int: yearlowerBound (values[0]) :- lower bound for random year generation.
2.int: yearupperBound (values[1]) :- upper bound for random year generation.

Eg:-
yearlowerBound:- 2009
yearupperBound:- 2020
Return type : (String):- 24-4-2014
*/
public class RandomDate implements BaseUtil {
    private Random rd;
    private final int yearlowerBound;
    private final int yearupperBound;


    public RandomDate(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        Random rnd = new Random();
        this.yearlowerBound = ((Number) values.get(0)).intValue();
        this.yearupperBound = ((Number) values.get(1)).intValue();
        if (yearlowerBound > yearupperBound || yearlowerBound == 0 && yearupperBound == 0 || yearlowerBound < 0)
            throw new RuntimeException("Please enter correct bounds for max and min year");

    }

    @Override
    public Object run() {
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
