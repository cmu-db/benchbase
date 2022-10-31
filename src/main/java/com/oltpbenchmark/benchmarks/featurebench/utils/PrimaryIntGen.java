package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.util.List;


/*
Description :- Integer Primary key generator between a range.
Params :
1.int: lowerRange (values[0]) :- Lower Range for Integer Primary key.
2.int: upperRange (values[1]) :- Upper Range for Integer Primary key.

Eg:-
lowerRange:- 10, upperRange: 20
Return type (Integer) :- Any value between 10 and 20 including these bounds.
*/

public class PrimaryIntGen implements BaseUtil {
    private int currentValue;
    private final int upperRange;
    private final int lowerRange;

    public PrimaryIntGen(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.currentValue = ((Number) values.get(0)).intValue() - 1;
        this.upperRange = ((Number) values.get(1)).intValue();
        this.lowerRange = ((Number) values.get(0)).intValue();
        if (upperRange < lowerRange) {
            throw new RuntimeException("Upper bound less than lower bound");
        }
    }

    private int findNextHigherValue() {
        currentValue++;
        return currentValue;
    }

    @Override
    public Object run() {
        if (currentValue >= upperRange) {
            throw new RuntimeException("Out of bounds primary key access");
        }
        currentValue = findNextHigherValue();
        return currentValue;
    }
}
