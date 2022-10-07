package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

/*
     Description :- Returns a random int value between minimum and maximum (inclusive).
     Params:-
     1. int: minimum(values[0]) :- lower range for random number.
     2. int: maximum(values[1]) :- upper range for random number.
     Eg:-
     minimum:- 4
     maximum:- 10
     Return type:- (int) :- 7
*/

public class RandomInt extends Random implements BaseUtil {

    private final int minimum;
    private final int maximum;

    public RandomInt(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.minimum = ((Number) values.get(0)).intValue();
        this.maximum = ((Number) values.get(1)).intValue();
        if (maximum < minimum)
            throw new RuntimeException("Please enter correct values for min and max");
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;
        return value;
    }
}
