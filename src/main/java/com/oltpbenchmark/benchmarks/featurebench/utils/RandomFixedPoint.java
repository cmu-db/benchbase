package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

/*
     Description :- returns a random double in the range[minimum,maximum] with fixed decimal places.
     Params:-
     1. int: decimalPlaces(values[0]) :- fixed decimal points for the random number.
     2. double: minimum(values[1]) :- lower range for random number.
     3. double: maximum(values[2]):- upper range for random number.
     Eg:-
     decimalPlaces:- 3
     minimum:- 4
     maximum:- 6
     Return type:- (double) :- 4.301
*/

public class RandomFixedPoint extends Random implements BaseUtil {

    private final int decimalPlaces;
    private final double minimum;
    private final double maximum;

    public RandomFixedPoint(List<Object> values) {

        super((int) System.nanoTime());
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.decimalPlaces = ((Number) values.get(0)).intValue();
        this.minimum = ((Number) values.get(1)).doubleValue();
        this.maximum = ((Number) values.get(2)).doubleValue();
        if (maximum < minimum)
            throw new RuntimeException("Please enter a correct range for min and max values");
    }

    public int number(int minimum, int maximum) {

        int range_size = maximum - minimum + 1;
        int value = this.nextInt(range_size);
        value += minimum;

        return value;
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        int multiplier = 1;
        for (int i = 0; i < decimalPlaces; ++i) {
            multiplier *= 10;
        }

        int int_min = (int) (minimum * multiplier + 0.5);
        int int_max = (int) (maximum * multiplier + 0.5);

        return (double) this.number(int_min, int_max) / (double) multiplier;
    }
}
