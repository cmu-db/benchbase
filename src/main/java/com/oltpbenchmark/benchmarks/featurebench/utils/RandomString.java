package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;

public class RandomString extends Random implements BaseUtil {


    /*
     Description :- returns a random string '{number}aaaaaa' Here number will lie between minimum and maximum value  .
     Params:-
     1. int: minimumNumber(values[0]) :- minimum number.
     2. int: maximumNumber(values[1]):- maximum number.
     3. int: Length(values[2]):- Length of string
     Return type:- String ('{number}aaaaaa')
*/
    private int minimumNumber;
    private int maximumNumber;
    private int length;

    public RandomString(List<Object> values) {
        super((int) System.nanoTime());
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimumNumber = ((Number) values.get(0)).intValue();
        this.maximumNumber = ((Number) values.get(1)).intValue();
        this.length = ((Number) values.get(2)).intValue();
        if (maximumNumber < minimumNumber || length <= 0)
            throw new RuntimeException("Please enter correct min, max and no. of characters for random string");
    }

    public RandomString(List<Object> values, int workerId, int totalWorkers) {
        super((int) System.nanoTime());
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.minimumNumber = ((Number) values.get(0)).intValue();
        this.maximumNumber = ((Number) values.get(1)).intValue();
        this.length = ((Number) values.get(2)).intValue();
        if (maximumNumber < minimumNumber || length <= 0)
            throw new RuntimeException("Please enter correct min, max and no. of characters for random string");
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
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(number(minimumNumber, maximumNumber)));
        while (baseNumberStr.length() < length) {
            baseNumberStr.append('a');
        }
        return baseNumberStr.toString();
    }
}
