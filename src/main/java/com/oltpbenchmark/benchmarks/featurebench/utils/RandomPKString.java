package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/*

Params :
1.int startNumber (values[0]) :- starting number for numeric string Primary Key.
2.int desiredLength (values[1]) :- desired length for numeric string Primary key.(extra characters appended by 'a').

Eg:-
startNumber=0, desiredLength: 5
String Numeric Primary keys generated :- "0aaaa","1aaaa","2aaaa","3aaaa",......
Return type :- String (Numeric)
*/

public class RandomPKString implements BaseUtil {
    private final int desiredLength;
    private final int startNumber;
    private final int endNumber;

    private String key;

    public RandomPKString(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.startNumber = ((Number) values.get(0)).intValue();
        this.endNumber = ((Number) values.get(1)).intValue();
        this.desiredLength = ((Number) values.get(2)).intValue();
        if (desiredLength <= 0) {
            throw new RuntimeException("Please use positive desired length for string primary keys");
        }
    }

    public RandomPKString(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.startNumber = ((Number) values.get(0)).intValue();
        this.endNumber = ((Number) values.get(1)).intValue();
        this.desiredLength = ((Number) values.get(2)).intValue();
        if (desiredLength <= 0) {
            throw new RuntimeException("Please use positive desired length for string primary keys");
        }

    }

    public String numberToIdString(int number) {
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(number));
        while (baseNumberStr.length() < desiredLength) {
            baseNumberStr.append('a');
        }
        return baseNumberStr.toString();
    }

    @Override
    public Object run() {
        key = numberToIdString(ThreadLocalRandom.current().nextInt(startNumber, endNumber + 1));
        return key;
    }
}