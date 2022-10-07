package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.util.List;

/*
Description :- String Integer Primary key generator between a range.
Params :
1.int startNumber (values[0]) :- starting number for numeric string Primary Key.
2.int desiredLength (values[1]) :- desired length for numeric string Primary key.(extra characters appended by 'a').

Eg:-
startNumber=0, desiredLength: 5
String Numeric Primary keys generated :- "0aaaa","1aaaa","2aaaa","3aaaa",......
Return type :- String (Numeric)
*/

public class PrimaryStringGen implements BaseUtil {
    private int currentValue;
    private final int desiredLength;
    private final int startNumber;
    private String key;

    public PrimaryStringGen(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function");
        }
        this.startNumber = ((Number) (int) values.get(0)).intValue();
        this.currentValue = startNumber - 1;
        this.desiredLength = ((Number) (int) values.get(1)).intValue();
        if (desiredLength <= 0) {
            throw new RuntimeException("Please use positive desired length for string primary keys");
        }
    }

    public String numberToIdString(int number) {
        StringBuilder baseNumberStr = new StringBuilder(String.valueOf(currentValue));
        while (baseNumberStr.length() < desiredLength) {
            baseNumberStr.append('a');
        }
        return baseNumberStr.toString();
    }

    @Override
    public Object run() {
        currentValue++;
        key = numberToIdString(currentValue);
        return key;
    }
}


