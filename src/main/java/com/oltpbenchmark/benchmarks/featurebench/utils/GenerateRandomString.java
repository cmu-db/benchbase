package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/*
Description :- Returns a random string from an array of random strings generated at runtime(length of random string and array passed as parameters).
Params :
1.int : Desired Length of random string stored in array.(values[0])
2.int : Length of the string array storing random strings.(values[1])

Eg:-
desiredLength: 3, sizeOfStringArray : 5
Array generated :- RandomStringGen :-  ["abc","hty","iki","pou","qwe"]
Return type (String) :- "abc" OR "hty" OR "iki" OR "pou" OR "qwe" (randomly chosen).
*/

public class GenerateRandomString implements BaseUtil {
    private final int desiredLength;
    private final int sizeOfStringArray;

    private ArrayList<String> RandomStringGen;

    GenerateRandomString(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters " +
                "for util function");
        }
        this.desiredLength = ((Number) values.get(0)).intValue();
        this.sizeOfStringArray = ((Number) values.get(1)).intValue();
        if (desiredLength < 0 || sizeOfStringArray <= 0) {
            throw new RuntimeException("Please enter valid desired Length " +
                "and size of string array for random picking");
        }
        RandomStringGen = new ArrayList<String>();
        this.generateList();
    }

    private void generateList() {
        for (int i = 0; i < sizeOfStringArray; i++) {
            RandomStringGen.add((String) new RandomStringAlphabets(List.of(desiredLength)).run());
            System.out.println(RandomStringGen.get(i));
        }
    }


    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        return RandomStringGen.get(new Random().nextInt(RandomStringGen.size()));
    }
}


