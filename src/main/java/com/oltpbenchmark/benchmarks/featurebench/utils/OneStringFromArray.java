package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.*;

/*
Description :- Chooses a string randomly from an array of strings passed.
Params :
1.Array of Strings (values)
Eg:-
str :-  ["abc","hty","iki","pou","qwe"]
Return type (String) :- "abc" OR "hty" OR "iki" OR "pou" OR "qwe" (randomly chosen).
*/


public class OneStringFromArray implements BaseUtil {
    private List<String> str;

    public OneStringFromArray(List<Object> values) {
        if (values.size() == 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        str = new ArrayList<>();
        for (Object value : values) {
            str.add((String) value);
        }
    }
    public OneStringFromArray(List<Object> values,int workerId,int totalWorkers) {
        if (values.size() == 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        str = new ArrayList<>();
        for (Object value : values) {
            str.add((String) value);
        }
    }

    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        try {
            return str.get(new Random().nextInt(str.size()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
