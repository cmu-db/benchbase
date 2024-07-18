package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;



/*
Description :- Chooses a uuid randomly from an array of uuids passed.
Params :
1.Array of UUIDs (values)
Eg:-
str :-  ["uuid1","uuid2"]
Return type (UUID) :- (randomly chosen).
*/


public class OneUUIDFromArray implements BaseUtil {
    private List<String> str;

    public OneUUIDFromArray(List<Object> values) {
        if (values.size() == 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        str = new ArrayList<>();
        for (Object value : values) {
            str.add((String) value);
        }
    }
    public OneUUIDFromArray(List<Object> values,int workerId,int totalWorkers) {
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
            return UUID.fromString(str.get(new Random().nextInt(str.size())));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}
