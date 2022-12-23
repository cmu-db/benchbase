package com.oltpbenchmark.benchmarks.featurebench.utils;


import org.json.JSONObject;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;

public class RandomJson implements BaseUtil {

    protected int fields;
    protected int valueLength;
    protected int nestedness;

    public RandomJson(List<Object> values) {
        if (values.size() < 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.fields = (int)values.get(0);
        this.valueLength = (int)values.get(1);
        if (values.size() > 2)
            this.nestedness = (int)values.get(2);
    }

    public RandomJson(List<Object> values, int workerId, int totalWorkers) {
        this(values);
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        JSONObject outer = new JSONObject();
        for (int i = 0; i < fields; i++) {
            outer.put(Integer.toString(i), new RandomStringAlphabets(Collections.singletonList(valueLength)).run());
        }
        return outer.toString();
    }
}
