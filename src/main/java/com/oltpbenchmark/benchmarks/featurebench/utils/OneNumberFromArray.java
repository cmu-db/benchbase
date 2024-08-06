package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class OneNumberFromArray implements BaseUtil {
    private List<Integer> listOfIntegers;

    public OneNumberFromArray(List<Object> values) {
        if (values.size() == 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        listOfIntegers = new ArrayList<>();
        for (Object value : values) {
            listOfIntegers.add(convertToInteger(value));
        }
    }

    public OneNumberFromArray(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() == 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        listOfIntegers = new ArrayList<>();
        for (Object value : values) {
            listOfIntegers.add(convertToInteger(value));
        }
    }

    private Integer convertToInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                throw new RuntimeException("Cannot convert value to Integer: " + value, e);
            }
        } else {
            throw new RuntimeException("Unsupported type for conversion to Integer: " + value.getClass());
        }
    }

    public Object run() throws ClassNotFoundException, InvocationTargetException,
        NoSuchMethodException, InstantiationException, IllegalAccessException {
        try {
            return listOfIntegers.get(new Random().nextInt(listOfIntegers.size()));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }
}