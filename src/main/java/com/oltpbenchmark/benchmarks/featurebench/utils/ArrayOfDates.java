package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ArrayOfDates implements BaseUtil {

    private final int arraySize;

    private List<Date> arrayOfDates;

    public ArrayOfDates(List<Object> values) {
        if (values.size() != 1) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.arraySize = ((Number) values.get(0)).intValue();
        if (arraySize <= 0) {
            throw new RuntimeException("Incorrect value for parameter " + this.getClass());
        }

    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        for (int i = 0; i < arraySize; i++) {
            arrayOfDates.add(new Date(ThreadLocalRandom.current().nextInt() * 1000L));
        }
        return arrayOfDates;
    }
}
