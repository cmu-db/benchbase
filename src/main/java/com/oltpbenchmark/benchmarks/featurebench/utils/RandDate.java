package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.time.Period;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandDate implements BaseUtil {

    LocalDate rd;
    public RandDate(List<Object> values)
    {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());

        }
    }
    public RandDate(List<Object> values,int workerId,int totalWorkers)
    {
        if (values.size() != 0) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());

        }
    }
    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException {
        rd = LocalDate.now().minus(Period.ofDays((ThreadLocalRandom.current().nextInt(365*70))));
        return rd;
    }
}