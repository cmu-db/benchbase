package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.util.List;

public class RandomDateBtwYears implements BaseUtil {

    private final int yearLowerBound;
    private final int yearUpperBound;


    public RandomDateBtwYears(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.yearLowerBound = ((Number) values.get(0)).intValue();
        this.yearUpperBound = ((Number) values.get(1)).intValue();
        if (yearLowerBound > yearUpperBound) {
            throw new RuntimeException("Please enter correct values for yearLowerBound and yearUpperBound");
        }
    }

    public RandomDateBtwYears(List<Object> values,int workerId,int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.yearLowerBound = ((Number) values.get(0)).intValue();
        this.yearUpperBound = ((Number) values.get(1)).intValue();
        if (yearLowerBound > yearUpperBound) {
            throw new RuntimeException("Please enter correct values for yearLowerBound and yearUpperBound");
        }
    }

    public static int createRandomIntBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public LocalDate createRandomDate(int startYear, int endYear) {
        int day = createRandomIntBetween(1, 28);
        int month = createRandomIntBetween(1, 12);
        int year = createRandomIntBetween(startYear, endYear);
        return LocalDate.of(year, month, day);
    }

    @Override
    public Object run() throws ClassNotFoundException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        LocalDate randomDate = createRandomDate(yearLowerBound, yearUpperBound);
        return randomDate;
    }
}
