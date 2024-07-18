package com.oltpbenchmark.benchmarks.featurebench.utils;


import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;

public class RandomTimestampWithTimezoneBtwMonths implements BaseUtil {

    private final int year;
    private final int monthLowerBound;
    private final int monthUpperBound;

    public RandomTimestampWithTimezoneBtwMonths(List<Object> values) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.year = ((Number) values.get(0)).intValue();
        this.monthLowerBound = ((Number) values.get(1)).intValue();
        this.monthUpperBound = ((Number) values.get(2)).intValue();
        if (monthLowerBound < 1 || monthUpperBound > 12 || monthLowerBound > monthUpperBound) {
            throw new RuntimeException("Please enter correct values for monthLowerBound and monthUpperBound");
        }
    }

    public RandomTimestampWithTimezoneBtwMonths(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 3) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }
        this.year = ((Number) values.get(0)).intValue();
        this.monthLowerBound = ((Number) values.get(1)).intValue();
        this.monthUpperBound = ((Number) values.get(2)).intValue();
        if (monthLowerBound < 1 || monthUpperBound > 12 || monthLowerBound > monthUpperBound) {
            throw new RuntimeException("Please enter correct values for monthLowerBound and monthUpperBound");
        }
    }

    public static int createRandomIntBetween(int start, int end) {
        return start + (int) Math.round(Math.random() * (end - start));
    }

    public Timestamp createRandomTimestamp(int year, int startMonth, int endMonth) {
        int day = createRandomIntBetween(1, 28);
        int month = createRandomIntBetween(startMonth, endMonth);
        int hour = createRandomIntBetween(0, 23);
        int minute = createRandomIntBetween(0, 59);
        int second = createRandomIntBetween(0, 59);

        LocalDateTime randomDateTime = LocalDateTime.of(year, month, day, hour, minute, second);
        return Timestamp.valueOf(randomDateTime);
    }

    @Override
    public Object run() {
        Timestamp randomTimestamp = createRandomTimestamp(year, monthLowerBound, monthUpperBound);
        return randomTimestamp;
    }
}