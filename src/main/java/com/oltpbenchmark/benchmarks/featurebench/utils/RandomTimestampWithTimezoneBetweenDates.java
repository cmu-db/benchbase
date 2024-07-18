package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;


public class RandomTimestampWithTimezoneBetweenDates implements BaseUtil {

    private final LocalDateTime startDate;
    private final LocalDateTime endDate;

    public RandomTimestampWithTimezoneBetweenDates(List<Object> values) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function " + this.getClass());
        }
        if (values.get(0) instanceof Date && values.get(1) instanceof Date) {
            this.startDate = ((Date) values.get(0)).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            this.endDate = ((Date) values.get(1)).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        else {
            this.startDate = LocalDateTime.parse(values.get(0).toString());
            this.endDate = LocalDateTime.parse(values.get(1).toString());
        }

        if (startDate.isAfter(endDate)) {
            throw new RuntimeException("Start date must be before end date");
        }
    }

    public RandomTimestampWithTimezoneBetweenDates(List<Object> values, int workerId, int totalWorkers) {
        if (values.size() != 2) {
            throw new RuntimeException("Incorrect number of parameters for util function " + this.getClass());
        }
        if (values.get(0) instanceof Date && values.get(1) instanceof Date) {
            this.startDate = ((Date) values.get(0)).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            this.endDate = ((Date) values.get(1)).toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        }
        else {
            this.startDate = LocalDateTime.parse(values.get(0).toString());
            this.endDate = LocalDateTime.parse(values.get(1).toString());
        }

        if (startDate.isAfter(endDate)) {
            throw new RuntimeException("Start date must be before end date");
        }
    }

    public static long createRandomLongBetween(long start, long end) {
        return start + (long) (Math.random() * (end - start));
    }

    public Timestamp createRandomTimestamp(LocalDateTime startDate, LocalDateTime endDate) {
        long start = startDate.toEpochSecond(ZoneOffset.UTC);
        long end = endDate.toEpochSecond(ZoneOffset.UTC);
        long randomEpochSecond = createRandomLongBetween(start, end);

        LocalDateTime randomDateTime = LocalDateTime.ofEpochSecond(randomEpochSecond, 0, ZoneOffset.UTC);
        return Timestamp.valueOf(randomDateTime);
    }

    @Override
    public Object run() {
        return createRandomTimestamp(startDate, endDate);
    }
}
