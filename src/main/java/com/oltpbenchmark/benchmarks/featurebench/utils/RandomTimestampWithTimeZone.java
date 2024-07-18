package com.oltpbenchmark.benchmarks.featurebench.utils;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;

public class RandomTimestampWithTimeZone implements BaseUtil {

    private final int numberOfTimestamps;
    private final Random rd = new Random(System.currentTimeMillis());

    // Adjust the start epoch as needed for your application
    private final long startEpoch = 1672511400000L;

    public RandomTimestampWithTimeZone(List<Object> values) {
        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberOfTimestamps = ((Number) values.get(0)).intValue();

        if (numberOfTimestamps < 0) {
            throw new RuntimeException("Please enter a positive number of timestamps");
        }
    }

    public RandomTimestampWithTimeZone(List<Object> values, int workerId, int totalWorkers) {
        if (values.isEmpty()) {
            throw new RuntimeException("Incorrect number of parameters for util function "
                + this.getClass());
        }

        this.numberOfTimestamps = ((Number) values.get(0)).intValue();

        if (numberOfTimestamps < 0) {
            throw new RuntimeException("Please enter a positive number of timestamps");
        }
    }

    @Override
    public Object run() {
        int offset = rd.nextInt(numberOfTimestamps);
        return Timestamp.from(OffsetDateTime.ofInstant(
            Instant.ofEpochMilli(startEpoch + offset * 10000000L),
            ZoneOffset.UTC).toInstant());
    }
}
