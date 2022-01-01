package com.oltpbenchmark.api.config;

import java.util.Arrays;

public record Phase(Integer time, Integer warmup, Boolean serial, Integer activeTerminals, PhaseRateType rateType, Integer rate, PhaseArrival arrival, String weight) {

    public int[] weights() {
        return Arrays.stream(weight.split(","))
                .mapToInt(Integer::parseInt)
                .toArray();
    }
}
