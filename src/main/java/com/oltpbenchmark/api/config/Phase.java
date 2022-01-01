package com.oltpbenchmark.api.config;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public record Phase(Integer time, Integer warmup, Boolean serial, Integer activeTerminals, PhaseRateType rateType, Integer rate, PhaseArrival arrival, String weight) {

    public List<Double> weights() {
        return Stream.of(weight.split(","))
                .map(Double::parseDouble).collect(Collectors.toList());
    }
}
