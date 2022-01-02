package com.oltpbenchmark.api.config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import com.oltpbenchmark.api.BenchmarkModule;

import java.util.List;

@JacksonXmlRootElement(localName = "configuration")
public record Configuration(List<Workload> workloads) {

    public Workload getWorkloadForBenchmark(Class<? extends BenchmarkModule> benchmarkClass) {
        for (Workload workload : workloads) {
            if (workload.benchmarkClass().equals(benchmarkClass)) {
                return workload;
            }
        }

        throw new IllegalArgumentException(String.format("No benchmark configured for class [%s]", benchmarkClass.toString()));
    }
}
