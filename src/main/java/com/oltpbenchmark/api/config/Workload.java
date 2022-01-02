package com.oltpbenchmark.api.config;


import com.oltpbenchmark.api.BenchmarkModule;

import java.util.List;

// https://github.com/FasterXML/jackson-databind/issues/3102 @JacksonXmlProperty(isAttribute = true)
public record Workload(Class<? extends BenchmarkModule> benchmarkClass,
                       Double scaleFactor,
                       Double selectivity,
                       Integer terminals,
                       String dataDirectory,
                       FileFormat fileFormat,
                       String traceFile1,
                       String traceFile2,
                       List<Phase> phases,
                       List<Transaction> transactions) {
}
