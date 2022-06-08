package com.oltpbenchmark.benchmarks.otmetrics;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestOTMetricsWorker extends AbstractTestWorker<OTMetricsBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestOTMetricsBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<OTMetricsBenchmark> benchmarkClass() {
        return OTMetricsBenchmark.class;
    }
}
