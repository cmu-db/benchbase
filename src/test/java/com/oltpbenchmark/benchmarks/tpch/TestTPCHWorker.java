package com.oltpbenchmark.benchmarks.tpch;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestTPCHWorker extends AbstractTestWorker<TPCHBenchmark> {

    private static final double SCALE_FACTOR = .001;

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTPCHBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    protected void customWorkloadConfiguration(WorkloadConfiguration workConf) {
        // let's set the SF even lower than .01 for actual worker tests
        this.workConf.setScaleFactor(SCALE_FACTOR);
    }

    @Override
    public Class<TPCHBenchmark> benchmarkClass() {
        return TPCHBenchmark.class;
    }
}