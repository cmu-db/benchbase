package com.oltpbenchmark.benchmarks.tpcc;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.List;

public class TestTPCCWorker extends AbstractTestWorker<TPCCBenchmark> {

    @Override
    public List<Class<? extends Procedure>> procedures() {
        return TestTPCCBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TPCCBenchmark> benchmarkClass() {
        return TPCCBenchmark.class;
    }

}
