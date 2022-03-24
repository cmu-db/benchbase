package com.oltpbenchmark.benchmarks.ycsb;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.HashSet;

public class TestYCSBWorker extends AbstractTestWorker<YCSBBenchmark> {

    @Override
    public HashSet<Class<? extends Procedure>> procedures() {
        return TestYCSBBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<YCSBBenchmark> benchmarkClass() {
        return YCSBBenchmark.class;
    }
}
