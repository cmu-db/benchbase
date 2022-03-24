package com.oltpbenchmark.benchmarks.tpch;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.HashSet;

public class TestTPCHWorker extends AbstractTestWorker<TPCHBenchmark> {

    @Override
    public HashSet<Class<? extends Procedure>> procedures() {
        return TestTPCHBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TPCHBenchmark> benchmarkClass() {
        return TPCHBenchmark.class;
    }
}