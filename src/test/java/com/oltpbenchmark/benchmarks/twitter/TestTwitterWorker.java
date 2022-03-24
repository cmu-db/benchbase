package com.oltpbenchmark.benchmarks.twitter;

import com.oltpbenchmark.api.AbstractTestWorker;
import com.oltpbenchmark.api.Procedure;

import java.util.HashSet;

public class TestTwitterWorker extends AbstractTestWorker<TwitterBenchmark> {

    @Override
    public HashSet<Class<? extends Procedure>> procedures() {
        return TestTwitterBenchmark.PROCEDURE_CLASSES;
    }

    @Override
    public Class<TwitterBenchmark> benchmarkClass() {
        return TwitterBenchmark.class;
    }
}
